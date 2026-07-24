"""dlt-based replication executor: StarRocks -> SQL Server.

All ``dlt`` imports are lazy (inside function bodies) so that the rest of the
codebase works even when the ``dlt`` package is not installed.
"""
from __future__ import annotations

import logging
from typing import Any
from urllib.parse import quote_plus

from ...resources.mssql import SqlServerClient
from ...resources.starrocks import StarRocksClient

logger = logging.getLogger(__name__)


def execute_replication(
    *,
    context: Any,
    spec: dict,
    starrocks_client: StarRocksClient,
    mssql_client: SqlServerClient,
) -> None:
    """Execute a single replication via dlt.

    Args:
        context: Dagster asset execution context (used for logging + partition key).
        spec: Replication spec dict from ``auto_config.build_auto_replication_specs``.
        starrocks_client: Source database client.
        mssql_client: Destination database client.
    """
    import dlt
    from dlt.sources.sql_database import sql_database

    source_database = spec["source_database"]
    source_table = spec["source_table"]
    destination_table = spec["destination_table"]
    destination_schema = spec["destination_schema"]
    write_disposition = spec["write_disposition"]
    partition_column = spec.get("partition_column")
    primary_key_col = spec.get("primary_key")

    # Extract partition key when the asset is partitioned
    partition_key: str | None = None
    try:
        partition_key = context.partition_key
    except Exception:
        pass

    log = getattr(context, "log", logger)
    log.info(
        "Replication: %s -> MSSQL (table=%s, schema=%s, disposition=%s, partition=%s, primary_key=%s)",
        source_table,
        destination_table,
        destination_schema,
        write_disposition,
        partition_key,
        primary_key_col,
    )

    # Build the StarRocks source connection string (MySQL protocol via pymysql)
    source_credentials = (
        f"mysql+pymysql://{starrocks_client.user}:{starrocks_client.password}"
        f"@{starrocks_client.host}:{starrocks_client.port}/{source_database}"
    )

    # Build SQL Server connection string for dlt
    # URL-encode credentials to handle special characters
    user_encoded = quote_plus(mssql_client.user)
    password_encoded = quote_plus(mssql_client.password)
    driver_encoded = quote_plus(mssql_client.driver)
    mssql_credentials = (
        f"mssql://{user_encoded}:{password_encoded}"
        f"@{mssql_client.host}:{mssql_client.port}/{mssql_client.database}"
        f"?driver={driver_encoded}&TrustServerCertificate=yes&Encrypt=no"
    )

    log.info("Source credentials: %s", source_credentials.replace(starrocks_client.password, "***"))
    log.info("Destination credentials: %s", mssql_credentials.replace(mssql_client.password, "***"))

    # Create dlt pipeline targeting SQL Server
    # Include partition key in pipeline_name to isolate dlt state per partition,
    # preventing race conditions when multiple partitions run concurrently.
    pipeline_suffix = f"_{partition_key}" if partition_key else ""
    pipeline = dlt.pipeline(
        pipeline_name=f"replicate_{spec['model']}{pipeline_suffix}",
        destination=dlt.destinations.mssql(mssql_credentials),
        dataset_name=destination_schema,
    )

    # Determine the effective dlt write disposition
    # For partitioned data with 'replace', we delete only the current partition's rows
    # then append, so the full table is preserved across partitions.
    effective_disposition = write_disposition
    if partition_column and partition_key and write_disposition == "replace":
        # Delete rows for this partition from destination, then append
        import sqlalchemy as sa
        dest_engine = sa.create_engine(mssql_credentials.replace("mssql://", "mssql+pyodbc://"))
        try:
            delete_sql = f"DELETE FROM {destination_schema}.{destination_table} WHERE {partition_column} = :pk"
            with dest_engine.begin() as conn:
                result = conn.execute(sa.text(delete_sql), {"pk": partition_key})
                log.info(
                    "Partition replace: deleted %s existing rows for %s = '%s'",
                    result.rowcount,
                    partition_column,
                    partition_key,
                )
        except Exception as e:
            log.warning("Partition replace: delete failed (table may not exist yet): %s", e)
        finally:
            dest_engine.dispose()
        effective_disposition = "append"

    # Build primary_key hint from the configured primary_key column
    # dlt uses this to create PK constraints in the destination and for merge upserts
    primary_key: list[str] | str | None = None
    if primary_key_col:
        primary_key = primary_key_col
        log.info("Using primary_key: %s", primary_key)

    # Build the source from StarRocks
    log.info("Reading source table '%s.%s' from StarRocks", source_database, source_table)

    if partition_column and partition_key:
        # Partition-aware: use custom filtered resource via SQLAlchemy
        log.info("Partition-aware replication: filtering %s = '%s' via SQL WHERE clause", partition_column, partition_key)
        import sqlalchemy as sa

        @dlt.resource(
            name=destination_table,
            write_disposition=effective_disposition, 
            primary_key=primary_key,
        )
        def _filtered_resource():
            engine = sa.create_engine(source_credentials)
            with engine.connect() as conn:
                result = conn.execute(
                    sa.text(f"SELECT * FROM {source_table} WHERE {partition_column} = :pk"),
                    {"pk": partition_key},
                )
                columns = list(result.keys())
                for row in result:
                    yield dict(zip(columns, row))

        resource = _filtered_resource
    else:
        # No partition filter: use standard sql_database source
        source = sql_database(
            credentials=source_credentials,
            table_names=[source_table],
            reflection_level="minimal",
        )
        resource = source.resources[source_table]
        resource.apply_hints(
            table_name=destination_table,
            write_disposition=effective_disposition,
            primary_key=primary_key,
        )

    # Execute the pipeline
    log.info(
        "Running dlt pipeline: destination_table=%s, destination_schema=%s, effective_disposition=%s (original=%s)",
        destination_table,
        destination_schema,
        effective_disposition,
        write_disposition,
    )
    load_info = pipeline.run(
        resource,
        table_name=destination_table,
        write_disposition=effective_disposition,
    )

    if primary_key:
        constraint_name = f"PK_{destination_table}"

        with pipeline.sql_client() as client:
        # Check if the constraint already exists to avoid errors on subsequent runs
            check_sql = f"""
            IF NOT EXISTS (
                SELECT 1 
                FROM sys.key_constraints 
                WHERE type = 'PK' AND parent_object_id = OBJECT_ID('{client.dataset_name}.{destination_table}')
            )
            BEGIN
                ALTER TABLE {client.dataset_name}.{destination_table} 
                ADD CONSTRAINT {constraint_name} PRIMARY KEY ({primary_key});
            END
            """
            client.execute_sql(check_sql)

    # Log detailed load information
    total_rows = 0
    if load_info and hasattr(load_info, "load_packages"):
        for package in load_info.load_packages:
            for table in package.jobs.get("completed_jobs", []):
                row_count = getattr(table, "row_count", 0) or 0
                total_rows += row_count
                log.info(
                    "Loaded table '%s': %s rows (job: %s)",
                    getattr(table, "table_name", "unknown"),
                    row_count,
                    getattr(table, "job_id", "unknown"),
                )

    log.info("Replication completed: total_rows=%s, load_info=%s", total_rows, load_info)

    if total_rows == 0:
        log.warning(
            "No rows were replicated. Possible causes: source table is empty, "
            "partition filter too restrictive, or connection issue."
        )
