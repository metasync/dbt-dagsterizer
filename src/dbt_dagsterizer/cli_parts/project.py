from __future__ import annotations

import importlib
import importlib.metadata
import re
from contextlib import contextmanager
from pathlib import Path

import click

from ..gitops_env import generate_gitops_env


def _default_dagster_version() -> str:
    try:
        return importlib.metadata.version("dagster")
    except importlib.metadata.PackageNotFoundError:
        return "1.12.19"


def _default_dbt_dagsterizer_version() -> str:
    try:
        return importlib.metadata.version("dbt-dagsterizer")
    except importlib.metadata.PackageNotFoundError:
        return ""


def _normalize_app_name(name: str) -> str:
    """Normalize a human-friendly name into a Python-safe identifier.

    Rules:
    - Lowercase.
    - Replace non-alphanumeric characters with `_`.
    - Collapse consecutive `_`.
    - Strip leading/trailing `_`.
    - If the result starts with a digit, prefix with `app_`.
    """
    name = name.strip().lower()
    name = re.sub(r"[^0-9a-zA-Z]+", "_", name)
    name = re.sub(r"_+", "_", name).strip("_")
    if not name:
        raise click.ClickException("--project-name/--name must not be empty after normalization")
    if name[0].isdigit():
        name = f"app_{name}"
    return name


def _normalize_namespace(namespace: str) -> str:
    ns = namespace.strip().lower()
    if not ns:
        return ""
    ns = re.sub(r"[^0-9a-zA-Z]+", "_", ns)
    ns = re.sub(r"_+", "_", ns).strip("_")
    if not ns:
        raise click.ClickException("--namespace must not be empty after normalization")
    if ns[0].isdigit():
        ns = f"ns_{ns}"
    return ns


def _print_template_names() -> None:
    base = importlib.resources.files("dbt_dagsterizer").joinpath("project_templates")
    try:
        names = sorted([p.name for p in base.iterdir() if p.is_dir()])
    except Exception as e:
        raise click.ClickException(f"Failed to list embedded templates: {e}") from e
    for name in names:
        click.echo(name)


@contextmanager
def _template_dir(template_name: str):
    pkg = "dbt_dagsterizer"
    rel = f"project_templates/{template_name}"
    root = importlib.resources.files(pkg).joinpath(rel)
    with importlib.resources.as_file(root) as path:
        yield Path(path)


def build_project_group() -> click.Group:
    @click.group()
    def project() -> None:
        pass

    @project.command("list-templates", help="List embedded project templates.")
    def project_list_templates() -> None:
        _print_template_names()

    @project.command("init")
    @click.option(
        "--template",
        "template_name",
        default="luban-dagster-dbt-starrocks-code-location-source-template",
        show_default=True,
    )
    @click.option("--output-dir", type=click.Path(path_type=Path), default=Path.cwd(), show_default=True)
    @click.option("--force/--no-force", default=False, show_default=True)
    @click.option(
        "--project-name",
        "--name",
        required=True,
        help="Human-friendly app name (used to derive code-location/package names)",
    )
    @click.option(
        "--namespace",
        default="",
        show_default=True,
        help="Optional namespace (Luban project). Used for OTEL service naming and StarRocks DB prefixes.",
    )
    @click.option(
        "--dagster-version",
        default=_default_dagster_version(),
        show_default=True,
        help="Dagster version pinned into the rendered project's dependencies.",
    )
    @click.option(
        "--dbt-dagsterizer-version",
        default=None,
        show_default="installed version (if available)",
        help="Pin dbt-dagsterizer in the rendered project's dependencies. Defaults to the installed dbt-dagsterizer version if available.",
    )
    @click.option(
        "--no-pin-dbt-dagsterizer",
        is_flag=True,
        default=False,
        show_default=True,
        help="Do not pin dbt-dagsterizer in the rendered project's dependencies (mutually exclusive with --dbt-dagsterizer-version).",
    )
    @click.option("--default-env", default="development", show_default=True)
    @click.option("--code-location-port", default="3000", show_default=True)
    @click.option("--include-sample-dbt-project", is_flag=True, default=False, show_default=True)
    @click.option("--include-docker", is_flag=True, default=False, show_default=True)
    @click.option("--author-name", default="")
    @click.option("--author-email", default="")
    @click.option("--python-index-url", default="")
    @click.option("--python-index-name", default="custom", show_default=True)
    def project_init(
        template_name: str,
        output_dir: Path,
        force: bool,
        project_name: str,
        namespace: str,
        dagster_version: str,
        dbt_dagsterizer_version: str | None,
        no_pin_dbt_dagsterizer: bool,
        default_env: str,
        code_location_port: str,
        include_sample_dbt_project: bool,
        include_docker: bool,
        author_name: str,
        author_email: str,
        python_index_url: str,
        python_index_name: str,
    ) -> None:
        try:
            from cookiecutter.main import cookiecutter
        except Exception as e:
            raise click.ClickException(
                "cookiecutter is required for project init. Install cookiecutter and retry."
            ) from e

        project_name = project_name.strip()
        if not project_name:
            raise click.ClickException("--project-name/--name must be non-empty")

        app_name = _normalize_app_name(project_name)
        package_name_value = app_name

        project_name_value = project_name
        namespace_value = _normalize_namespace(namespace)

        dagster_version_value = dagster_version.strip()
        if not dagster_version_value:
            raise click.ClickException("--dagster-version must be non-empty")

        if no_pin_dbt_dagsterizer:
            if dbt_dagsterizer_version is not None:
                raise click.ClickException(
                    "--no-pin-dbt-dagsterizer and --dbt-dagsterizer-version are mutually exclusive"
                )
            dbt_dagsterizer_version_value = ""
        elif dbt_dagsterizer_version is None:
            dbt_dagsterizer_version_value = _default_dbt_dagsterizer_version()
        else:
            dbt_dagsterizer_version_value = dbt_dagsterizer_version.strip()
            if not dbt_dagsterizer_version_value:
                raise click.ClickException(
                    "--dbt-dagsterizer-version must be non-empty (use --no-pin-dbt-dagsterizer to leave it unpinned)"
                )

        output_dir = output_dir.expanduser().resolve()
        output_dir.mkdir(parents=True, exist_ok=True)

        extra_context = {
            "project_name": project_name_value,
            "app_name": app_name,
            "package_name": package_name_value,
            "namespace": namespace_value,
            "dagster_version": dagster_version_value,
            "dbt_dagsterizer_version": dbt_dagsterizer_version_value,
            "default_env": default_env,
            "code_location_port": str(code_location_port),
            "include_sample_dbt_project": bool(include_sample_dbt_project),
            "include_docker": bool(include_docker),
            "author_name": author_name,
            "author_email": author_email,
            "python_index_url": python_index_url,
            "python_index_name": python_index_name,
        }

        with _template_dir(template_name) as tmpl:
            cookiecutter(
                str(tmpl),
                no_input=True,
                extra_context=extra_context,
                output_dir=str(output_dir),
                overwrite_if_exists=force,
            )

        project_root = output_dir / app_name
        click.echo(str(project_root))

    @project.command("gen-gitops-env")
    @click.option("--project-dir", type=click.Path(path_type=Path), default=Path.cwd(), show_default=True)
    @click.option("--env-file", type=click.Path(path_type=Path), default=Path(".env"), show_default=True)
    @click.option(
        "--output-dir",
        "output_dir_",
        type=click.Path(path_type=Path),
        default=Path(".gitops-env"),
        show_default=True,
    )
    @click.option(
        "--dagster-home",
        default="/tmp/dagster_home",
        show_default=True,
        help="Value to write as DAGSTER_HOME in the generated GitOps ConfigMap (do not reuse the local .env value).",
    )
    @click.option("--overwrite/--no-overwrite", default=False, show_default=True)
    @click.option("--update-gitignore/--no-update-gitignore", default=True, show_default=True)
    def project_gen_gitops_env(
        project_dir: Path,
        env_file: Path,
        output_dir_: Path,
        dagster_home: str,
        overwrite: bool,
        update_gitignore: bool,
    ) -> None:
        try:
            out = generate_gitops_env(
                project_dir=project_dir,
                env_file=env_file,
                output_dir=output_dir_,
                dagster_home=dagster_home,
                overwrite=overwrite,
                update_gitignore=update_gitignore,
            )
        except FileNotFoundError as e:
            raise click.ClickException(f"Missing file: {e}") from e
        except FileExistsError as e:
            raise click.ClickException(f"Output dir already exists: {e} (use --overwrite)") from e
        except ValueError as e:
            raise click.ClickException(str(e)) from e

        click.echo(str(out))

    return project
