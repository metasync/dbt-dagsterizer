from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version

import click

from .macros import build_macros_group
from .meta import build_meta_group
from .project import build_project_group


def _cli_version() -> str:
    try:
        return version("dbt-dagsterizer")
    except PackageNotFoundError:
        return "0.0.0"


def build_cli() -> click.Group:
    @click.group()
    @click.version_option(_cli_version(), prog_name="dbt-dagsterizer")
    def cli() -> None:
        pass

    cli.add_command(build_meta_group())
    cli.add_command(build_macros_group())
    cli.add_command(build_project_group())
    return cli
