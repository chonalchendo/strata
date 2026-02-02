"""Strata CLI — Feature store that works.

Define features in Python, run locally, scale to Databricks.
"""

from __future__ import annotations

from typing import Annotated

import cyclopts
from rich.console import Console

import strata.errors as errors
import strata.settings as settings

# Version is defined here and in pyproject.toml
__version__ = "0.1.0"

console = Console()

app = cyclopts.App(
    name="strata",
    help="Feature store that works. Define features in Python, run locally, scale to Databricks.",
    version=__version__,
)


def _handle_error(e: errors.StrataError) -> None:
    """Display a structured error message."""
    console.print(f"[bold red]Error:[/bold red] {e.context}\n")
    console.print(f"[yellow]Cause:[/yellow] {e.cause}\n")
    console.print(f"[green]Fix:[/green] {e.fix}")


@app.command
def new():
    """Create a new Strata project."""
    # TODO: Implement project scaffolding
    console.print("[yellow]Not implemented yet[/yellow]")


@app.command
def env(
    name: Annotated[
        str | None,
        cyclopts.Parameter(help="Environment name to show details for"),
    ] = None,
):
    """Show current environment or details of a specific environment."""
    try:
        strata_settings = settings.load_strata_settings()

        if name:
            # Show specific environment
            if name not in strata_settings.environments:
                raise errors.EnvironmentNotFoundError(
                    env=name,
                    available=list(strata_settings.environments.keys()),
                )
            env_settings = strata_settings.environments[name]
            console.print(f"[bold]Environment:[/bold] {name}")
            console.print(f"  Catalog: {env_settings.catalog or '(not set)'}")
            console.print(f"  Registry: {env_settings.registry.kind}")
            console.print(f"  Storage: {env_settings.storage.kind}")
            console.print(f"  Compute: {env_settings.compute.kind}")
        else:
            # Show current/default environment
            console.print(
                f"[bold]Current environment:[/bold] {strata_settings.active_env}"
            )
            console.print(f"[dim]Default:[/dim] {strata_settings.default_env}")
            console.print(
                f"[dim]Available:[/dim] {', '.join(strata_settings.environments.keys())}"
            )

    except errors.StrataError as e:
        _handle_error(e)
        raise SystemExit(1)


@app.command(name="env-list")
def env_list():
    """List all available environments."""
    try:
        strata_settings = settings.load_strata_settings()
        console.print("[bold]Environments:[/bold]")
        for name, env_settings in strata_settings.environments.items():
            marker = (
                " [green](default)[/green]"
                if name == strata_settings.default_env
                else ""
            )
            console.print(f"  {name}{marker}")
            console.print(f"    catalog: {env_settings.catalog or '(not set)'}")
    except errors.StrataError as e:
        _handle_error(e)
        raise SystemExit(1)


@app.command
def validate(
    env_name: Annotated[
        str | None,
        cyclopts.Parameter(name="--env", help="Environment to validate against"),
    ] = None,
):
    """Validate project configuration and feature definitions."""
    try:
        strata_settings = settings.load_strata_settings(env=env_name)
        console.print("[green]✓[/green] Configuration valid")
        console.print(f"  Project: {strata_settings.name}")
        console.print(f"  Environment: {strata_settings.active_env}")

        if strata_settings.schedules:
            console.print(f"  Schedules: {', '.join(strata_settings.schedules)}")

        # TODO: Validate feature definitions once SDK is implemented
    except errors.StrataError as e:
        _handle_error(e)
        raise SystemExit(1)


@app.command
def up(
    dry_run: Annotated[
        bool,
        cyclopts.Parameter(name="--dry-run", help="Preview changes without applying"),
    ] = False,
    env_name: Annotated[
        str | None,
        cyclopts.Parameter(name="--env", help="Environment to deploy to"),
    ] = None,
):
    """Sync feature definitions to registry."""
    try:
        strata_settings = settings.load_strata_settings(env=env_name)

        if dry_run:
            console.print(
                f"[bold]Preview changes for {strata_settings.active_env}:[/bold]"
            )
        else:
            console.print(f"[bold]Syncing to {strata_settings.active_env}...[/bold]")

        # TODO: Implement registry sync
        console.print("[yellow]Not implemented yet[/yellow]")

    except errors.StrataError as e:
        _handle_error(e)
        raise SystemExit(1)


@app.command
def build(
    table: Annotated[
        str | None,
        cyclopts.Parameter(
            help="Specific table to build (builds all if not specified)"
        ),
    ] = None,
    schedule: Annotated[
        str | None,
        cyclopts.Parameter(name="--schedule", help="Filter tables by schedule tag"),
    ] = None,
    start_date: Annotated[
        str | None,
        cyclopts.Parameter(
            name="--start-date", help="Backfill start date (YYYY-MM-DD)"
        ),
    ] = None,
    end_date: Annotated[
        str | None,
        cyclopts.Parameter(name="--end-date", help="Backfill end date (YYYY-MM-DD)"),
    ] = None,
    env_name: Annotated[
        str | None,
        cyclopts.Parameter(name="--env", help="Environment to build in"),
    ] = None,
):
    """Materialize feature tables."""
    try:
        strata_settings = settings.load_strata_settings(env=env_name)

        # Validate schedule tag if provided
        if schedule:
            strata_settings.validate_schedule(schedule)

        console.print(f"[bold]Building in {strata_settings.active_env}...[/bold]")

        if table:
            console.print(f"  Table: {table}")
        if schedule:
            console.print(f"  Schedule filter: {schedule}")
        if start_date and end_date:
            console.print(f"  Date range: {start_date} to {end_date}")

        # TODO: Implement build
        console.print("[yellow]Not implemented yet[/yellow]")

    except errors.StrataError as e:
        _handle_error(e)
        raise SystemExit(1)


@app.command
def compile(
    env_name: Annotated[
        str | None,
        cyclopts.Parameter(name="--env", help="Environment to compile for"),
    ] = None,
):
    """Generate SQL files to .strata/ directory."""
    try:
        strata_settings = settings.load_strata_settings(env=env_name)
        console.print(f"[bold]Compiling for {strata_settings.active_env}...[/bold]")

        # TODO: Implement compile
        console.print("[yellow]Not implemented yet[/yellow]")

    except errors.StrataError as e:
        _handle_error(e)
        raise SystemExit(1)


@app.command
def down():
    """Destroy project infrastructure."""
    # TODO: Implement teardown
    console.print("[yellow]Not implemented yet[/yellow]")
