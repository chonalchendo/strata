"""Strata CLI — Feature store that works.

Define features in Python, run locally, scale to Databricks.
"""

from __future__ import annotations

import getpass
import json
import socket
import time
from pathlib import Path
from typing import Annotated

import cyclopts
from loguru import logger
from rich.console import Console

import strata.diff as diff
import strata.discovery as discovery
import strata.errors as errors
import strata.output as output
import strata.registry as reg_types
import strata.settings as settings
import strata.validation as validation

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


def _get_registry(strata_settings: settings.StrataSettings):
    """Get registry backend for current environment."""
    return strata_settings.active_environment.registry


def _get_applied_by() -> str:
    """Get user@hostname string for changelog."""
    return f"{getpass.getuser()}@{socket.gethostname()}"


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
def preview(
    env_name: Annotated[
        str | None,
        cyclopts.Parameter(name="--env", help="Environment to preview"),
    ] = None,
    show_unchanged: Annotated[
        bool,
        cyclopts.Parameter(name="--show-unchanged", help="Show unchanged resources"),
    ] = False,
):
    """Preview changes without applying.

    Shows what would be created, updated, or deleted when you run `strata up`.
    """
    try:
        strata_settings = settings.load_strata_settings(env=env_name)
        console.print(
            f"[bold]Previewing changes for {strata_settings.active_env}[/bold]"
        )
        console.print()

        # Discover definitions
        discovered = discovery.discover_definitions(strata_settings)

        # Get registry and initialize
        reg = _get_registry(strata_settings)
        reg.initialize()

        # Compute diff
        result = diff.compute_diff(discovered, reg)

        # Render diff
        output.render_diff(result, show_unchanged=show_unchanged)

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
    """Validate project configuration and feature definitions.

    Checks:
    - Configuration file syntax and schema
    - Entity, FeatureTable, SourceTable, Dataset definitions
    - References between objects (entity refs, source refs)
    - Schedule tags against allowed list
    """
    try:
        strata_settings = settings.load_strata_settings(env=env_name)
        console.print(f"[bold]Validating project: {strata_settings.name}[/bold]")
        console.print()

        # Validate configuration
        console.print("[green]✓[/green] Configuration valid")
        console.print(f"  Project: {strata_settings.name}")
        console.print(f"  Environment: {strata_settings.active_env}")
        if strata_settings.schedules:
            console.print(f"  Schedules: {', '.join(strata_settings.schedules)}")
        console.print()

        # Validate definitions
        console.print("[dim]Validating definitions...[/dim]")
        result = validation.validate_definitions(strata_settings)

        # Report warnings
        for issue in result.warnings:
            _render_issue(issue, "yellow", "warning")

        # Report errors
        for issue in result.errors:
            _render_issue(issue, "red", "error")

        console.print()

        if result.has_errors:
            console.print(
                f"[bold red]Validation failed:[/bold red] {len(result.errors)} error(s)"
            )
            raise SystemExit(1)
        elif result.has_warnings:
            console.print(
                f"[bold yellow]Validation passed with warnings:[/bold yellow] {len(result.warnings)} warning(s)"
            )
        else:
            console.print("[bold green]✓ Validation passed[/bold green]")

    except errors.StrataError as e:
        _handle_error(e)
        raise SystemExit(1)


def _render_issue(issue: validation.ValidationIssue, color: str, label: str) -> None:
    """Render a validation issue."""
    console.print(f"[{color}]{label}:[/{color}] {issue.message}")
    if issue.source_file:
        console.print(f"  [dim]File: {issue.source_file}[/dim]")
    if issue.object_kind and issue.object_name:
        console.print(f"  [dim]Object: {issue.object_kind} '{issue.object_name}'[/dim]")
    if issue.fix_suggestion:
        console.print(f"  [green]Fix: {issue.fix_suggestion}[/green]")
    console.print()


@app.command
def up(
    dry_run: Annotated[
        bool,
        cyclopts.Parameter(
            name="--dry-run",
            help="Preview changes without applying (alias for preview)",
        ),
    ] = False,
    yes: Annotated[
        bool,
        cyclopts.Parameter(name="--yes", help="Skip confirmation prompt"),
    ] = False,
    env_name: Annotated[
        str | None,
        cyclopts.Parameter(name="--env", help="Environment to deploy to"),
    ] = None,
):
    """Sync feature definitions to registry.

    Shows changes and prompts for confirmation before applying.
    Use --yes to skip confirmation (for CI/automation).
    Use --dry-run to preview without applying.
    """
    try:
        strata_settings = settings.load_strata_settings(env=env_name)

        # Discovery phase with timing telemetry
        t0 = time.perf_counter()
        discovered = discovery.discover_definitions(strata_settings)
        t_discovery = time.perf_counter() - t0
        logger.debug(
            f"Discovery: {t_discovery * 1000:.1f}ms ({len(discovered)} objects)"
        )

        # Get registry and initialize
        reg = _get_registry(strata_settings)
        reg.initialize()

        # Diff phase with timing telemetry
        t0 = time.perf_counter()
        result = diff.compute_diff(discovered, reg)
        t_diff = time.perf_counter() - t0
        logger.debug(f"Diff: {t_diff * 1000:.1f}ms ({len(result.changes)} changes)")

        if dry_run:
            # Just preview
            console.print(f"[bold]Preview for {strata_settings.active_env}:[/bold]")
            console.print()
            output.render_diff(result)
            return

        # Show diff
        console.print(f"[bold]Changes for {strata_settings.active_env}:[/bold]")
        console.print()
        output.render_diff(result)

        if not result.has_changes:
            output.render_no_changes()
            return

        # Confirm or auto-apply
        if not yes:
            if not output.prompt_apply():
                output.render_cancelled()
                return

        # Apply phase with timing telemetry
        output.render_apply_start()
        applied_by = _get_applied_by()
        t0 = time.perf_counter()
        applied_count = 0

        for change in result.changes:
            if change.operation == diff.ChangeOperation.UNCHANGED:
                continue

            output.render_apply_progress(change)

            if change.operation in (
                diff.ChangeOperation.CREATE,
                diff.ChangeOperation.UPDATE,
            ):
                obj = reg_types.ObjectRecord(
                    kind=change.kind,
                    name=change.name,
                    spec_hash=change.new_hash,
                    spec_json=change.spec_json,
                    version=1,  # Registry handles versioning
                )
                reg.put_object(obj, applied_by=applied_by)
                applied_count += 1

            elif change.operation == diff.ChangeOperation.DELETE:
                reg.delete_object(change.kind, change.name, applied_by=applied_by)
                applied_count += 1

        t_apply = time.perf_counter() - t0
        logger.debug(f"Apply: {t_apply * 1000:.1f}ms ({applied_count} changes applied)")
        output.render_apply_complete(result)

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
    table: Annotated[
        str | None,
        cyclopts.Parameter(
            help="Specific table to compile (compiles all if not specified)"
        ),
    ] = None,
    env_name: Annotated[
        str | None,
        cyclopts.Parameter(name="--env", help="Environment to compile for"),
    ] = None,
):
    """Generate SQL files to .strata/compiled/ directory.

    Creates query.sql and lineage.json for each feature table.
    Useful for debugging and auditing.
    """
    try:
        strata_settings = settings.load_strata_settings(env=env_name)
        console.print(f"[bold]Compiling for {strata_settings.active_env}...[/bold]")
        console.print()

        # Discover definitions
        discovered = discovery.discover_definitions(strata_settings)

        # Filter to feature tables (only things we compile)
        feature_tables = [d for d in discovered if d.kind == "feature_table"]

        if table:
            # Check if it's a source_table (which can't be compiled)
            source_tables = [d for d in discovered if d.kind == "source_table"]
            for st in source_tables:
                if st.name == table:
                    console.print(f"[red]Error:[/red] '{table}' is a SourceTable")
                    console.print()
                    console.print(
                        "[dim]Hint: Only FeatureTable definitions can be compiled."
                    )
                    console.print(
                        "SourceTables define raw data sources and don't have computed features.[/dim]"
                    )
                    raise SystemExit(1)

            # Filter to specific table
            feature_tables = [d for d in feature_tables if d.name == table]
            if not feature_tables:
                console.print(f"[red]Table '{table}' not found[/red]")
                raise SystemExit(1)

        # Create output directory
        project_root = (
            Path(strata_settings._config_path).parent
            if strata_settings._config_path
            else Path.cwd()
        )
        output_dir = project_root / ".strata" / "compiled"

        compiled_count = 0
        for disc in feature_tables:
            table_dir = output_dir / disc.name
            table_dir.mkdir(parents=True, exist_ok=True)

            # Generate spec as placeholder for SQL (actual Ibis compilation is Phase 4)
            spec = discovery.serialize_to_spec(disc.obj, disc.kind)

            # Write query.sql (placeholder - will be real SQL in Phase 4)
            query_path = table_dir / "query.sql"
            query_path.write_text(
                f"-- Compiled from {disc.source_file}\n"
                f"-- Actual SQL generation in Phase 4\n"
                f"-- Feature table: {disc.name}\n"
            )

            # Write lineage.json
            lineage_path = table_dir / "lineage.json"
            lineage = {
                "table": disc.name,
                "source_file": disc.source_file,
                "entity": spec.get("entity"),
                "source": spec.get("source"),
                "aggregates": [a["name"] for a in spec.get("aggregates", [])],
                "custom_features": [f["name"] for f in spec.get("custom_features", [])],
            }
            lineage_path.write_text(json.dumps(lineage, indent=2))

            console.print(f"[green]✓[/green] {disc.name}")
            console.print(f"  [dim]{table_dir}/query.sql[/dim]")
            console.print(f"  [dim]{table_dir}/lineage.json[/dim]")
            compiled_count += 1

        console.print()
        if compiled_count > 0:
            console.print(
                f"[bold green]Compiled {compiled_count} table(s) to .strata/compiled/[/bold green]"
            )
        else:
            console.print("[dim]No feature tables to compile[/dim]")

    except errors.StrataError as e:
        _handle_error(e)
        raise SystemExit(1)


VALID_KINDS = ("entity", "feature_table", "source_table", "dataset")


@app.command
def down(
    kind: Annotated[
        str | None,
        cyclopts.Parameter(
            help="Kind of object to remove (entity, feature_table, source_table, dataset)"
        ),
    ] = None,
    name: Annotated[
        str | None,
        cyclopts.Parameter(help="Name of specific object to remove"),
    ] = None,
    yes: Annotated[
        bool,
        cyclopts.Parameter(name="--yes", help="Skip confirmation prompt"),
    ] = False,
    env_name: Annotated[
        str | None,
        cyclopts.Parameter(name="--env", help="Environment to remove from"),
    ] = None,
):
    """Remove definitions from registry.

    With no arguments, removes all definitions (like terraform destroy).
    With kind and name, removes a specific object (like terraform state rm).

    Examples:
        strata down                    # Remove all definitions
        strata down entity user        # Remove specific entity
        strata down --yes              # Remove all without confirmation
    """
    try:
        strata_settings = settings.load_strata_settings(env=env_name)

        # Validate arguments: either both kind+name or neither
        if (kind is None) != (name is None):
            console.print(
                "[red]Error:[/red] Must provide both kind and name, or neither"
            )
            console.print(
                f"[dim]Valid kinds: {', '.join(VALID_KINDS)}[/dim]"
            )
            raise SystemExit(1)

        # Validate kind if provided
        if kind is not None and kind not in VALID_KINDS:
            console.print(f"[red]Error:[/red] Invalid kind '{kind}'")
            console.print(
                f"[dim]Valid kinds: {', '.join(VALID_KINDS)}[/dim]"
            )
            raise SystemExit(1)

        # Get registry and initialize
        reg = _get_registry(strata_settings)
        reg.initialize()

        if kind is not None and name is not None:
            # Remove specific object
            existing = reg.get_object(kind, name)
            if existing is None:
                console.print(
                    f"[yellow]Object not found:[/yellow] {kind} '{name}'"
                )
                return

            console.print(f"[bold]Removing {kind} '{name}'[/bold]")

            if not yes:
                confirm = console.input(
                    "[yellow]Are you sure? (y/N):[/yellow] "
                )
                if confirm.lower() != "y":
                    console.print("[dim]Cancelled[/dim]")
                    return

            reg.delete_object(kind, name, applied_by=_get_applied_by())
            console.print(f"[green]✓[/green] Removed {kind} '{name}'")

        else:
            # Remove all objects
            objects = reg.list_objects()

            if not objects:
                console.print("[dim]No objects registered[/dim]")
                return

            console.print(f"[bold]Removing all definitions ({len(objects)} objects)[/bold]")
            console.print()

            # Show what will be deleted
            from rich.table import Table

            table = Table(show_header=True, header_style="bold")
            table.add_column("Kind")
            table.add_column("Name")

            for obj in objects:
                table.add_row(obj.kind, obj.name)

            console.print(table)
            console.print()

            if not yes:
                confirm = console.input(
                    "[yellow]Are you sure you want to remove all objects? (y/N):[/yellow] "
                )
                if confirm.lower() != "y":
                    console.print("[dim]Cancelled[/dim]")
                    return

            # Delete all objects
            applied_by = _get_applied_by()
            for obj in objects:
                reg.delete_object(obj.kind, obj.name, applied_by=applied_by)
                console.print(f"[red]-[/red] {obj.kind} '{obj.name}'")

            console.print()
            console.print(
                f"[bold green]✓ Removed {len(objects)} object(s)[/bold green]"
            )

    except errors.StrataError as e:
        _handle_error(e)
        raise SystemExit(1)


@app.command
def ls(
    kind: Annotated[
        str | None,
        cyclopts.Parameter(
            help="Filter by kind (entity, feature_table, source_table, dataset)"
        ),
    ] = None,
    env_name: Annotated[
        str | None,
        cyclopts.Parameter(name="--env", help="Environment to list from"),
    ] = None,
):
    """List registered objects.

    Shows all objects in the registry with version and content hash.
    Optionally filter by kind.

    Examples:
        strata ls                    # List all objects
        strata ls entity             # List only entities
        strata ls feature_table      # List only feature tables
    """
    try:
        strata_settings = settings.load_strata_settings(env=env_name)

        # Validate kind if provided
        if kind is not None and kind not in VALID_KINDS:
            console.print(f"[red]Error:[/red] Invalid kind '{kind}'")
            console.print(
                f"[dim]Valid kinds: {', '.join(VALID_KINDS)}[/dim]"
            )
            raise SystemExit(1)

        # Get registry and initialize
        reg = _get_registry(strata_settings)
        reg.initialize()

        # List objects
        objects = reg.list_objects(kind=kind)

        if not objects:
            if kind:
                console.print(f"[dim]No {kind} objects registered[/dim]")
            else:
                console.print("[dim]No objects registered[/dim]")
            return

        # Display as Rich table
        from rich.table import Table

        table = Table(show_header=True, header_style="bold")
        table.add_column("Kind")
        table.add_column("Name")
        table.add_column("Version")
        table.add_column("Hash")

        for obj in objects:
            table.add_row(
                obj.kind,
                obj.name,
                str(obj.version),
                obj.spec_hash[:8],
            )

        console.print(table)
        console.print()
        console.print(f"[dim]Total: {len(objects)} object(s)[/dim]")

    except errors.StrataError as e:
        _handle_error(e)
        raise SystemExit(1)
