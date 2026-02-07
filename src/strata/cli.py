"""Strata CLI — Feature store that works.

Define features in Python, run locally, scale to Databricks.
"""

from __future__ import annotations

import getpass
import socket
import time
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Annotated

if TYPE_CHECKING:
    import strata.build as build_mod

import cyclopts
from loguru import logger
from rich.console import Console

import strata.diff as diff
import strata.discovery as discovery
import strata.errors as errors
import strata.output as output
import strata.backends as backends
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


def _get_registry(strata_settings: settings.StrataSettings) -> backends.RegistryKind:
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
            console.print(f"  Backend: {env_settings.backend.kind}")
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
    full_refresh: Annotated[
        bool,
        cyclopts.Parameter(
            name="--full-refresh",
            help="Drop and rebuild all tables from scratch",
        ),
    ] = False,
    start: Annotated[
        str | None,
        cyclopts.Parameter(
            name="--start", help="Backfill start date (YYYY-MM-DD)"
        ),
    ] = None,
    end: Annotated[
        str | None,
        cyclopts.Parameter(name="--end", help="Backfill end date (YYYY-MM-DD)"),
    ] = None,
    env_name: Annotated[
        str | None,
        cyclopts.Parameter(name="--env", help="Environment to build in"),
    ] = None,
):
    """Materialize feature tables.

    Builds all discovered feature tables in DAG order. Use a table name
    to build a specific table (with its dependencies). Use --schedule to
    filter tables by schedule tag.

    Operational overrides:
      --full-refresh drops and rebuilds tables (ignores table write_mode).
      --start / --end builds for a specific date range (backfill).

    Examples:
        strata build                         # Build all tables
        strata build user_transactions       # Build specific table + deps
        strata build --schedule hourly       # Build hourly-scheduled tables
        strata build --full-refresh          # Drop and rebuild everything
        strata build --start 2024-01-01 --end 2024-02-01  # Backfill range
    """
    try:
        import strata.build as build_mod

        strata_settings = settings.load_strata_settings(env=env_name)

        # Validate schedule tag if provided
        if schedule:
            strata_settings.validate_schedule(schedule)

        # Validate date range: both or neither
        if (start is None) != (end is None):
            console.print(
                "[red]Error:[/red] --start and --end must be used together"
            )
            raise SystemExit(1)

        # Parse date strings
        start_dt = _parse_date(start) if start else None
        end_dt = _parse_date(end) if end else None

        # Print build context header
        console.print(f"[bold]Building in {strata_settings.active_env}...[/bold]")
        if table:
            console.print(f"  Target: {table}")
        if schedule:
            console.print(f"  Schedule: {schedule}")
        if full_refresh:
            console.print("  Mode: [yellow]full-refresh[/yellow] (drop and rebuild)")
        if start_dt and end_dt:
            console.print(f"  Date range: {start} to {end}")
        console.print()

        # Discover feature tables
        discovered = discovery.discover_definitions(strata_settings)
        feature_tables = [
            d.obj for d in discovered if d.kind == "feature_table"
        ]

        if not feature_tables:
            console.print("[dim]No feature tables found[/dim]")
            return

        # Filter by schedule tag if provided
        if schedule:
            feature_tables = [
                ft for ft in feature_tables if ft.schedule == schedule
            ]
            if not feature_tables:
                console.print(
                    f"[dim]No feature tables with schedule '{schedule}'[/dim]"
                )
                return

        # Build using settings backend (NOT manually constructed)
        env_cfg = strata_settings.active_environment
        engine = build_mod.BuildEngine(backend=env_cfg.backend)

        targets = [table] if table else None
        t0 = time.perf_counter()

        result = engine.build(
            tables=feature_tables,
            targets=targets,
            full_refresh=full_refresh,
            start=start_dt,
            end=end_dt,
        )

        elapsed = time.perf_counter() - t0

        # Display results in Pulumi-style output
        _render_build_results(result, elapsed)

        # Exit non-zero on failure
        if not result.is_success:
            raise SystemExit(1)

    except errors.StrataError as e:
        _handle_error(e)
        raise SystemExit(1)


def _parse_date(date_str: str) -> datetime:
    """Parse a YYYY-MM-DD date string into a datetime."""
    try:
        return datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        console.print(
            f"[red]Error:[/red] Invalid date format '{date_str}'. "
            "Expected YYYY-MM-DD."
        )
        raise SystemExit(1)


def _render_build_results(
    result: build_mod.BuildResult, elapsed: float
) -> None:
    """Render build results in Pulumi-style output."""
    import strata.build as build_mod

    for table_result in result.table_results:
        status = table_result.status
        name = table_result.table_name

        if status == build_mod.BuildStatus.SUCCESS:
            duration = (
                f" ({table_result.duration_ms:.0f}ms)"
                if table_result.duration_ms is not None
                else ""
            )
            rows = (
                f" [{table_result.row_count} rows]"
                if table_result.row_count is not None
                else ""
            )
            console.print(
                f"[green]\u2713[/green] {name}{rows}{duration}"
            )
        elif status == build_mod.BuildStatus.FAILED:
            error = f": {table_result.error}" if table_result.error else ""
            console.print(f"[red]\u2717[/red] {name}{error}")
        elif status == build_mod.BuildStatus.SKIPPED:
            error = f": {table_result.error}" if table_result.error else ""
            console.print(f"[yellow]\u2298[/yellow] {name}{error}")

    console.print()

    # Summary line
    parts = []
    if result.success_count:
        parts.append(f"[green]{result.success_count} succeeded[/green]")
    if result.failed_count:
        parts.append(f"[red]{result.failed_count} failed[/red]")
    if result.skipped_count:
        parts.append(f"[yellow]{result.skipped_count} skipped[/yellow]")

    total = len(result.table_results)
    summary = ", ".join(parts) if parts else "0 tables"
    console.print(f"[bold]Build complete:[/bold] {summary} ({total} total, {elapsed:.1f}s)")


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

    Creates query.sql, ibis_expr.txt, lineage.json, and build_context.json
    for each feature table. Useful for debugging and auditing.
    """
    try:
        import strata.compile_output as compile_output
        import strata.compiler as compiler_mod

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
                        "[dim]Hint: Only FeatureTable definitions can be compiled. "
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

        ibis_compiler = compiler_mod.IbisCompiler()
        compiled_count = 0

        for disc in feature_tables:
            # Compile the table to real SQL via Ibis
            compiled = ibis_compiler.compile_table(disc.obj)

            # Write enhanced output (query.sql, ibis_expr.txt, lineage.json, build_context.json)
            table_dir = compile_output.write_compile_output(
                compiled=compiled,
                disc=disc,
                output_dir=output_dir,
                env=strata_settings.active_env,
                strata_version=__version__,
            )

            console.print(f"[green]\u2713[/green] {disc.name}")
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
            console.print(f"[dim]Valid kinds: {', '.join(VALID_KINDS)}[/dim]")
            raise SystemExit(1)

        # Validate kind if provided
        if kind is not None and kind not in VALID_KINDS:
            console.print(f"[red]Error:[/red] Invalid kind '{kind}'")
            console.print(f"[dim]Valid kinds: {', '.join(VALID_KINDS)}[/dim]")
            raise SystemExit(1)

        # Get registry and initialize
        reg = _get_registry(strata_settings)
        reg.initialize()

        if kind is not None and name is not None:
            # Remove specific object
            existing = reg.get_object(kind, name)
            if existing is None:
                console.print(f"[yellow]Object not found:[/yellow] {kind} '{name}'")
                return

            console.print(f"[bold]Removing {kind} '{name}'[/bold]")

            if not yes:
                confirm = console.input("[yellow]Are you sure? (y/N):[/yellow] ")
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

            console.print(
                f"[bold]Removing all definitions ({len(objects)} objects)[/bold]"
            )
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
            console.print(f"[dim]Valid kinds: {', '.join(VALID_KINDS)}[/dim]")
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
