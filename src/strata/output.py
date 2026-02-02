"""Rich-based output formatting for CLI.

Provides Pulumi-style diff output with colors and symbols.
"""

from __future__ import annotations

from rich.console import Console
from rich.table import Table

import strata.diff as diff


# Global console instance
console = Console()


def render_diff(result: diff.DiffResult, show_unchanged: bool = False) -> None:
    """Render diff result in Pulumi style.

    Args:
        result: DiffResult from compute_diff
        show_unchanged: Whether to show unchanged objects (default False)
    """
    if not result.has_changes and not result.unchanged:
        console.print("[dim]No resources found.[/dim]")
        return

    # Create table
    table = Table(show_header=True, header_style="bold", box=None)
    table.add_column("", width=2)  # Symbol column
    table.add_column("Type", style="dim")
    table.add_column("Name")
    table.add_column("Status")

    # Add changes in order: creates, updates, unchanged, deletes
    for change in result.creates:
        table.add_row(
            "[green]+[/green]",
            change.kind,
            f"[green]{change.name}[/green]",
            "[green]create[/green]",
        )

    for change in result.updates:
        table.add_row(
            "[yellow]~[/yellow]",
            change.kind,
            f"[yellow]{change.name}[/yellow]",
            "[yellow]update[/yellow]",
        )

    if show_unchanged:
        for change in result.unchanged:
            table.add_row(
                " ",
                change.kind,
                f"[dim]{change.name}[/dim]",
                "[dim]unchanged[/dim]",
            )

    for change in result.deletes:
        table.add_row(
            "[red]-[/red]",
            change.kind,
            f"[red]{change.name}[/red]",
            "[red]delete[/red]",
        )

    console.print(table)
    console.print()

    # Summary line
    summary = result.summary()
    if result.has_changes:
        console.print(f"[bold]Summary:[/bold] {summary}")
    else:
        console.print(f"[dim]{summary}[/dim]")


def render_apply_start() -> None:
    """Render message before applying changes."""
    console.print()
    console.print("[bold]Applying changes...[/bold]")


def render_apply_progress(change: diff.Change) -> None:
    """Render progress for a single change being applied."""
    if change.operation == diff.ChangeOperation.CREATE:
        console.print(
            f"  [green]+[/green] Creating {change.kind} [green]{change.name}[/green]"
        )
    elif change.operation == diff.ChangeOperation.UPDATE:
        console.print(
            f"  [yellow]~[/yellow] Updating {change.kind} [yellow]{change.name}[/yellow]"
        )
    elif change.operation == diff.ChangeOperation.DELETE:
        console.print(f"  [red]-[/red] Deleting {change.kind} [red]{change.name}[/red]")


def render_apply_complete(result: diff.DiffResult) -> None:
    """Render completion message after apply."""
    console.print()
    console.print(f"[bold green]Apply complete![/bold green] {result.summary()}")


def render_no_changes() -> None:
    """Render message when there are no changes to apply."""
    console.print("[dim]No changes to apply.[/dim]")


def prompt_apply() -> bool:
    """Prompt user to confirm apply. Returns True if confirmed."""
    console.print()
    response = console.input("[bold]Apply these changes?[/bold] [dim](y/N)[/dim] ")
    return response.lower() in ("y", "yes")


def render_cancelled() -> None:
    """Render message when apply is cancelled."""
    console.print("[dim]Apply cancelled.[/dim]")


def render_error(message: str) -> None:
    """Render error message."""
    console.print(f"[bold red]Error:[/bold red] {message}")
