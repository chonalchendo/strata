"""Enhanced compile output generation for debugging and auditing.

Writes structured output files for each compiled feature table:
- query.sql: The compiled SQL (Ibis -> DuckDB dialect)
- ibis_expr.txt: Ibis expression tree for debugging
- lineage.json: Column-level lineage (source tables)
- build_context.json: Build metadata for reproducibility
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import strata.compiler as compiler_mod
import strata.discovery as discovery
import strata.registry as reg_types


def write_compile_output(
    compiled: compiler_mod.CompiledQuery,
    disc: discovery.DiscoveredObject,
    output_dir: Path,
    *,
    env: str,
    strata_version: str,
    registry_serial: int | None = None,
) -> Path:
    """Write enhanced compile output files for a single table.

    Creates the output directory and writes:
    - query.sql: The compiled SQL
    - ibis_expr.txt: String representation of the Ibis expression
    - lineage.json: Source table lineage
    - build_context.json: Build metadata

    Args:
        compiled: The CompiledQuery result from the compiler.
        disc: The DiscoveredObject for the feature table.
        output_dir: Base output directory (e.g. .strata/compiled).
        env: Active environment name.
        strata_version: Current strata version string.
        registry_serial: Optional registry serial number.

    Returns:
        Path to the table output directory.
    """
    table_dir = output_dir / compiled.table_name
    table_dir.mkdir(parents=True, exist_ok=True)

    # Serialize spec once for reuse
    spec = discovery.serialize_to_spec(disc.obj, disc.kind)

    # Write query.sql
    query_path = table_dir / "query.sql"
    query_path.write_text(
        f"-- Compiled from {disc.source_file}\n"
        f"-- Feature table: {compiled.table_name}\n"
        f"--\n"
        f"{compiled.sql}\n"
    )

    # Write ibis_expr.txt
    ibis_path = table_dir / "ibis_expr.txt"
    ibis_path.write_text(str(compiled.ibis_expr))

    # Write lineage.json
    lineage = {
        "table": compiled.table_name,
        "source_file": disc.source_file,
        "source_tables": compiled.source_tables,
        "entity": spec.get("entity"),
        "source": spec.get("source"),
        "aggregates": [a["name"] for a in spec.get("aggregates", [])],
        "custom_features": [f["name"] for f in spec.get("custom_features", [])],
    }
    lineage_path = table_dir / "lineage.json"
    lineage_path.write_text(json.dumps(lineage, indent=2))

    # Write build_context.json
    spec_json = discovery.spec_to_json(spec)
    table_spec_hash = reg_types.compute_spec_hash(spec_json)[:8]
    build_context = {
        "compiled_at": datetime.now(tz=timezone.utc).isoformat(),
        "strata_version": strata_version,
        "registry_serial": registry_serial,
        "table_spec_hash": table_spec_hash,
        "env": env,
        "source": spec.get("source"),
    }
    context_path = table_dir / "build_context.json"
    context_path.write_text(json.dumps(build_context, indent=2))

    return table_dir
