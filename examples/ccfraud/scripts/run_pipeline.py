#!/usr/bin/env python3
"""End-to-end pipeline runner for the ccfraud example.

Orchestrates the full Strata workflow:
    1. Generate data   -- create synthetic CSV files
    2. strata up       -- sync definitions to registry
    3. strata build    -- materialize feature tables
    4. train           -- train XGBoost fraud model
    5. strata publish  -- sync online=True tables to online store
    6. Summary         -- print results

Run from the examples/ccfraud/ directory:
    python scripts/run_pipeline.py
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path


def run_cmd(args: list[str], description: str) -> None:
    """Run a CLI command with output and error handling."""
    print(f"\n{'='*60}")
    print(f"  {description}")
    print(f"{'='*60}\n")

    result = subprocess.run(
        args,
        capture_output=False,
        text=True,
    )

    if result.returncode != 0:
        print(f"\nWARNING: '{' '.join(args)}' exited with code {result.returncode}")
        print("Continuing pipeline execution...")


def main() -> None:
    """Run the full ccfraud pipeline."""
    # Ensure we're in the right directory
    project_root = Path(__file__).parent.parent
    strata_yaml = project_root / "strata.yaml"
    if not strata_yaml.exists():
        print(f"Error: strata.yaml not found at {strata_yaml}")
        print("Run this script from the examples/ccfraud/ directory.")
        sys.exit(1)

    # Step 1: Generate synthetic data
    run_cmd(
        [sys.executable, str(project_root / "scripts" / "generate_data.py")],
        "Step 1: Generating synthetic data",
    )

    # Step 2: Sync definitions to registry
    run_cmd(
        [sys.executable, "-m", "strata", "up", "--yes"],
        "Step 2: Syncing definitions to registry (strata up)",
    )

    # Step 3: Build/materialize feature tables
    run_cmd(
        [sys.executable, "-m", "strata", "build"],
        "Step 3: Materializing feature tables (strata build)",
    )

    # Step 4: Train model
    print(f"\n{'='*60}")
    print("  Step 4: Training fraud detection model")
    print(f"{'='*60}\n")

    import ccfraud.pipelines.training as training

    train_metrics = training.train(
        start="2024-01-01",
        end="2024-08-01",
        model_dir=str(project_root / "models"),
    )
    print(f"Training complete. Metrics: {train_metrics}")

    # Step 5: Publish online=True tables to online store
    run_cmd(
        [sys.executable, "-m", "strata", "publish"],
        "Step 5: Publishing to online store (strata publish)",
    )

    # Summary
    print(f"\n{'='*60}")
    print("  Pipeline Complete")
    print(f"{'='*60}")
    print(f"  Precision: {train_metrics['precision']:.3f}")
    print(f"  Recall:    {train_metrics['recall']:.3f}")
    print(f"  F1:        {train_metrics['f1']:.3f}")
    print(f"  AUC:       {train_metrics['auc']:.3f}")
    print(f"{'='*60}")
    print()
    print("To serve predictions:")
    print("  uvicorn ccfraud.app.api:app --port 8000")
    print()
    print("To view the dashboard:")
    print("  streamlit run src/ccfraud/app/frontend.py")
    print()


if __name__ == "__main__":
    main()
