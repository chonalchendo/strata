#!/usr/bin/env python3
"""End-to-end pipeline runner for the air-quality example.

Orchestrates the full Strata workflow:
    1. strata up       -- sync definitions to registry
    2. strata build    -- materialize feature tables
    3. train           -- train PM2.5 model (predictions ephemeral)
    4. predict         -- batch inference (predictions persisted)

Run from the examples/air-quality/ directory:
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
    """Run the full air-quality pipeline."""
    # Ensure we're in the right directory
    project_root = Path(__file__).parent.parent
    strata_yaml = project_root / "strata.yaml"
    if not strata_yaml.exists():
        print(f"Error: strata.yaml not found at {strata_yaml}")
        print("Run this script from the examples/air-quality/ directory.")
        sys.exit(1)

    # Step 1: Sync definitions to registry
    run_cmd(
        [sys.executable, "-m", "strata", "up", "--yes"],
        "Step 1: Syncing definitions to registry (strata up)",
    )

    # Step 2: Build/materialize feature tables
    run_cmd(
        [sys.executable, "-m", "strata", "build"],
        "Step 2: Materializing feature tables (strata build)",
    )

    # Step 3: Train model (predictions ephemeral -- not persisted)
    print(f"\n{'='*60}")
    print("  Step 3: Training PM2.5 prediction model")
    print(f"{'='*60}\n")

    import air_quality.pipelines.training as training

    train_metrics = training.train(
        start="2023-01-01",
        end="2023-10-01",
        model_dir=str(project_root / "models"),
    )
    print(f"Training complete. Metrics: {train_metrics}")

    # Step 4: Batch inference (predictions persisted via project.write_table)
    print(f"\n{'='*60}")
    print("  Step 4: Running batch inference")
    print(f"{'='*60}\n")

    import air_quality.pipelines.inference as inference

    predictions = inference.predict(
        start="2023-10-01",
        end="2024-01-01",
        model_dir=str(project_root / "models"),
    )
    print(f"Inference complete. Generated {len(predictions)} predictions.")

    # Summary
    print(f"\n{'='*60}")
    print("  Pipeline Complete")
    print(f"{'='*60}")
    print(f"  Training MAE:  {train_metrics['mae']:.2f}")
    print(f"  Training RMSE: {train_metrics['rmse']:.2f}")
    print(f"  Training R2:   {train_metrics['r2']:.3f}")
    print(f"  Predictions:   {len(predictions)} rows")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
