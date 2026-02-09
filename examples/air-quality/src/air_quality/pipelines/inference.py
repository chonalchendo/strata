"""Inference pipeline for PM2.5 batch predictions.

Loads a trained model, reads features for an inference window via
read_features(), generates predictions, and writes them to a predictions
table via project.write_table(). Only inference predictions are persisted.

Usage:
    from air_quality.pipelines.inference import predict
    predict()
"""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa

import strata as st


def predict(
    start: str = "2023-10-01",
    end: str = "2024-01-01",
    model_dir: str = "models",
) -> pa.Table:
    """Run batch inference for PM2.5 predictions.

    Steps:
        1. Connect to the Strata project
        2. Load the trained model from disk
        3. Read features for the inference window via read_features()
        4. Generate predictions
        5. Write predictions to the predictions table via project.write_table()

    Args:
        start: Inference start date (inclusive).
        end: Inference end date (exclusive).
        model_dir: Directory containing the trained model.

    Returns:
        PyArrow Table with predictions.
    """
    import joblib
    import numpy as np

    # 1. Connect to Strata project
    project = st.connect()

    # 2. Load model
    model_path = Path(model_dir) / "pm25_model.joblib"
    if not model_path.exists():
        msg = f"Model not found at {model_path}. Run training pipeline first."
        raise FileNotFoundError(msg)
    model = joblib.load(model_path)

    # 3. Read features for inference window
    ds = project.get_dataset("air_quality_prediction")
    features_table = ds.read_features(start=start, end=end)

    # 4. Convert to pandas and extract feature columns
    df = features_table.to_pandas()
    meta_cols = {"date", "country", "city", "street", "pm25"}
    feature_cols = [c for c in df.columns if c not in meta_cols]

    # Drop rows with NaN features
    df = df.dropna(subset=feature_cols)

    if len(df) == 0:
        print("No valid feature rows for inference window. Skipping.")
        return pa.table({})

    x_inference = df[feature_cols].values

    # Generate predictions
    predictions = model.predict(x_inference)

    # 5. Build predictions table
    # Include actual PM2.5 if available for monitoring
    actual_pm25 = df["pm25"].values if "pm25" in df.columns else np.full(len(df), np.nan)

    predictions_table = pa.table({
        "date": pa.array(df["date"].values, type=pa.timestamp("us")),
        "country": pa.array(df["country"].values if "country" in df.columns else ["United Kingdom"] * len(df)),
        "city": pa.array(df["city"].values if "city" in df.columns else ["Edinburgh"] * len(df)),
        "street": pa.array(df["street"].values if "street" in df.columns else ["St-Leonards"] * len(df)),
        "pm25_predicted": pa.array(predictions, type=pa.float64()),
        "pm25_actual": pa.array(actual_pm25, type=pa.float64()),
    })

    # Write predictions via project.write_table() (NOT project._backend)
    project.write_table("pm25_predictions", predictions_table, mode="append")
    print(f"Wrote {len(predictions_table)} predictions to pm25_predictions table")

    return predictions_table


if __name__ == "__main__":
    predict()
