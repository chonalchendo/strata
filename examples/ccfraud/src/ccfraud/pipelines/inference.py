"""Batch inference pipeline for fraud predictions.

Loads a trained model, reads features for an inference window via
read_features(), generates predictions, and writes them to a predictions
table via project.write_table(). Only inference predictions are persisted.

Usage:
    from ccfraud.pipelines.inference import predict
    predict()
"""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa

import strata as st


def predict(
    start: str = "2024-08-01",
    end: str = "2024-10-01",
    model_dir: str = "models",
) -> pa.Table:
    """Run batch inference for fraud predictions.

    Steps:
        1. Connect to the Strata project
        2. Load the trained model from disk
        3. Read features for the inference window via read_features()
        4. Generate predictions (probability of fraud)
        5. Write predictions to the fraud_predictions table

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
    model_path = Path(model_dir) / "fraud_model.joblib"
    if not model_path.exists():
        msg = f"Model not found at {model_path}. Run training pipeline first."
        raise FileNotFoundError(msg)
    model = joblib.load(model_path)

    # 3. Read features for inference window
    ds = project.get_dataset("fraud_detection")
    features_table = ds.read_features(start=start, end=end)

    # 4. Convert to pandas and extract feature columns
    df = features_table.to_pandas()
    label_col = "is_fraud"
    meta_cols = {"datetime", "cc_num", "merchant_id", label_col}
    feature_cols = [c for c in df.columns if c not in meta_cols]

    # Drop rows with NaN features
    df = df.dropna(subset=feature_cols)

    if len(df) == 0:
        print("No valid feature rows for inference window. Skipping.")
        return pa.table({})

    x_inference = df[feature_cols].values

    # Generate predictions
    fraud_proba = model.predict_proba(x_inference)[:, 1]
    fraud_pred = model.predict(x_inference)

    # 5. Build predictions table
    actual_fraud = (
        df[label_col].values.astype(float)
        if label_col in df.columns
        else np.full(len(df), np.nan)
    )

    predictions_table = pa.table({
        "datetime": pa.array(df["datetime"].values, type=pa.timestamp("us")),
        "cc_num": pa.array(
            df["cc_num"].values if "cc_num" in df.columns else ["unknown"] * len(df)
        ),
        "fraud_probability": pa.array(fraud_proba, type=pa.float64()),
        "fraud_predicted": pa.array(fraud_pred.astype(int), type=pa.int64()),
        "is_fraud_actual": pa.array(actual_fraud, type=pa.float64()),
    })

    # Write predictions via project.write_table() (NOT project._backend)
    project.write_table("fraud_predictions", predictions_table, mode="append")
    print(f"Wrote {len(predictions_table)} predictions to fraud_predictions table")

    return predictions_table


if __name__ == "__main__":
    predict()
