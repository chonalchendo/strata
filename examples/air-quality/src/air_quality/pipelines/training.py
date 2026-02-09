"""Training pipeline for PM2.5 prediction model.

Loads features via read_features(), trains a scikit-learn model, evaluates
predictions in-memory (ephemeral -- not persisted), and saves the model
to disk with joblib.

Usage:
    from air_quality.pipelines.training import train
    train()
"""

from __future__ import annotations

from pathlib import Path


import strata as st


def train(
    start: str = "2023-01-01",
    end: str = "2023-10-01",
    model_dir: str = "models",
) -> dict:
    """Train a PM2.5 prediction model using Strata features.

    Steps:
        1. Connect to the Strata project
        2. Load the air_quality_prediction dataset via read_features()
        3. Split features and label
        4. Train a Ridge regression model
        5. Evaluate in-memory (training predictions are ephemeral)
        6. Save model to disk with joblib

    Args:
        start: Training start date (inclusive).
        end: Training end date (exclusive).
        model_dir: Directory to save the trained model.

    Returns:
        Dict with training metrics (mae, rmse, r2).
    """
    import joblib
    import numpy as np
    import sklearn.linear_model as lm
    import sklearn.metrics as metrics

    # 1. Connect to Strata project
    project = st.connect()

    # 2. Load features
    ds = project.get_dataset("air_quality_prediction")
    features_table = ds.read_features(start=start, end=end)

    # 3. Convert to numpy for sklearn
    df = features_table.to_pandas()

    # Feature columns are prefixed: air_quality_features__pm25_7d_avg, etc.
    feature_cols = [c for c in df.columns if c not in ("date", "country", "city", "street", "pm25")]
    label_col = "pm25"

    # Drop rows with NaN (rolling aggregates produce nulls at start)
    df = df.dropna(subset=feature_cols + [label_col])

    x_train = df[feature_cols].values
    y_train = df[label_col].values

    # 4. Train model
    model = lm.Ridge(alpha=1.0)
    model.fit(x_train, y_train)

    # 5. Evaluate in-memory (ephemeral -- not persisted)
    y_pred = model.predict(x_train)
    mae = float(metrics.mean_absolute_error(y_train, y_pred))
    rmse = float(np.sqrt(metrics.mean_squared_error(y_train, y_pred)))
    r2 = float(metrics.r2_score(y_train, y_pred))

    train_metrics = {"mae": mae, "rmse": rmse, "r2": r2}
    print(f"Training metrics: MAE={mae:.2f}, RMSE={rmse:.2f}, R2={r2:.3f}")

    # 6. Save model
    model_path = Path(model_dir)
    model_path.mkdir(parents=True, exist_ok=True)
    joblib.dump(model, model_path / "pm25_model.joblib")
    print(f"Model saved to {model_path / 'pm25_model.joblib'}")

    return train_metrics


if __name__ == "__main__":
    train()
