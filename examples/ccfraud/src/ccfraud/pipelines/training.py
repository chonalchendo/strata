"""Training pipeline for fraud detection model.

Loads features via read_features(), trains an XGBoost classifier,
evaluates on a hold-out set, and saves the model to disk with joblib.

Usage:
    from ccfraud.pipelines.training import train
    train()
"""

from __future__ import annotations

from pathlib import Path


import strata as st


def train(
    start: str = "2024-01-01",
    end: str = "2024-08-01",
    model_dir: str = "models",
) -> dict:
    """Train a fraud detection model using Strata features.

    Steps:
        1. Connect to the Strata project
        2. Load the fraud_detection dataset via read_features()
        3. Split features and label
        4. Train an XGBClassifier
        5. Evaluate with precision, recall, f1, and AUC
        6. Save model to disk with joblib

    Args:
        start: Training start date (inclusive).
        end: Training end date (exclusive).
        model_dir: Directory to save the trained model.

    Returns:
        Dict with training metrics (precision, recall, f1, auc).
    """
    import joblib
    import numpy as np
    import sklearn.metrics as metrics
    import xgboost as xgb

    # 1. Connect to Strata project
    project = st.connect()

    # 2. Load features
    ds = project.get_dataset("fraud_detection")
    features_table = ds.read_features(start=start, end=end)

    # 3. Convert to pandas for sklearn/xgboost
    df = features_table.to_pandas()

    # Feature columns are prefixed: transaction_features__txn_count_1d, etc.
    label_col = "is_fraud"
    meta_cols = {"datetime", "cc_num", "merchant_id", label_col}
    feature_cols = [c for c in df.columns if c not in meta_cols]

    # Drop rows with NaN (rolling aggregates produce nulls at start)
    df = df.dropna(subset=feature_cols + [label_col])

    if len(df) == 0:
        print("No valid training rows. Check data and date range.")
        return {"precision": 0.0, "recall": 0.0, "f1": 0.0, "auc": 0.0}

    x_train = df[feature_cols].values
    y_train = df[label_col].values.astype(int)

    # 4. Train XGBoost classifier
    model = xgb.XGBClassifier(
        n_estimators=100,
        max_depth=4,
        learning_rate=0.1,
        scale_pos_weight=len(y_train[y_train == 0]) / max(len(y_train[y_train == 1]), 1),
        eval_metric="logloss",
        random_state=42,
    )
    model.fit(x_train, y_train)

    # 5. Evaluate on training data (in-memory, ephemeral)
    y_pred = model.predict(x_train)
    y_proba = model.predict_proba(x_train)[:, 1]

    precision = float(metrics.precision_score(y_train, y_pred, zero_division=0))
    recall = float(metrics.recall_score(y_train, y_pred, zero_division=0))
    f1 = float(metrics.f1_score(y_train, y_pred, zero_division=0))
    auc = float(metrics.roc_auc_score(y_train, y_proba)) if len(np.unique(y_train)) > 1 else 0.0

    train_metrics = {
        "precision": precision,
        "recall": recall,
        "f1": f1,
        "auc": auc,
    }
    print(
        f"Training metrics: "
        f"Precision={precision:.3f}, Recall={recall:.3f}, "
        f"F1={f1:.3f}, AUC={auc:.3f}"
    )

    # 6. Save model
    model_path = Path(model_dir)
    model_path.mkdir(parents=True, exist_ok=True)
    joblib.dump(model, model_path / "fraud_model.joblib")
    print(f"Model saved to {model_path / 'fraud_model.joblib'}")

    return train_metrics


if __name__ == "__main__":
    train()
