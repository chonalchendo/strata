"""Training pipeline for fraud detection model.

Loads features via read_features(), trains an XGBoost classifier,
evaluates on a hold-out set, and saves the model to disk with joblib.

Usage:
    from ccfraud.pipelines.training import train
    train()
"""

from __future__ import annotations

from pathlib import Path

import rich.box as box
import rich.console as rc
import rich.panel as panel
import rich.table as rt

import strata as st

console = rc.Console()


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

    console.print()
    console.rule("[bold blue]Fraud Detection Training Pipeline[/]")
    console.print()

    # 1. Connect to Strata project
    with console.status("[bold green]Connecting to Strata project..."):
        project = st.connect()
    console.print("[green]Connected to Strata project[/]")

    # 2. Load features
    with console.status(f"[bold green]Loading dataset 'fraud_detection' ({start} to {end})..."):
        ds = project.get_dataset("fraud_detection")
        features_table = ds.read_features(start=start, end=end)
    console.print(f"[green]Loaded dataset[/] with [bold]{features_table.num_rows:,}[/] rows, [bold]{features_table.num_columns}[/] columns")

    # 3. Convert to pandas for sklearn/xgboost
    df = features_table.to_pandas()

    label_col = "is_fraud"
    meta_cols = {"datetime", "cc_num", label_col}
    feature_cols = [c for c in df.columns if c not in meta_cols]

    # Show schema table
    schema_table = rt.Table(title="Dataset Schema", box=box.ROUNDED)
    schema_table.add_column("Column", style="cyan")
    schema_table.add_column("Type", style="yellow")
    schema_table.add_column("Role", style="green")
    for col in df.columns:
        role = "label" if col == label_col else ("meta" if col in meta_cols else "feature")
        schema_table.add_row(col, str(df[col].dtype), role)
    console.print(schema_table)

    # Show data preview
    console.print()
    preview_table = rt.Table(title="Data Preview (first 5 rows)", box=box.SIMPLE)
    for col in df.columns:
        preview_table.add_column(col, max_width=16)
    for _, row in df.head(5).iterrows():
        preview_table.add_row(
            *[f"{v:.4f}" if isinstance(v, float) else str(v) for v in row]
        )
    console.print(preview_table)

    # Show null counts before dropping
    null_counts = df[feature_cols + [label_col]].isnull().sum()
    if null_counts.any():
        console.print()
        null_table = rt.Table(title="Null Counts (before dropna)", box=box.ROUNDED)
        null_table.add_column("Column", style="cyan")
        null_table.add_column("Nulls", style="red", justify="right")
        null_table.add_column("Pct", style="yellow", justify="right")
        for col_name in null_counts.index:
            count = null_counts[col_name]
            if count > 0:
                pct = count / len(df) * 100
                null_table.add_row(col_name, str(count), f"{pct:.1f}%")
        console.print(null_table)

    # Drop rows with NaN (rolling aggregates produce nulls at start)
    rows_before = len(df)
    df = df.dropna(subset=feature_cols + [label_col])
    rows_dropped = rows_before - len(df)
    if rows_dropped > 0:
        console.print(f"[yellow]Dropped {rows_dropped:,} rows with NaN ({rows_dropped/rows_before*100:.1f}%)[/]")
    console.print(f"[green]Training rows:[/] [bold]{len(df):,}[/]")

    if len(df) == 0:
        console.print("[bold red]No valid training rows. Check data and date range.[/]")
        return {"precision": 0.0, "recall": 0.0, "f1": 0.0, "auc": 0.0}

    x_train = df[feature_cols].values
    y_train = df[label_col].values.astype(int)

    # Show class balance
    n_fraud = int(y_train.sum())
    n_legit = len(y_train) - n_fraud
    console.print()
    balance_table = rt.Table(title="Class Balance", box=box.ROUNDED)
    balance_table.add_column("Class", style="cyan")
    balance_table.add_column("Count", justify="right")
    balance_table.add_column("Pct", justify="right")
    balance_table.add_row("Legit (0)", f"{n_legit:,}", f"{n_legit/len(y_train)*100:.1f}%")
    balance_table.add_row("Fraud (1)", f"{n_fraud:,}", f"{n_fraud/len(y_train)*100:.1f}%")
    console.print(balance_table)

    # 4. Train XGBoost classifier
    scale_pos_weight = n_legit / max(n_fraud, 1)
    console.print()
    console.print(panel.Panel(
        f"[bold]XGBClassifier[/]\n"
        f"  n_estimators:     [cyan]100[/]\n"
        f"  max_depth:        [cyan]4[/]\n"
        f"  learning_rate:    [cyan]0.1[/]\n"
        f"  scale_pos_weight: [cyan]{scale_pos_weight:.2f}[/]\n"
        f"  features:         [cyan]{len(feature_cols)}[/]",
        title="Model Config",
        border_style="blue",
    ))

    with console.status("[bold green]Training XGBoost model..."):
        model = xgb.XGBClassifier(
            n_estimators=100,
            max_depth=4,
            learning_rate=0.1,
            scale_pos_weight=scale_pos_weight,
            eval_metric="logloss",
            random_state=42,
        )
        model.fit(x_train, y_train)
    console.print("[green]Model training complete[/]")

    # 5. Evaluate on training data (in-memory, ephemeral)
    y_pred = model.predict(x_train)
    y_proba = model.predict_proba(x_train)[:, 1]

    precision = float(metrics.precision_score(y_train, y_pred, zero_division=0))
    recall = float(metrics.recall_score(y_train, y_pred, zero_division=0))
    f1 = float(metrics.f1_score(y_train, y_pred, zero_division=0))
    auc = (
        float(metrics.roc_auc_score(y_train, y_proba))
        if len(np.unique(y_train)) > 1
        else 0.0
    )

    train_metrics = {
        "precision": precision,
        "recall": recall,
        "f1": f1,
        "auc": auc,
    }

    # Metrics panel
    console.print()
    metrics_table = rt.Table(title="Training Metrics", box=box.HEAVY_EDGE)
    metrics_table.add_column("Metric", style="bold cyan")
    metrics_table.add_column("Value", justify="right", style="bold green")
    metrics_table.add_row("Precision", f"{precision:.4f}")
    metrics_table.add_row("Recall", f"{recall:.4f}")
    metrics_table.add_row("F1 Score", f"{f1:.4f}")
    metrics_table.add_row("AUC-ROC", f"{auc:.4f}")
    console.print(metrics_table)

    # Feature importance
    console.print()
    importance = model.feature_importances_
    sorted_idx = np.argsort(importance)[::-1]
    imp_table = rt.Table(title="Feature Importance", box=box.ROUNDED)
    imp_table.add_column("Rank", justify="right", style="dim")
    imp_table.add_column("Feature", style="cyan")
    imp_table.add_column("Importance", justify="right", style="yellow")
    imp_table.add_column("Bar", min_width=20)
    max_imp = importance[sorted_idx[0]] if len(sorted_idx) > 0 else 1.0
    for rank, idx in enumerate(sorted_idx, 1):
        imp_val = importance[idx]
        bar_len = int(imp_val / max_imp * 20) if max_imp > 0 else 0
        bar = "[green]" + "█" * bar_len + "[/]" + "░" * (20 - bar_len)
        imp_table.add_row(str(rank), feature_cols[idx], f"{imp_val:.4f}", bar)
    console.print(imp_table)

    # Confusion matrix
    cm = metrics.confusion_matrix(y_train, y_pred)
    console.print()
    cm_table = rt.Table(title="Confusion Matrix", box=box.HEAVY_EDGE)
    cm_table.add_column("", style="bold")
    cm_table.add_column("Pred: Legit", justify="right")
    cm_table.add_column("Pred: Fraud", justify="right")
    cm_table.add_row("Actual: Legit", f"[green]{cm[0][0]:,}[/]", f"[red]{cm[0][1]:,}[/]")
    cm_table.add_row("Actual: Fraud", f"[red]{cm[1][0]:,}[/]", f"[green]{cm[1][1]:,}[/]")
    console.print(cm_table)

    # 6. Save model
    model_path = Path(model_dir)
    model_path.mkdir(parents=True, exist_ok=True)
    save_path = model_path / "fraud_model.joblib"
    joblib.dump(model, save_path)

    console.print()
    console.print(f"[green]Model saved to[/] [bold]{save_path}[/]")
    console.rule("[bold blue]Pipeline Complete[/]")
    console.print()

    return train_metrics


if __name__ == "__main__":
    train()
