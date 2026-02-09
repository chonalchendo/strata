"""FastAPI endpoint for real-time fraud prediction.

Serves fraud predictions using features from the Strata online store.
Features are published via ``strata publish`` and looked up in real-time
using ``ds.lookup_features()``.

Usage:
    uvicorn ccfraud.app.api:app --reload --port 8000
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from pathlib import Path

import pydantic as pdt
import strata as st

# Global state (initialized on startup)
_state: dict = {}


@asynccontextmanager
async def lifespan(app):
    """Initialize model and project connection on startup."""
    import joblib

    model_path = Path("models/fraud_model.joblib")
    if not model_path.exists():
        print(
            f"WARNING: Model not found at {model_path}. "
            "Run training pipeline first."
        )
    else:
        _state["model"] = joblib.load(model_path)
        print(f"Loaded model from {model_path}")

    _state["project"] = st.connect()
    print("Connected to Strata project")

    yield

    _state.clear()


import fastapi as fa

app = fa.FastAPI(
    title="Ccfraud Prediction API",
    description="Real-time fraud prediction using Strata online features",
    version="0.1.0",
    lifespan=lifespan,
)


class PredictRequest(pdt.BaseModel, strict=True):
    """Request body for fraud prediction."""

    cc_num: str = pdt.Field(description="Credit card number to check")


class PredictResponse(pdt.BaseModel):
    """Response body with fraud prediction results."""

    cc_num: str
    fraud_probability: float
    is_fraud: bool
    features: dict[str, float | int | None]


class HealthResponse(pdt.BaseModel):
    """Health check response."""

    status: str
    model_loaded: bool
    project_connected: bool


@app.get("/health", response_model=HealthResponse)
def health() -> HealthResponse:
    """Health check endpoint."""
    return HealthResponse(
        status="ok",
        model_loaded="model" in _state,
        project_connected="project" in _state,
    )


@app.post("/predict", response_model=PredictResponse)
def predict(request: PredictRequest) -> PredictResponse:
    """Predict fraud probability for a credit card.

    Looks up pre-computed features from the online store and runs
    the trained XGBoost model for real-time prediction.
    """
    if "model" not in _state:
        raise fa.HTTPException(
            status_code=503,
            detail="Model not loaded. Run training pipeline first.",
        )
    if "project" not in _state:
        raise fa.HTTPException(
            status_code=503,
            detail="Strata project not connected.",
        )

    project = _state["project"]
    model = _state["model"]

    # Look up features from online store
    ds = project.get_dataset("fraud_detection")
    feature_row = ds.lookup_features({"cc_num": request.cc_num})

    # Extract feature values (excluding metadata columns)
    feature_dict: dict[str, float | int | None] = {}
    meta_cols = {"_feature_timestamp", "cc_num", "datetime"}
    for col_name in feature_row.column_names:
        if col_name not in meta_cols:
            values = feature_row.column(col_name).to_pylist()
            feature_dict[col_name] = values[0] if values else None

    # Build feature array for prediction (matching training order)
    feature_values = list(feature_dict.values())

    # Check if all features are None (entity not found)
    if all(v is None for v in feature_values):
        return PredictResponse(
            cc_num=request.cc_num,
            fraud_probability=0.0,
            is_fraud=False,
            features=feature_dict,
        )

    # Replace None with 0 for prediction
    import numpy as np

    x = np.array([[v if v is not None else 0.0 for v in feature_values]])
    fraud_proba = float(model.predict_proba(x)[0, 1])
    is_fraud = bool(model.predict(x)[0])

    return PredictResponse(
        cc_num=request.cc_num,
        fraud_probability=round(fraud_proba, 4),
        is_fraud=is_fraud,
        features=feature_dict,
    )
