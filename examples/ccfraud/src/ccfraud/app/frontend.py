"""Streamlit frontend for fraud detection demo.

Interactive UI that:
    - Lets user enter or select a card number
    - Calls the FastAPI prediction endpoint
    - Displays fraud probability and feature values
    - Shows a visual risk indicator

Usage:
    streamlit run src/ccfraud/app/frontend.py

Requires the FastAPI server to be running:
    uvicorn ccfraud.app.api:app --port 8000
"""

from __future__ import annotations

import requests as req
import streamlit as slt

API_URL = "http://localhost:8000"


def main() -> None:
    """Render the Streamlit fraud detection dashboard."""
    slt.set_page_config(
        page_title="Credit Card Fraud Detection",
        page_icon="",
        layout="wide",
    )

    slt.title("Credit Card Fraud Detection")
    slt.markdown(
        "Real-time fraud prediction using features from the Strata online store."
    )

    # Sidebar: API health check
    with slt.sidebar:
        slt.header("API Status")
        try:
            health = req.get(f"{API_URL}/health", timeout=5).json()
            if health["status"] == "ok":
                slt.success("API: Connected")
            else:
                slt.error("API: Unhealthy")
            slt.text(f"Model loaded: {health.get('model_loaded', False)}")
            slt.text(f"Project connected: {health.get('project_connected', False)}")
        except req.exceptions.ConnectionError:
            slt.error("API: Not running")
            slt.markdown(
                "Start the API server:\n\n"
                "```\nuvicorn ccfraud.app.api:app --port 8000\n```"
            )

    # Main area: prediction form
    slt.header("Check a Transaction")

    col1, col2 = slt.columns([2, 3])

    with col1:
        cc_num = slt.text_input(
            "Credit Card Number",
            placeholder="Enter a card number from the dataset",
            help="Use a cc_num from the generated transactions.csv",
        )

        check_button = slt.button("Check for Fraud", type="primary")

    if check_button and cc_num:
        with col2:
            try:
                response = req.post(
                    f"{API_URL}/predict",
                    json={"cc_num": cc_num},
                    timeout=10,
                )

                if response.status_code == 200:
                    result = response.json()
                    fraud_prob = result["fraud_probability"]
                    is_fraud = result["is_fraud"]

                    # Risk indicator
                    if fraud_prob > 0.7:
                        slt.error(f"HIGH RISK -- Fraud probability: {fraud_prob:.1%}")
                    elif fraud_prob > 0.3:
                        slt.warning(f"MEDIUM RISK -- Fraud probability: {fraud_prob:.1%}")
                    else:
                        slt.success(f"LOW RISK -- Fraud probability: {fraud_prob:.1%}")

                    slt.metric("Fraud Predicted", "Yes" if is_fraud else "No")

                    # Feature values
                    slt.subheader("Feature Values")
                    features = result.get("features", {})
                    if features:
                        feature_cols = slt.columns(min(len(features), 4))
                        for idx, (name, value) in enumerate(features.items()):
                            col_idx = idx % len(feature_cols)
                            with feature_cols[col_idx]:
                                display_name = name.replace("__", " / ")
                                display_val = (
                                    f"{value:.2f}" if isinstance(value, float) else str(value)
                                )
                                slt.metric(display_name, display_val)
                    else:
                        slt.info("No features found for this card number.")

                elif response.status_code == 503:
                    slt.error(response.json().get("detail", "Service unavailable"))
                else:
                    slt.error(f"API error: {response.status_code}")

            except req.exceptions.ConnectionError:
                slt.error("Cannot connect to the API. Is the server running?")

    elif check_button and not cc_num:
        with col2:
            slt.warning("Please enter a credit card number.")


if __name__ == "__main__":
    main()
