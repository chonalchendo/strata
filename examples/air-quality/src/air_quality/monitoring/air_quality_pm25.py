"""Monitoring table for PM2.5 prediction output.

The inference pipeline writes batch predictions to this table via
project.write_table(). Used for monitoring model performance over time.
"""

import strata as st


predictions_entity = st.Entity(
    name="PredictionEntity",
    description="Entity for PM2.5 predictions keyed by location",
    join_keys=["country", "city", "street"],
)


class PredictionsSchema(st.Schema):
    date = st.Field(description="Prediction date", dtype="datetime")
    country = st.Field(description="Country", dtype="string")
    city = st.Field(description="City", dtype="string")
    street = st.Field(description="Street", dtype="string")
    pm25_predicted = st.Field(
        description="Predicted PM2.5 concentration", dtype="float64"
    )
    pm25_actual = st.Field(
        description="Actual PM2.5 concentration (if available)", dtype="float64"
    )


pm25_predictions_st = st.SourceTable(
    name="pm25_predictions",
    description="PM2.5 batch predictions for monitoring",
    source=st.BatchSource(
        name="PM2.5 predictions source",
        description="Predictions written by the inference pipeline",
        config=st.LocalSourceConfig(
            path=".strata/dev/lakehouse/pm25_predictions", format="parquet"
        ),
        timestamp_field="date",
    ),
    entity=predictions_entity,
    timestamp_field="date",
    schema=PredictionsSchema,
)
