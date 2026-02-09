"""Air quality source and feature tables for Edinburgh St-Leonards monitoring station."""

from datetime import timedelta

import strata as st

street = st.Entity(
    name="Street",
    description="Street associated with air quality data.",
    join_keys=["country", "city", "street"],
)


air_quality_source = st.BatchSource(
    name="Edinburgh air quality",
    description="Historical air quality data for Edinburgh",
    config=st.LocalSourceConfig(path="data/air-quality-edinburgh.csv", format="csv"),
    timestamp_field="date",
)


class AirQualitySchema(st.Schema):
    date = st.Field(description="Date of the measurement", dtype="datetime")
    country = st.Field(description="Country of the measurement", dtype="string")
    city = st.Field(description="City of the measurement", dtype="string")
    street = st.Field(description="Street of the measurement", dtype="string")
    pm25 = st.Field(description="PM2.5 concentration", dtype="float64", ge=0, le=500)
    pm10 = st.Field(description="PM10 concentration", dtype="float64", ge=0, le=600)
    no2 = st.Field(description="NO2 concentration", dtype="float64", ge=0)
    o3 = st.Field(description="Ozone concentration", dtype="float64", ge=0)


air_quality_st = st.SourceTable(
    name="air_quality",
    description="Air quality characteristics of each day",
    source=air_quality_source,
    entity=street,
    timestamp_field="date",
    schema=AirQualitySchema,
)

# -- Feature table: rolling PM2.5 aggregates for prediction modelling --

air_quality_ft = st.FeatureTable(
    name="air_quality_features",
    description="Rolling air quality aggregates for PM2.5 prediction",
    source=air_quality_source,
    entity=street,
    timestamp_field="date",
    schedule="daily",
)

pm25 = air_quality_ft.aggregate(
    name="pm25",
    field=st.Field(dtype="float64", description="Latest PM2.5 reading"),
    column="pm25",
    function="avg",
    window=timedelta(days=1),
)

pm25_7d_avg = air_quality_ft.aggregate(
    name="pm25_7d_avg",
    field=st.Field(dtype="float64", description="7-day rolling average PM2.5"),
    column="pm25",
    function="avg",
    window=timedelta(days=7),
)

pm25_30d_avg = air_quality_ft.aggregate(
    name="pm25_30d_avg",
    field=st.Field(dtype="float64", description="30-day rolling average PM2.5"),
    column="pm25",
    function="avg",
    window=timedelta(days=30),
)
