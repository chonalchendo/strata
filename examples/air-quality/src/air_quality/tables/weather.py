"""Weather source and feature tables for Edinburgh."""

from datetime import timedelta

import strata as st

city = st.Entity(
    name="City",
    description="City to get weather data for.",
    join_keys=["city"],
)


weather_source = st.BatchSource(
    name="Weather source",
    description="Source of weather data.",
    config=st.LocalSourceConfig(
        path="data/historical-weather-edinburgh.csv", format="csv"
    ),
    timestamp_field="date",
)


class WeatherSchema(st.Schema):
    date = st.Field(description="Date of the weather data.", dtype="datetime")
    city = st.Field(description="City of the weather data.", dtype="string")
    temperature_2m_mean = st.Field(
        description="Mean temperature at 2 meters above ground.", dtype="float64"
    )
    precipitation_sum = st.Field(
        description="Total precipitation sum.", dtype="float64", ge=0
    )
    wind_speed_10m_max = st.Field(
        description="Maximum wind speed at 10 meters above ground.",
        dtype="float64",
        ge=0,
        le=1000,
    )
    wind_direction_10m_dominant = st.Field(
        description="Dominant wind direction at 10 meters above ground.",
        dtype="float64",
    )


weather_st = st.SourceTable(
    name="weather",
    description="Weather characteristics of each day.",
    source=weather_source,
    entity=city,
    timestamp_field="date",
    schema=WeatherSchema,
)

# -- Feature table: weather features for PM2.5 prediction modelling --

weather_ft = st.FeatureTable(
    name="weather_features",
    description="Weather features for air quality prediction",
    source=weather_source,
    entity=city,
    timestamp_field="date",
    schedule="daily",
)

temperature_2m_mean = weather_ft.aggregate(
    name="temperature_2m_mean",
    field=st.Field(dtype="float64", description="Daily mean temperature"),
    column="temperature_2m_mean",
    function="avg",
    window=timedelta(days=1),
)

precipitation_sum = weather_ft.aggregate(
    name="precipitation_sum",
    field=st.Field(dtype="float64", description="Daily precipitation total"),
    column="precipitation_sum",
    function="sum",
    window=timedelta(days=1),
)

wind_speed_10m_max = weather_ft.aggregate(
    name="wind_speed_10m_max",
    field=st.Field(dtype="float64", description="Daily max wind speed"),
    column="wind_speed_10m_max",
    function="max",
    window=timedelta(days=1),
)
