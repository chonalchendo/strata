"""Dataset definition for PM2.5 prediction model.

Groups features from air quality and weather feature tables into a single
Dataset used for training and inference pipelines.
"""

import strata as st

import air_quality.tables.air_quality as aq_tables
import air_quality.tables.weather as weather_tables

air_quality_dataset = st.Dataset(
    name="air_quality_prediction",
    description="Features for PM2.5 prediction model",
    features=[
        aq_tables.air_quality_ft.pm25_7d_avg,
        aq_tables.air_quality_ft.pm25_30d_avg,
        weather_tables.weather_ft.temperature_2m_mean,
        weather_tables.weather_ft.precipitation_sum,
        weather_tables.weather_ft.wind_speed_10m_max,
    ],
    label=aq_tables.air_quality_ft.pm25,
)
