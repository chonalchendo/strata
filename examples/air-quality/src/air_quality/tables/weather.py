import strata as st
import strata.backends.local.storage as local_storage

city = st.Entity(
    name="City", description="City to get weather data for.", join_keys=["city"]
)


weather_source = st.BatchSource(
    name="Weather source",
    description="Source of weather data.",
    config=local_storage.LocalSourceConfig(
        path="data/historical-weather-edinburgh.csv", format="csv"
    ),
    timestamp_field="date",
)


class WeatherSchema(st.Schema):
    date = st.Field(description="Date of the weather data.", dtype="datetime")
    city = st.Field(description="City of the weather data.", dtype="string")
    temperature_2m_mean = st.Field(
        description="Mean temperature at 2 meters above ground.", dtype="float"
    )
    precipitation_sum = st.Field(
        description="Total precipitation sum.", dtype="float", ge=0
    )
    wind_speed_10m_max = st.Field(
        description="Maximum wind speed at 10 meters above ground.",
        dtype="float",
        ge=0,
        le=1000,
    )
    wind_direction_10m_dominant = st.Field(
        description="Dominant wind direction at 10 meters above ground.", dtype="string"
    )


weather_st = st.SourceTable(
    name="weather",
    description="Weather characteristics of each day.",
    source=weather_source,
    entity=city,
    timestamp_field="date",
    schema=WeatherSchema,
)
