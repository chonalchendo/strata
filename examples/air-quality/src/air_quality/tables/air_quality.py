import strata as st
import strata.plugins.local.storage as local_storage

# historical_url = (
#     "https://aqicn.org/historical/#city:united-kingdom/edinburgh-st-leonards"
# )
# aqicn_url = "https://api.waqi.info/feed/@3176"
# country = "United Kingdom"
# city = "Edinburgh"
# street = "St-Leonards"

street = st.Entity(
    name="Street",
    description="Street associated with air quality data.",
    join_keys=["country", "city", "street"],
)


air_quality_source = st.BatchSource(
    name="Edinburgh air quality",
    description="Historical air quality data for Edinburgh",
    config=local_storage.LocalSourceConfig(
        path="data/air-quality-edinburgh.csv", format="csv"
    ),
    timestamp_field="date",
)


class AirQualitySchema(st.Schema):
    date = st.Field(description="Date of the measurement", dtype="datetime")
    country = st.Field(description="Country of the measurement", dtype="string")
    city = st.Field(description="City of the measurement", dtype="string")
    street = st.Field(description="Street of the measurement", dtype="string")
    pm25 = st.Field(description="PM2.5 concentration", dtype="float", ge=0, le=500)


air_quality_st = st.SourceTable(
    name="air_quality",
    description="Air quality characteristics of each day",
    source=air_quality_source,
    entity=street,
    timestamp_field="date",
    schema=AirQualitySchema,
)
