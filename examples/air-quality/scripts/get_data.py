from datetime import date, datetime, timedelta
from pathlib import Path

import openmeteo_requests
import pandas as pd
import requests
import requests_cache
from geopy.geocoders import Nominatim
from retry_requests import retry
from rich import print


def get_historical_weather(
    city: str,
    start_date: date,
    end_date: date,
    latitude: float,
    longitude: float,
) -> pd.DataFrame:
    # latitude, longitude = get_city_coordinates(city)

    # Setup the Open-Meteo API client with cache and retry on error
    cache_session = requests_cache.CachedSession(".cache", expire_after=-1)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)

    # Make sure all required weather variables are listed here
    # The order of variables in hourly or daily is important to assign them correctly below
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": start_date,
        "end_date": end_date,
        "daily": [
            "temperature_2m_mean",
            "precipitation_sum",
            "wind_speed_10m_max",
            "wind_direction_10m_dominant",
        ],
    }
    responses = openmeteo.weather_api(url, params=params)

    # Process first location. Add a for-loop for multiple locations or weather models
    response = responses[0]
    print(f"Coordinates {response.Latitude()}°N {response.Longitude()}°E")
    print(f"Elevation {response.Elevation()} m asl")
    print(f"Timezone {response.Timezone()} {response.TimezoneAbbreviation()}")
    print(f"Timezone difference to GMT+0 {response.UtcOffsetSeconds()} s")

    # Process daily data. The order of variables needs to be the same as requested.
    daily = response.Daily()
    daily_temperature_2m_mean = daily.Variables(0).ValuesAsNumpy()
    daily_precipitation_sum = daily.Variables(1).ValuesAsNumpy()
    daily_wind_speed_10m_max = daily.Variables(2).ValuesAsNumpy()
    daily_wind_direction_10m_dominant = daily.Variables(3).ValuesAsNumpy()

    daily_data = {
        "date": pd.date_range(
            start=pd.to_datetime(daily.Time(), unit="s"),
            end=pd.to_datetime(daily.TimeEnd(), unit="s"),
            freq=pd.Timedelta(seconds=daily.Interval()),
            inclusive="left",
        )
    }
    daily_data["temperature_2m_mean"] = daily_temperature_2m_mean
    daily_data["precipitation_sum"] = daily_precipitation_sum
    daily_data["wind_speed_10m_max"] = daily_wind_speed_10m_max
    daily_data["wind_direction_10m_dominant"] = daily_wind_direction_10m_dominant

    daily_dataframe = pd.DataFrame(data=daily_data)
    daily_dataframe = daily_dataframe.dropna()
    daily_dataframe["city"] = city
    return daily_dataframe


def get_hourly_weather_forecast(city, latitude, longitude):
    # latitude, longitude = get_city_coordinates(city)

    # Setup the Open-Meteo API client with cache and retry on error
    cache_session = requests_cache.CachedSession(".cache", expire_after=3600)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)

    # Make sure all required weather variables are listed here
    # The order of variables in hourly or daily is important to assign them correctly below
    url = "https://api.open-meteo.com/v1/ecmwf"
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "hourly": [
            "temperature_2m",
            "precipitation",
            "wind_speed_10m",
            "wind_direction_10m",
        ],
    }
    responses = openmeteo.weather_api(url, params=params)

    # Process first location. Add a for-loop for multiple locations or weather models
    response = responses[0]
    print(f"Coordinates {response.Latitude()}°N {response.Longitude()}°E")
    print(f"Elevation {response.Elevation()} m asl")
    print(f"Timezone {response.Timezone()} {response.TimezoneAbbreviation()}")
    print(f"Timezone difference to GMT+0 {response.UtcOffsetSeconds()} s")

    # Process hourly data. The order of variables needs to be the same as requested.

    hourly = response.Hourly()
    hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()
    hourly_precipitation = hourly.Variables(1).ValuesAsNumpy()
    hourly_wind_speed_10m = hourly.Variables(2).ValuesAsNumpy()
    hourly_wind_direction_10m = hourly.Variables(3).ValuesAsNumpy()

    hourly_data = {
        "date": pd.date_range(
            start=pd.to_datetime(hourly.Time(), unit="s"),
            end=pd.to_datetime(hourly.TimeEnd(), unit="s"),
            freq=pd.Timedelta(seconds=hourly.Interval()),
            inclusive="left",
        )
    }
    hourly_data["temperature_2m_mean"] = hourly_temperature_2m
    hourly_data["precipitation_sum"] = hourly_precipitation
    hourly_data["wind_speed_10m_max"] = hourly_wind_speed_10m
    hourly_data["wind_direction_10m_dominant"] = hourly_wind_direction_10m

    hourly_dataframe = pd.DataFrame(data=hourly_data)
    hourly_dataframe = hourly_dataframe.dropna()
    return hourly_dataframe


def get_city_coordinates(city_name: str):
    """
    Takes city name and returns its latitude and longitude (rounded to 2 digits after dot).
    """
    # Initialize Nominatim API (for getting lat and long of the city)
    user_agent = "Hopsworks air quality predictor/1.0 (contact: jim@hopsworks.ai)"
    geolocator = Nominatim(user_agent=user_agent)
    city = geolocator.geocode(city_name)

    latitude = round(city.latitude, 2)
    longitude = round(city.longitude, 2)

    return latitude, longitude


def trigger_request(url: str):
    response = requests.get(url)
    if response.status_code == 200:
        # Extract the JSON content from the response
        data = response.json()
    else:
        print("Failed to retrieve data. Status Code:", response.status_code)
        raise requests.exceptions.RequestException(response.status_code)

    return data


def get_pm25(
    aqicn_url: str,
    country: str,
    city: str,
    street: str,
    day: date,
    AQI_API_KEY: str,
):
    """
    Returns DataFrame with air quality (pm25) as dataframe
    """
    # The API endpoint URL
    url = f"{aqicn_url}/?token={AQI_API_KEY}"

    # Make a GET request to fetch the data from the API
    data = trigger_request(url)

    # if we get 'Unknown station' response then retry with city in url
    if data["data"] == "Unknown station":
        url1 = f"https://api.waqi.info/feed/{country}/{street}/?token={AQI_API_KEY}"
        data = trigger_request(url1)

    if data["data"] == "Unknown station":
        url2 = (
            f"https://api.waqi.info/feed/{country}/{city}/{street}/?token={AQI_API_KEY}"
        )
        data = trigger_request(url2)

    # Check if the API response contains the data
    if data["status"] == "ok":
        # Extract the air quality data
        aqi_data = data["data"]
        aq_today_df = pd.DataFrame()
        aq_today_df["pm25"] = [aqi_data["iaqi"].get("pm25", {}).get("v", None)]
        aq_today_df["pm25"] = aq_today_df["pm25"].astype("float32")

        aq_today_df["country"] = country
        aq_today_df["city"] = city
        aq_today_df["street"] = street
        aq_today_df["date"] = day
        aq_today_df["date"] = pd.to_datetime(aq_today_df["date"])
        aq_today_df["url"] = aqicn_url
    else:
        print(
            "Error: There may be an incorrect  URL for your Sensor or it is not contactable right now. The API response does not contain data.  Error message:",
            data["data"],
        )
        raise requests.exceptions.RequestException(data["data"])

    return aq_today_df


if __name__ == "__main__":
    START = datetime(2014, 8, 8).date()
    today = date.today()
    yesterday = today - timedelta(days=1)
    END = yesterday
    CITY = "Edinburgh"
    LATITUDE = 55.9533
    LONGITUDE = -3.1883
    weather_df: pd.DataFrame = get_historical_weather(CITY, START, END, LATITUDE, LONGITUDE)

    output_path = Path(__file__).parent.parent / "data" / "historical-weather-edinburgh.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    weather_df.to_csv(output_path, index=False)
    print(f"Weather data written to {output_path}")
