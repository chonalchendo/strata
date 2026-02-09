# Air Quality Example

End-to-end batch pipeline for PM2.5 air quality prediction in Edinburgh, demonstrating the full Strata feature store workflow.

## Architecture

```
Feature Pipeline (strata up + build)
  -> Materialises air_quality_features and weather_features tables

Training Pipeline (pipelines/training.py)
  -> read_features() for 2023-01-01 to 2023-10-01
  -> Trains Ridge regression model
  -> Evaluates in-memory (ephemeral -- not persisted)
  -> Saves model to models/pm25_model.joblib

Inference Pipeline (pipelines/inference.py)
  -> Loads trained model
  -> read_features() for 2023-10-01 to 2024-01-01
  -> Generates batch predictions
  -> Writes to pm25_predictions table via project.write_table()
```

## Project Structure

```
src/air_quality/
  tables/
    air_quality.py    -- SourceTable + FeatureTable (rolling PM2.5 aggregates)
    weather.py        -- SourceTable + FeatureTable (temperature, precip, wind)
  datasets/
    air_quality.py    -- Dataset definition referencing features from both tables
  monitoring/
    air_quality_pm25.py -- Predictions output table for monitoring
  pipelines/
    training.py       -- Training pipeline (read_features -> train -> save model)
    inference.py      -- Inference pipeline (load model -> predict -> write_table)
data/
  air-quality-edinburgh.csv       -- 365 rows of daily air quality data (2023)
  historical-weather-edinburgh.csv -- 365 rows of daily weather data (2023)
scripts/
  run_pipeline.py    -- End-to-end orchestrator (up -> build -> train -> predict)
  get_data.py        -- Data fetching utilities (Open-Meteo API)
```

## Quick Start

```bash
# From the examples/air-quality/ directory:
python scripts/run_pipeline.py
```

Or run individual steps:

```bash
strata up --yes          # Sync definitions
strata build             # Materialize feature tables
python -c "from air_quality.pipelines.training import train; train()"
python -c "from air_quality.pipelines.inference import predict; predict()"
```

## Data

Sample data covers Edinburgh St-Leonards monitoring station for 2023:
- **Air quality**: PM2.5, PM10, NO2, O3 daily readings with seasonal variation
- **Weather**: Temperature, precipitation, wind speed/direction from Open-Meteo

## Features

The `air_quality_prediction` Dataset includes:
- `pm25_7d_avg` -- 7-day rolling average PM2.5 (air quality table)
- `pm25_30d_avg` -- 30-day rolling average PM2.5 (air quality table)
- `temperature_2m_mean` -- Daily mean temperature (weather table)
- `precipitation_sum` -- Daily precipitation total (weather table)
- `wind_speed_10m_max` -- Daily max wind speed (weather table)
- **Label**: `pm25` -- actual PM2.5 reading (prediction target)
