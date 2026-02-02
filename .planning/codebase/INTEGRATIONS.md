# External Integrations

**Analysis Date:** 2026-02-02

## APIs & External Services

**Weather Data (example project):**
- Open-Meteo API - Historical and forecast weather data
  - SDK: `openmeteo-requests>=1.7.5`
  - Endpoints: `archive-api.open-meteo.com`, `api.open-meteo.com`
  - Auth: None required

**Geolocation (example project):**
- OpenStreetMap Nominatim API - City geocoding
  - SDK: `geopy>=2.4.1`
  - Auth: User-Agent required

**Air Quality (example project):**
- WAQI API - Air quality measurements
  - Auth: Requires `AQI_API_KEY` token
  - SDK: `requests>=2.32.5`

## Data Storage

**Databases:**
- SQLite - Metadata/feature registry storage
  - Configuration: Via `strata.yaml` under `registry.kind: sqlite`
  - Example paths: `.strata/dev/registry.db`, `.strata/stg/registry.db`

**File Storage:**
- Local filesystem only
  - Storage type: `local`
  - Format: Parquet and JSON files
  - Example paths: `.strata/dev/lakehouse`, `.strata/stg/lakehouse`

**Caching:**
- Requests Cache - HTTP response caching
  - Package: `requests-cache>=1.2.1`
  - Used in example scripts

## Compute & Query Engines

**Query Engine:**
- DuckDB 1.4.4+ (optional) - SQL compute for feature transformations
  - Configuration: Via `strata.yaml` under `compute.kind: duckdb`
  - Extensions: `parquet`, `json`
  - Abstraction: Ibis Framework

## Environment Configuration

**Required env vars:**
- `AQI_API_KEY` - WAQI air quality API token (examples only)

**Configuration files:**
- `.python-version` - Python version (3.14.2)
- `pyproject.toml` - Package dependencies
- `strata.yaml` - Runtime configuration (dev, stg, prd)

## Data Formats

**Supported:**
- Parquet - Via DuckDB extension
- JSON - Via DuckDB extension
- CSV - Used in examples
- Arrow Tables - Internal representation
- Delta Lake - Lakehouse table format

---

*Integration audit: 2026-02-02*
