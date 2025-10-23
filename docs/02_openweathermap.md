# OpenWeatherMap Weather Collection

## Purpose
Collect weather parameters (temperature, humidity, wind speed, wind direction) for monitoring locations using OpenWeatherMap API.

## Script
- `scripts/fetch_weather.py`

## What it does
- Reads `data/global_locations_cleaned.csv` to get monitoring coordinates.
- Fetches current weather for each location.
- Saves outputs:
  - `data/weather_data_enhanced.csv`
  - `data/weather_data_enhanced.json`

## How to run
1. Add your OpenWeatherMap API key to `.env` as `OWM_API_KEY`.
2. From project root:
```powershell
python scripts/fetch_weather.py
```

## Notes
- Skips locations without valid coordinates.
- Avoids duplicate fetches by loading existing CSV if present.
- Saves temp files periodically.

## Verification
- Check `data/weather_data_enhanced.csv` for `temperature`, `humidity`, `wind_speed`, `wind_direction`, `api_timestamp` and `fetched_timestamp`.