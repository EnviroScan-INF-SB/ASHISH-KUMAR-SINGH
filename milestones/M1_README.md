# Milestone 1 (Week 1–2)

Module 1: Data Collection from APIs and Location Databases

Implemented scripts and docs:

- `scripts/fetch_openaq.py` — OpenAQ locations and measurements
- `scripts/fetch_weather.py` — OpenWeatherMap weather collection
- `scripts/extract_geographic_features.py` — OSMnx-based geographic extraction
- `scripts/run_module1_collection.py` — Orchestrator to run and verify Module 1
- Docs:
  - `MODULE1_README.md` (high-level)
  - `docs/02_openweathermap.md`

Outputs (stored under `data/`):
- `openaq_locations_with_metadata.csv`
- `openaq_air_quality_data.csv`, `openaq_air_quality_data.json`
- `weather_data_enhanced.csv`, `weather_data_enhanced.json`
- `geographic_features_summary.csv`, `geographic_features_detailed.json`
- `module1_verification_report.json`, `module1_completion_summary.md`

How to run

1. Fill `.env` with API keys (copy from `.env.template`).
2. `pip install -r requirements.txt`
3. `python scripts/run_module1_collection.py`

Notes
- Some per-module docs are in `docs/`. Review `MODULE1_README.md` for full details.