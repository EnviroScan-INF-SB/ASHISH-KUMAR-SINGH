# EnviroScan Module 1: Data Collection from APIs and Location Databases

**Milestone:** Week 1-2  
**Project:** AI-Powered Pollution Source Identifier using Geospatial Analytics

## Overview

This module implements comprehensive data collection from multiple APIs and location databases to gather:
- Air quality data (PM2.5, PM10, NO₂, CO, SO₂, O₃) from OpenAQ API
- Weather data (temperature, humidity, wind speed, wind direction) from OpenWeatherMap API
- Geographic features (roads, industrial zones, dump sites, agricultural fields) using OpenStreetMap via OSMnx

## Features Implemented

### ✅ Air Quality Data Collection (`fetch_openaq.py`)
- Collects all required pollutants from OpenAQ API
- Tags each data point with latitude, longitude, timestamp, and source API metadata
- Stores data in both CSV and JSON formats
- Handles API rate limiting and error recovery

### ✅ Weather Data Collection (`fetch_weather.py`)
- Collects comprehensive weather parameters from OpenWeatherMap API
- Includes all Module 1 requirements: temperature, humidity, wind_speed, wind_direction
- Enhanced with additional meteorological data
- Proper metadata tagging and structured storage

### ✅ Geographic Features Extraction (`extract_geographic_features.py`)
- Extracts nearby physical features using OSMnx library
- Categories: roads, industrial_zones, dump_sites, agricultural_fields, water_bodies, green_spaces
- Calculates feature counts and distances for each monitoring location
- Comprehensive feature analysis within configurable radius

### ✅ Data Orchestration (`run_module1_collection.py`)
- Main script to run all data collection modules
- Environment validation and error handling
- Data quality verification
- Automated reporting and summary generation

## Quick Start

1. **Install Dependencies**
   ```bash
   pip install -r requirements.txt
   ```

2. **Setup Environment**
   ```bash
   cp .env.template .env
   # Edit .env file with your API keys
   ```

3. **Run Complete Data Collection**
   ```bash
   python scripts/run_module1_collection.py
   ```

   Or run individual modules:
   ```bash
   python scripts/fetch_openaq.py
   python scripts/fetch_weather.py
   python scripts/extract_geographic_features.py
   ```

## API Keys Required

- **OpenAQ API Key** (optional but recommended): Get from [OpenAQ.org](https://openaq.org/)
- **OpenWeatherMap API Key** (required): Get from [OpenWeatherMap](https://openweathermap.org/api)

## Output Files

### Air Quality Data
- `data/openaq_locations_with_metadata.csv` - Monitoring locations
- `data/openaq_air_quality_data.csv` - Air quality measurements
- `data/openaq_air_quality_data.json` - Full metadata

### Weather Data
- `data/weather_data_enhanced.csv` - Weather measurements
- `data/weather_data_enhanced.json` - Full metadata

### Geographic Features
- `data/geographic_features_summary.csv` - Feature counts per location
- `data/geographic_features_detailed.json` - Detailed feature information

### Reports
- `data/module1_verification_report.json` - Data quality verification
- `data/module1_completion_summary.md` - Completion summary

## Data Format

All collected data includes standardized metadata:
- `latitude`, `longitude` - Coordinates
- `timestamp` - Data collection time
- `source_api` - Source API identifier
- Additional API-specific metadata

## Module 1 Requirements Compliance

- ✅ Collect air quality data (PM2.5, PM10, NO₂, CO, SO₂, O₃) from OpenAQ API
- ✅ Collect weather data (temperature, humidity, wind speed, wind direction) from OpenWeatherMap API  
- ✅ Extract nearby physical features using OpenStreetMap via OSMnx
- ✅ Tag each data point with latitude, longitude, timestamp, and source API metadata
- ✅ Store collected data in structured CSV/JSON format

## Next Steps

- Data preprocessing and feature engineering (Module 2)
- Pollution source labeling based on geographic features
- Model training and evaluation

## Project Structure

```
scripts/
├── fetch_openaq.py              # OpenAQ air quality data collection
├── fetch_weather.py             # OpenWeatherMap weather data collection
├── extract_geographic_features.py # OSMnx geographic features extraction
└── run_module1_collection.py    # Main orchestrator

data/                            # Generated data files
requirements.txt                 # Python dependencies
.env.template                   # Environment variables template
```

## Contact

This module implements the data collection requirements for Week 1-2 of the EnviroScan project.