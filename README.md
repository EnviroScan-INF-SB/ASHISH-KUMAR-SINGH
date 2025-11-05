<img width="1920" height="1080" alt="Screenshot 2025-11-05 154332" src="https://github.com/user-attachments/assets/dbd3b33f-4a73-468d-89bf-f500de340201" />













Absolutely! Here's the complete and clean `README.md` content formatted as a single code block so you can copy and paste it directly into your project:

```markdown
# 🌍 Global Real-Time Pollution Tracker

A real-time global air pollution monitoring and source prediction system built using Python, Streamlit, and machine learning. This project fetches live air quality and weather data, processes it, predicts pollution sources using ML models, and visualizes the results on an interactive map.

---

## Features

- Fetches live air quality data from **OpenAQ** and **OpenWeatherMap APIs**
- Calculates spatial features like distance to roads, factories, and farmlands
- Rule-based and ML-based labeling of pollution sources (vehicular, industrial, agricultural, natural, unknown)
- Supports real-time **AQI** visualization with color-coded markers
- Interactive **map view** using Folium
- Auto-refresh feature for live updates
- Machine Learning models: **Random Forest** and **XGBoost**
- Feature importance visualization

---

## Project Structure

---
├── data/
│   ├── global_locations_cleaned.csv
│   ├── weather_data.csv
│   ├── pollution_data.csv
│   ├── merged_realtime_data.csv
│   └── processed_data.csv
│   └── labeled_features.csv
├── models/
│   ├── pollution_model.pkl
│   └── label_encoder.pkl
├── scripts/
│   ├── fetch_openq.py
│   ├── fetch_weather.py
│   ├── fetch_weather_retry_missing.py (only if the data is missing)
│   ├── fetch_pollution.py
│   ├── preproces.py
│   ├── merge_for_labeling.py
│   ├── label_rules_advanced.py
│   └── ml_model.py
│   └── app.py
├── requirements.txt
└── README.md
---

---

## Setup Instructions

1. **Clone the repository:**
   ```bash
   git clone https://github.com/magnabenita/EnviroScan
   cd <repo-folder>
   ```

2. **Create a virtual environment and install dependencies:**
   ```bash
   python -m venv venv
   source venv/bin/activate  # Linux/Mac
   venv\Scripts\activate     # Windows
   pip install -r requirements.txt
   ```

3. **Add your API keys in `app.py` and scripts if needed:**
   - `OPENAQ_KEY` for OpenAQ API
   - `OPENWEATHER_KEY` for OpenWeatherMap API

4. **Fetch and preprocess data:**
   ```bash
   python scripts/fetch_openq.py
   python scripts/fetch_weather.py
   python scripts/fetch_pollution.py
   python scripts/preprocess.py
   python scripts/merge_for_labeling.py
   python scripts/label_rules_advanced.py
   ```

5. **Train ML models:**
   ```bash
   python scripts/ml_model.py
   ```

6. **Run the Streamlit app:**
   ```bash
   streamlit run scripts/app.py
   ```

---

## Usage

### Sidebar Filters:
- Select pollution sources (vehicular, industrial, agricultural, natural, unknown)
- Highlight by pollutant or AQI
- Set refresh interval and number of locations

### Map Visualization:
- Color-coded markers based on source or AQI
- Interactive popups with location, pollutant values, and model confidence

### Data Table:
- Displays live pollution readings, weather data, predicted source, and confidence

---

## Model & ML

### Features:
- Pollutants: `pm2_5`, `pm10`, `no2`, `so2`, `co`, `o3`
- AQI, temperature, humidity, wind speed
- Spatial distances: road, factory, farmland

### Models:
- Random Forest (default) and XGBoost
- Label encoding stored in `models/label_encoder.pkl`

---

## Notes

- `.gitignore` ignores `.pkl`, `.csv`, `venv`, `.cache`, and `.bak` files
- Rate limits are handled in API fetch scripts
- Backup of weather data is created before retrying missing locations
- The project currently supports India locations; can be extended globally

---

## License

MIT License
```
