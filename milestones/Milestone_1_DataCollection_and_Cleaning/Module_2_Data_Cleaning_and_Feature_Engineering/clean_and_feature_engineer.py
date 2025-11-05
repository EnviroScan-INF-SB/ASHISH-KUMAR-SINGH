"""
Module 2: Data Cleaning and Feature Engineering

This script performs the following:
- Remove duplicates and invalid records
- Handle missing values (interpolation or median imputation)
- Standardize timestamps and coordinates
- Normalize pollutant and weather values
- Calculate spatial proximity features from geographic features summary
- Derive temporal features (hour, dayofweek, season)
- Combine pollution, weather and geographic features into a unified DataFrame

Outputs:
- data/processed_data.csv
- data/processed_data_normalized.csv
- data/feature_list.json

Usage:
    python scripts/clean_and_feature_engineer.py

"""

import os
import json
from datetime import datetime
import pandas as pd
import numpy as np
from sklearn.preprocessing import MinMaxScaler

DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data')
PROCESSED_FILE = os.path.join(DATA_DIR, 'processed_data.csv')
PROCESSED_NORM_FILE = os.path.join(DATA_DIR, 'processed_data_normalized.csv')
FEATURE_LIST_FILE = os.path.join(DATA_DIR, 'feature_list.json')

# helper: haversine distance
from math import radians, cos, sin, asin, sqrt

def haversine(lon1, lat1, lon2, lat2):
    # convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    km = 6367 * c
    return km


def season_from_month(month):
    if month in [12, 1, 2]:
        return 'winter'
    if month in [3, 4, 5]:
        return 'spring'
    if month in [6, 7, 8]:
        return 'summer'
    return 'autumn'


def main():
    os.makedirs(DATA_DIR, exist_ok=True)

    # Load datasets
    aq_file = os.path.join(DATA_DIR, 'openaq_air_quality_data.csv')
    weather_file = os.path.join(DATA_DIR, 'weather_data_enhanced.csv')
    geo_file = os.path.join(DATA_DIR, 'geographic_features_summary.csv')

    if not os.path.exists(aq_file):
        print(f"Missing air quality file: {aq_file}. Run Module 1 first.")
        return
    if not os.path.exists(weather_file):
        print(f"Missing weather file: {weather_file}. Run Module 1 first.")
        return

    print('Loading air quality data...')
    df_aq = pd.read_csv(aq_file)
    print('Loading weather data...')
    df_w = pd.read_csv(weather_file) if os.path.exists(weather_file) else pd.DataFrame()
    print('Loading geographic summary (optional)...')
    df_geo = pd.read_csv(geo_file) if os.path.exists(geo_file) else pd.DataFrame()

    # --- Standardize and clean AQ data ---
    # Drop duplicates
    df_aq = df_aq.drop_duplicates()

    # Remove invalid records (no coords or no value)
    if 'latitude' in df_aq.columns and 'longitude' in df_aq.columns:
        df_aq = df_aq.dropna(subset=['latitude', 'longitude'])
    df_aq = df_aq[df_aq['value'].notna()]

    # Standardize timestamp: try 'datetime' or 'date.utc' or 'fetched_timestamp'
    if 'datetime' in df_aq.columns:
        df_aq['timestamp'] = pd.to_datetime(df_aq['datetime'], utc=True, errors='coerce')
    elif 'date.utc' in df_aq.columns:
        df_aq['timestamp'] = pd.to_datetime(df_aq['date.utc'], utc=True, errors='coerce')
    elif 'fetched_timestamp' in df_aq.columns:
        df_aq['timestamp'] = pd.to_datetime(df_aq['fetched_timestamp'], utc=True, errors='coerce')
    else:
        df_aq['timestamp'] = pd.NaT

    # If parameter column uses different naming (e.g., 'parameter' or 'param') unify
    if 'parameter' not in df_aq.columns and 'param' in df_aq.columns:
        df_aq.rename(columns={'param':'parameter'}, inplace=True)

    # Pivot pollutants to columns for easier merging
    print('Pivoting air quality pollutants...')
    # Keep only target pollutants
    target_params = ['pm25','pm10','no2','co','so2','o3']
    df_aq['parameter'] = df_aq['parameter'].astype(str).str.lower()

    df_pivot = df_aq.pivot_table(index=['location_id','location_name','latitude','longitude','timestamp'],
                                 columns='parameter', values='value', aggfunc='mean').reset_index()

    # Ensure all target param columns exist
    for p in target_params:
        if p not in df_pivot.columns:
            df_pivot[p] = np.nan

    # --- Clean weather data ---
    # Standardize timestamps
    if 'api_timestamp' in df_w.columns:
        df_w['timestamp'] = pd.to_datetime(df_w['api_timestamp'], utc=True, errors='coerce')
    elif 'fetched_timestamp' in df_w.columns:
        df_w['timestamp'] = pd.to_datetime(df_w['fetched_timestamp'], utc=True, errors='coerce')
    else:
        df_w['timestamp'] = pd.NaT

    if 'latitude' not in df_w.columns and 'coordinates' in df_w.columns:
        # attempt to parse
        try:
            df_w['latitude'] = df_w['coordinates'].apply(lambda x: eval(x).get('latitude') if pd.notna(x) else np.nan)
            df_w['longitude'] = df_w['coordinates'].apply(lambda x: eval(x).get('longitude') if pd.notna(x) else np.nan)
        except Exception:
            pass

    # Deduplicate and remove invalid
    df_w = df_w.drop_duplicates()
    if 'latitude' in df_w.columns and 'longitude' in df_w.columns:
        df_w = df_w.dropna(subset=['latitude','longitude'])

    # --- Merge AQ and weather by nearest timestamp and location_id ---
    # We'll merge on location_id where possible, otherwise nearest spatial join using coords
    print('Merging AQ and weather datasets...')
    # normalize location ids to comparable type
    if 'location_id' in df_pivot.columns and 'location_id' in df_w.columns:
        merged = pd.merge(df_pivot, df_w, on='location_id', suffixes=('_aq','_w'), how='left')
    else:
        # fallback: nearest spatial merge using coordinates (within small distance)
        merged = df_pivot.copy()
        if 'latitude' in df_w.columns and 'longitude' in df_w.columns:
            merged = pd.merge(merged, df_w, on=['latitude','longitude'], suffixes=('_aq','_w'), how='left')

    # --- Handle missing values ---
    print('Handling missing values...')
    numeric_cols = ['pm25','pm10','no2','co','so2','o3','temperature','humidity','wind_speed']
    # Ensure weather numeric columns exist in merged
    for c in ['temperature','humidity','wind_speed','wind_direction']:
        if c not in merged.columns:
            merged[c] = np.nan

    # Interpolate per location_id for temporal continuity if timestamp present
    if 'timestamp' in merged.columns and merged['timestamp'].notna().any():
        try:
            merged = merged.sort_values(['location_id','timestamp'])
            merged[numeric_cols] = merged.groupby('location_id')[numeric_cols].apply(lambda g: g.interpolate(limit_direction='both'))
        except Exception:
            pass

    # Fill remaining NaNs with median
    for col in numeric_cols:
        if col in merged.columns:
            med = merged[col].median()
            merged[col] = merged[col].fillna(med)

    # Coordinates cleanup: ensure floats
    for c in ['latitude','longitude']:
        if c in merged.columns:
            merged[c] = pd.to_numeric(merged[c], errors='coerce')

    # --- Spatial proximity features ---
    print('Adding spatial proximity features from geographic summary...')
    if not df_geo.empty:
        # df_geo expected to have columns like 'roads_closest_km' or 'roads_closest_km' - earlier we saved '{feature}_closest_km'
        geo_cols = {}
        for col in df_geo.columns:
            if col.endswith('_closest_km'):
                feature = col.replace('_closest_km','')
                geo_cols[feature+'_closest_km'] = col
        # Map geographic info by location id if possible
        if 'location_id' in df_geo.columns and 'location_id' in merged.columns:
            merged = pd.merge(merged, df_geo[['location_id'] + list(geo_cols.values())].drop_duplicates(), on='location_id', how='left')
            # rename to friendly names
            for feature_key,col in geo_cols.items():
                newname = f'{feature_key}_dist_km'
                merged.rename(columns={col:newname}, inplace=True)
        else:
            print('Geographic summary exists but no matching location_id for merging; skipping automated proximity merge.')
    else:
        print('No geographic summary file found; proximity features will be computed from detailed file if available later.')

    # If proximity columns missing, try to compute approximate distances using available feature coordinates if detailed file exists

    # --- Temporal features ---
    print('Deriving temporal features...')
    if 'timestamp' in merged.columns:
        merged['hour'] = merged['timestamp'].dt.hour
        merged['dayofweek'] = merged['timestamp'].dt.dayofweek
        merged['month'] = merged['timestamp'].dt.month
        merged['season'] = merged['month'].apply(lambda m: season_from_month(m) if not np.isnan(m) else None)
    else:
        merged['hour'] = np.nan
        merged['dayofweek'] = np.nan
        merged['month'] = np.nan
        merged['season'] = None

    # --- Normalization ---
    print('Normalizing pollutant and weather values...')
    norm_cols = [c for c in ['pm25','pm10','no2','co','so2','o3','temperature','humidity','wind_speed'] if c in merged.columns]
    scaler = MinMaxScaler()
    merged_norm = merged.copy()
    if norm_cols:
        try:
            merged_norm[[c + '_norm' for c in norm_cols]] = scaler.fit_transform(merged[norm_cols])
        except Exception as e:
            print('Normalization failed:', e)

    # --- Final save ---
    print('Saving processed data...')
    merged.to_csv(PROCESSED_FILE, index=False)
    merged_norm.to_csv(PROCESSED_NORM_FILE, index=False)

    # Save feature list
    features = list(merged.columns)
    with open(FEATURE_LIST_FILE, 'w') as f:
        json.dump({'features':features}, f, indent=2)

    print(f'Processed data saved to: {PROCESSED_FILE}')
    print(f'Normalized data saved to: {PROCESSED_NORM_FILE}')
    print(f'Feature list saved to: {FEATURE_LIST_FILE}')


if __name__ == '__main__':
    main()
