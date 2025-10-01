"""
Module 1: OpenWeatherMap Weather Data Collection
Collects weather data (temperature, humidity, wind speed, wind direction) from OpenWeatherMap API
Tags each data point with latitude, longitude, timestamp, and source API metadata
"""

import pandas as pd
import requests
import time
import os
import ast
import json
from datetime import datetime, timezone
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

API_KEY = os.getenv("OWM_API_KEY")
LOCATIONS_FILE = "data/global_locations_cleaned.csv"
WEATHER_FILE = "data/weather_data_enhanced.csv"
WEATHER_JSON_FILE = "data/weather_data_enhanced.json"

def parse_coords(coord_str):
    """Parse latitude and longitude from coordinates column"""
    if pd.isnull(coord_str) or coord_str == '':
        return None, None
    try:
        if isinstance(coord_str, str):
            coord = ast.literal_eval(coord_str)
        else:
            coord = coord_str
        return coord.get('latitude'), coord.get('longitude')
    except:
        return None, None

def fetch_weather_data(lat, lon, location_info):
    """Fetch comprehensive weather data for a specific location"""
    url = f"http://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={API_KEY}&units=metric"
    
    try:
        resp = requests.get(url, timeout=10)
        if resp.status_code == 200:
            weather = resp.json()
            
            # Extract comprehensive weather data
            weather_data = {
                'location_id': location_info.get('id'),
                'location_name': location_info.get('name', ''),
                'latitude': lat,
                'longitude': lon,
                'country': location_info.get('country', ''),
                'city': location_info.get('city', ''),
                
                # Core weather parameters for Module 1
                'temperature': weather['main']['temp'],
                'humidity': weather['main']['humidity'],
                'wind_speed': weather.get('wind', {}).get('speed', 0),
                'wind_direction': weather.get('wind', {}).get('deg', None),
                
                # Additional meteorological data
                'pressure': weather['main'].get('pressure'),
                'feels_like': weather['main'].get('feels_like'),
                'temp_min': weather['main'].get('temp_min'),
                'temp_max': weather['main'].get('temp_max'),
                'visibility': weather.get('visibility'),
                'weather_main': weather['weather'][0]['main'] if weather.get('weather') else None,
                'weather_description': weather['weather'][0]['description'] if weather.get('weather') else None,
                'cloudiness': weather.get('clouds', {}).get('all'),
                
                # Metadata
                'source_api': 'OpenWeatherMap',
                'api_timestamp': datetime.fromtimestamp(weather['dt'], tz=timezone.utc).isoformat(),
                'fetched_timestamp': datetime.now(timezone.utc).isoformat(),
                'timezone_offset': weather.get('timezone', 0)
            }
            
            return weather_data
        else:
            print(f"Failed to fetch weather for {location_info.get('name', 'Unknown')}: {resp.status_code} - {resp.text}")
            return None
            
    except Exception as e:
        print(f"Exception fetching weather for {location_info.get('name', 'Unknown')}: {e}")
        return None

def collect_weather_data():
    """Main function to collect comprehensive weather data"""
    
    # Check if locations file exists
    if not os.path.exists(LOCATIONS_FILE):
        print(f"Locations file not found: {LOCATIONS_FILE}")
        print("Please run fetch_openaq.py first to generate location data.")
        return
    
    # Load locations
    print("Loading location data...")
    df_locations = pd.read_csv(LOCATIONS_FILE)
    
    # Parse coordinates
    df_locations['latitude'], df_locations['longitude'] = zip(*df_locations['coordinates'].apply(parse_coords))
    
    # Filter locations with valid coordinates
    valid_locations = df_locations.dropna(subset=['latitude', 'longitude'])
    print(f"Found {len(valid_locations)} locations with valid coordinates")
    
    # Initialize or load existing weather data
    all_weather_data = []
    fetched_ids = set()
    
    if os.path.exists(WEATHER_FILE):
        try:
            existing_df = pd.read_csv(WEATHER_FILE)
            if not existing_df.empty:
                fetched_ids = set(existing_df['location_id'])
                all_weather_data = existing_df.to_dict('records')
                print(f"Loaded {len(existing_df)} existing weather records")
        except Exception as e:
            print(f"Error loading existing weather data: {e}")
    
    print(f"Starting weather data collection for {len(valid_locations)} locations...")
    print("Required parameters: temperature, humidity, wind_speed, wind_direction")
    
    # Fetch weather data for each location
    processed_count = 0
    new_records = 0
    
    for idx, row in valid_locations.iterrows():
        loc_id = row['id']
        
        # Skip if already fetched
        if loc_id in fetched_ids:
            continue
        
        lat, lon = row['latitude'], row['longitude']
        location_info = {
            'id': loc_id,
            'name': row.get('name', ''),
            'country': row.get('country', ''),
            'city': row.get('city', '')
        }
        
        weather_data = fetch_weather_data(lat, lon, location_info)
        
        if weather_data:
            all_weather_data.append(weather_data)
            fetched_ids.add(loc_id)
            new_records += 1
        
        processed_count += 1
        
        # Progress update
        if processed_count % 25 == 0:
            print(f"Processed {processed_count}/{len(valid_locations)} locations ({new_records} new records)")
        
        # Save progress every 50 locations
        if processed_count % 50 == 0:
            temp_df = pd.DataFrame(all_weather_data)
            temp_df.to_csv(f"{WEATHER_FILE}.temp", index=False)
            print(f"Temporary save: {len(all_weather_data)} total weather records")
        
        # Rate limiting
        time.sleep(1)
    
    # Final save
    if all_weather_data:
        weather_df = pd.DataFrame(all_weather_data)
        weather_df.to_csv(WEATHER_FILE, index=False)
        
        # Also save as JSON for additional metadata preservation
        with open(WEATHER_JSON_FILE, 'w') as f:
            json.dump(all_weather_data, f, indent=2)
        
        print(f"\nWeather data collection completed!")
        print(f"Total records: {len(weather_df)}")
        print(f"New records collected: {new_records}")
        print(f"Saved to:")
        print(f"  - {WEATHER_FILE}")
        print(f"  - {WEATHER_JSON_FILE}")
        
        # Data quality summary
        print(f"\nData quality summary:")
        print(f"  - Records with temperature: {weather_df['temperature'].notna().sum()}")
        print(f"  - Records with humidity: {weather_df['humidity'].notna().sum()}")
        print(f"  - Records with wind speed: {weather_df['wind_speed'].notna().sum()}")
        print(f"  - Records with wind direction: {weather_df['wind_direction'].notna().sum()}")
        
        # Clean up temp file
        temp_file = f"{WEATHER_FILE}.temp"
        if os.path.exists(temp_file):
            os.remove(temp_file)
    
    else:
        print("No weather data collected.")

if __name__ == "__main__":
    collect_weather_data()
