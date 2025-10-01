"""
Module 1: OpenAQ Air Quality Data Collection
Collects air quality data (PM2.5, PM10, NO₂, CO, SO₂, O₃) from OpenAQ API
Tags each data point with latitude, longitude, timestamp, and source API metadata
"""

import requests
import pandas as pd
import time
import os
import json
from datetime import datetime, timezone
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
API_KEY = os.getenv("OPENAQ_API_KEY")

# API Configuration
BASE_URL_LOCATIONS = "https://api.openaq.org/v3/locations"
BASE_URL_MEASUREMENTS = "https://api.openaq.org/v3/measurements"

# Target pollutants for Module 1
TARGET_POLLUTANTS = ['pm25', 'pm10', 'no2', 'co', 'so2', 'o3']

def fetch_locations(country_code="IN", limit=100):
    """Fetch all available monitoring locations from OpenAQ API"""
    page = 1
    all_locations = []
    headers = {"X-API-Key": API_KEY} if API_KEY else {}

    print("Fetching monitoring locations from OpenAQ API...")
    
    while True:
        params = {"country": country_code, "limit": limit, "page": page}
        resp = requests.get(BASE_URL_LOCATIONS, headers=headers, params=params)

        if resp.status_code == 429:
            print("Rate limit hit. Waiting 10 seconds...")
            time.sleep(10)
            continue
        elif resp.status_code != 200:
            print(f"Error fetching locations: {resp.status_code} - {resp.text}")
            break

        data = resp.json()
        results = data.get("results", [])
        if not results:
            break

        # Process each location to extract metadata
        for location in results:
            location_data = {
                'id': location.get('id'),
                'name': location.get('name'),
                'country': location.get('country'),
                'city': location.get('city'),
                'latitude': location.get('coordinates', {}).get('latitude'),
                'longitude': location.get('coordinates', {}).get('longitude'),
                'parameters': [param['name'] for param in location.get('parameters', [])],
                'source_api': 'OpenAQ',
                'fetched_timestamp': datetime.now(timezone.utc).isoformat()
            }
            all_locations.append(location_data)

        print(f"Page {page} fetched, total locations so far: {len(all_locations)}")
        page += 1
        time.sleep(1)  # Rate limiting

    return all_locations

def fetch_measurements_for_location(location_id, limit=100):
    """Fetch latest measurements for a specific location"""
    headers = {"X-API-Key": API_KEY} if API_KEY else {}
    params = {
        'location_id': location_id,
        'limit': limit,
        'order_by': 'datetime',
        'sort': 'desc'
    }
    
    try:
        resp = requests.get(BASE_URL_MEASUREMENTS, headers=headers, params=params)
        if resp.status_code == 200:
            data = resp.json()
            return data.get("results", [])
        else:
            print(f"Error fetching measurements for location {location_id}: {resp.status_code}")
            return []
    except Exception as e:
        print(f"Exception fetching measurements for location {location_id}: {e}")
        return []

def collect_air_quality_data():
    """Main function to collect comprehensive air quality data"""
    
    # Step 1: Fetch all locations
    locations = fetch_locations()
    
    # Save locations data
    locations_df = pd.DataFrame(locations)
    locations_df.to_csv("data/openaq_locations_with_metadata.csv", index=False)
    print(f"Saved {len(locations_df)} locations to data/openaq_locations_with_metadata.csv")
    
    # Step 2: Fetch measurements for locations that have target pollutants
    all_measurements = []
    
    print("\nFetching air quality measurements...")
    for i, location in enumerate(locations):
        location_id = location['id']
        location_params = location.get('parameters', [])
        
        # Check if location has any of our target pollutants
        has_target_pollutant = any(param in TARGET_POLLUTANTS for param in location_params)
        
        if has_target_pollutant:
            measurements = fetch_measurements_for_location(location_id)
            
            for measurement in measurements:
                measurement_data = {
                    'location_id': location_id,
                    'location_name': location['name'],
                    'latitude': location['latitude'],
                    'longitude': location['longitude'],
                    'country': location['country'],
                    'city': location['city'],
                    'parameter': measurement.get('parameter'),
                    'value': measurement.get('value'),
                    'unit': measurement.get('unit'),
                    'datetime': measurement.get('date', {}).get('utc'),
                    'source_api': 'OpenAQ',
                    'fetched_timestamp': datetime.now(timezone.utc).isoformat()
                }
                all_measurements.append(measurement_data)
            
            print(f"Processed location {i+1}/{len(locations)}: {location['name']} - {len(measurements)} measurements")
            time.sleep(1)  # Rate limiting
        
        # Save progress every 50 locations
        if (i + 1) % 50 == 0:
            temp_df = pd.DataFrame(all_measurements)
            temp_df.to_csv("data/openaq_measurements_temp.csv", index=False)
            print(f"Temporary save: {len(all_measurements)} measurements so far...")
    
    # Step 3: Save final measurements data
    if all_measurements:
        measurements_df = pd.DataFrame(all_measurements)
        measurements_df.to_csv("data/openaq_air_quality_data.csv", index=False)
        
        # Also save as JSON for additional metadata preservation
        with open("data/openaq_air_quality_data.json", 'w') as f:
            json.dump(all_measurements, f, indent=2)
        
        print(f"\nCompleted! Saved {len(measurements_df)} measurements to:")
        print("- data/openaq_air_quality_data.csv")
        print("- data/openaq_air_quality_data.json")
        
        # Show summary by pollutant
        pollutant_summary = measurements_df['parameter'].value_counts()
        print("\nPollutant measurement counts:")
        for pollutant, count in pollutant_summary.items():
            print(f"  {pollutant}: {count}")
    
    else:
        print("No measurements collected.")

if __name__ == "__main__":
    collect_air_quality_data()
