"""
Module 5: Geospatial Mapping and Heatmap Visualization

- Load predictions and location data and create interactive Folium maps and heatmaps
- Overlay source-specific markers and export map HTML

Usage:
    python scripts/generate_maps_and_heatmaps.py
"""

import os
import pandas as pd
import folium
from folium.plugins import HeatMap

DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data')
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), '..', 'maps')
LABELED_FILE = os.path.join(DATA_DIR, 'labeled_data.csv')
MAP_FILE = os.path.join(OUTPUT_DIR, 'pollution_heatmap.html')

os.makedirs(OUTPUT_DIR, exist_ok=True)


def main():
    if not os.path.exists(LABELED_FILE):
        print('Labeled data not found. Run Module 3 first.')
        return

    df = pd.read_csv(LABELED_FILE)
    df = df.dropna(subset=['latitude','longitude'])

    # Create base map centered at data mean
    center = [df['latitude'].mean(), df['longitude'].mean()]
    m = folium.Map(location=center, zoom_start=10)

    # Heatmap layer (use normalized PM if available else pm25)
    if 'pm25_norm' in df.columns:
        heat_data = [[row['latitude'], row['longitude'], row['pm25_norm']] for idx,row in df.iterrows()]
    else:
        heat_data = [[row['latitude'], row['longitude'], row.get('pm25',0)] for idx,row in df.iterrows()]

    HeatMap(heat_data, radius=15, blur=10, max_zoom=13).add_to(m)

    # Add markers colored by source type
    color_map = {
        'Industrial':'red',
        'Vehicular':'orange',
        'Agricultural':'green',
        'Burning':'purple',
        'Natural':'blue'
    }
    for idx,row in df.iterrows():
        src = row.get('pollution_source','Natural')
        folium.CircleMarker(location=[row['latitude'], row['longitude']], radius=4,
                            color=color_map.get(src,'gray'), fill=True, fill_opacity=0.7,
                            popup=f"{src} | PM2.5: {row.get('pm25','-')}").add_to(m)

    m.save(MAP_FILE)
    print('Map saved to', MAP_FILE)

if __name__ == '__main__':
    main()
