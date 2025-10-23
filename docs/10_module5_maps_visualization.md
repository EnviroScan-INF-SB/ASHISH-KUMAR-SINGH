# Module 5: Geospatial Mapping and Heatmap Visualization

## Purpose
Create interactive maps and heatmaps showing pollution intensity and predicted sources.

## Script
- `scripts/generate_maps_and_heatmaps.py`

## What it does
- Loads labeled data and plots a heatmap and source-specific markers using Folium.
- Saves map to `maps/pollution_heatmap.html`.

## How to run
```powershell
python scripts/generate_maps_and_heatmaps.py
```

## Notes
- Requires `data/labeled_data.csv` with `latitude`, `longitude`, `pm25_norm` and `pollution_source`.

## Verification
- Open `maps/pollution_heatmap.html` in a browser.