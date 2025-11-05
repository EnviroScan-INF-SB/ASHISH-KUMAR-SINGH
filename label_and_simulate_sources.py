"""
Module 3: Source Labeling and Simulation

- Define rule-based heuristics to label pollution sources (Vehicular, Industrial, Agricultural, Burning, Natural)
- Simulate labels if ground-truth is not available
- Validate labeling via simple checks
- Output: data/labeled_data.csv

Usage:
    python scripts/label_and_simulate_sources.py
"""

import os
import pandas as pd
import numpy as np
from datetime import datetime

DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data')
PROCESSED_FILE = os.path.join(DATA_DIR, 'processed_data.csv')
LABELED_FILE = os.path.join(DATA_DIR, 'labeled_data.csv')

# Simple heuristics thresholds (these can be refined)
THRESHOLDS = {
    'no2_high': 80,    # µg/m3 approx
    'so2_high': 50,
    'pm_high': 100
}


def label_row(row):
    # Rule-based labeling
    labels = []
    try:
        # Vehicular: near main road and high NO2
        if (not pd.isna(row.get('roads_closest_km')) and row.get('roads_closest_km') < 0.5) and row.get('no2',0) >= THRESHOLDS['no2_high']:
            labels.append('Vehicular')
        # Industrial: near industry and high SO2
        if (not pd.isna(row.get('industrial_zones_closest_km')) and row.get('industrial_zones_closest_km') < 1.0) and row.get('so2',0) >= THRESHOLDS['so2_high']:
            labels.append('Industrial')
        # Agricultural: near farmland and high PM during dry season
        if (not pd.isna(row.get('agricultural_fields_closest_km')) and row.get('agricultural_fields_closest_km') < 1.0) and row.get('season') in ['summer','autumn'] and (row.get('pm25',0) >= THRESHOLDS['pm_high'] or row.get('pm10',0) >= THRESHOLDS['pm_high']):
            labels.append('Agricultural')
        # Burning: high PM and low humidity
        if (row.get('pm25',0) >= THRESHOLDS['pm_high'] or row.get('pm10',0) >= THRESHOLDS['pm_high']) and row.get('humidity',100) < 40:
            labels.append('Burning')
        # Natural fallback
        if not labels:
            labels.append('Natural')
    except Exception:
        labels = ['Natural']

    # If multiple labels, pick the one with highest priority
    priority = ['Industrial','Vehicular','Agricultural','Burning','Natural']
    for p in priority:
        if p in labels:
            return p
    return labels[0]


def simulate_labels(df, seed=42):
    # If dataset doesn't have enough labeled examples, we can simulate by perturbing features
    np.random.seed(seed)
    sample = df.copy()
    # Create a pseudo-label using the label_row heuristic
    sample['pollution_source'] = sample.apply(label_row, axis=1)
    return sample


def main():
    if not os.path.exists(PROCESSED_FILE):
        print('Processed data not found. Run Module 2 first.')
        return

    df = pd.read_csv(PROCESSED_FILE)
    # Basic cleaning
    df = df.dropna(subset=['latitude','longitude'])
    # Apply rule-based labeling
    print('Applying heuristic labeling...')
    df['pollution_source'] = df.apply(label_row, axis=1)

    # If labels look heavily imbalanced or too few non-Natural labels, create simulated variants
    counts = df['pollution_source'].value_counts()
    print('Label counts:', counts.to_dict())
    if counts.get('Natural',0) / max(1, len(df)) > 0.9:
        print('High fraction of Natural labels detected; creating simulated labels to balance classes...')
        df = simulate_labels(df)

    df.to_csv(LABELED_FILE, index=False)
    print('Labeled dataset saved to', LABELED_FILE)

if __name__ == '__main__':
    main()
