# Milestone 2 (Week 3–4)

Modules 2 & 3: Data Cleaning, Feature Engineering, Source Labeling & Simulation

Implemented scripts and docs:
- `scripts/clean_and_feature_engineer.py` — Module 2
- `scripts/label_and_simulate_sources.py` — Module 3
- Docs:
  - `docs/07_module2_cleaning_feature_engineering.md`
  - `docs/08_module3_labeling_and_simulation.md`

Outputs:
- `data/processed_data.csv`
- `data/processed_data_normalized.csv`
- `data/feature_list.json`
- `data/labeled_data.csv`

How to run

1. Ensure Module 1 outputs are present in `data/`.
2. `python scripts/clean_and_feature_engineer.py`
3. `python scripts/label_and_simulate_sources.py`

Notes
- Labeling uses a heuristic rule set that can be tuned in `scripts/label_and_simulate_sources.py`.