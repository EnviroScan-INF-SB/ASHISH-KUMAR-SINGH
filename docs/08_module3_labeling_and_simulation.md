# Module 3: Source Labeling and Simulation

## Purpose
Label pollution sources (Vehicular, Industrial, Agricultural, Burning, Natural) using rule-based heuristics and simulate labeled data when ground-truth is limited.

## Script
- `scripts/label_and_simulate_sources.py`

## What it does
- Applies rule-based heuristics using proximity and pollutant thresholds.
- If labels are overwhelmingly `Natural`, creates simulated labels to balance classes.
- Saves: `data/labeled_data.csv`

## How to run
```powershell
python scripts/label_and_simulate_sources.py
```

## Notes
- Heuristic thresholds are adjustable in the script (`THRESHOLDS`).
- This is a deterministic rule-based approach; later you can add ML-based or expert-validated labeling.

## Verification
- Inspect `data/labeled_data.csv` and distribution of `pollution_source`.