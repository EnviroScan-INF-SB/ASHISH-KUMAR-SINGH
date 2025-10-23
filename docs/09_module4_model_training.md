# Module 4: Model Training and Source Prediction

## Purpose
Train classification models (Random Forest, XGBoost, Decision Tree) to predict `pollution_source` using engineered features.

## Script
- `scripts/train_and_evaluate_models.py`

## What it does
- Loads `data/labeled_data.csv` and automatically selects numeric features for training.
- Trains RandomForest and DecisionTree baselines.
- Saves baseline model to `models/best_model.joblib` and performance JSON to `data/model_performance.json`.

## How to run
```powershell
python scripts/train_and_evaluate_models.py
```

## Notes
- This script is a baseline: you can extend it to do GridSearchCV, XGBoost training and model selection.
- Ensure `data/labeled_data.csv` exists and has a `pollution_source` column.

## Verification
- Inspect `data/model_performance.json` and `models/best_model.joblib`.