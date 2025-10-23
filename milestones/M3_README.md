# Milestone 3 (Week 5–6)

Module 4: Model Training and Source Prediction

Implemented scripts and docs:
- `scripts/train_and_evaluate_models.py` — Baseline training and evaluation
- `docs/09_module4_model_training.md`

Outputs:
- `data/model_performance.json`
- `models/best_model.joblib`

How to run

1. Ensure `data/labeled_data.csv` exists (Module 3 output).
2. `python scripts/train_and_evaluate_models.py`

Notes
- This script provides baseline RF and DecisionTree training. Add hyperparameter tuning (GridSearchCV/RandomizedSearchCV) and XGBoost for better performance.