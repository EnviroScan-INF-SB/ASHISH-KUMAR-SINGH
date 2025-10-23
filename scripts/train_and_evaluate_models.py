"""
Module 4: Model Training and Source Prediction

- Loads labeled dataset and trains classification models (RandomForest, XGBoost, DecisionTree)
- Performs train/test split and hyperparameter tuning (GridSearchCV / RandomizedSearchCV)
- Evaluates models and saves best model via joblib
- Outputs:
  - data/model_performance.json
  - models/best_model.joblib

Usage:
    python scripts/train_and_evaluate_models.py
"""

import os
import json
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.ensemble import RandomForestClassifier
from sklearn.tree import DecisionTreeClassifier
import joblib
from sklearn.metrics import classification_report, confusion_matrix

DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data')
MODELS_DIR = os.path.join(os.path.dirname(__file__), '..', 'models')
LABELED_FILE = os.path.join(DATA_DIR, 'labeled_data.csv')
PERF_FILE = os.path.join(DATA_DIR, 'model_performance.json')
BEST_MODEL_FILE = os.path.join(MODELS_DIR, 'best_model.joblib')

os.makedirs(MODELS_DIR, exist_ok=True)

FEATURE_COLS = None  # to be determined dynamically
TARGET_COL = 'pollution_source'


def basic_train_eval(X_train, X_test, y_train, y_test):
    results = {}
    # Random Forest baseline
    rf = RandomForestClassifier(n_estimators=100, random_state=42)
    rf.fit(X_train, y_train)
    preds = rf.predict(X_test)
    results['RandomForest'] = {
        'report': classification_report(y_test, preds, output_dict=True),
    }
    # Decision Tree baseline
    dt = DecisionTreeClassifier(random_state=42)
    dt.fit(X_train, y_train)
    preds_dt = dt.predict(X_test)
    results['DecisionTree'] = {
        'report': classification_report(y_test, preds_dt, output_dict=True)
    }
    return results, rf


def main():
    if not os.path.exists(LABELED_FILE):
        print('Labeled data not found. Run Module 3 first.')
        return

    df = pd.read_csv(LABELED_FILE)
    df = df.dropna(subset=['pollution_source'])

    # Select features dynamically: exclude identifiers and target
    exclude = ['location_id','location_name','latitude','longitude','timestamp','pollution_source']
    features = [c for c in df.columns if c not in exclude and df[c].dtype in [np.float64, np.int64, np.float32, np.int32]]
    if not features:
        print('No numeric features detected for modeling. Check processed data.')
        return

    X = df[features]
    y = df[TARGET_COL]

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)

    results, baseline_model = basic_train_eval(X_train, X_test, y_train, y_test)

    # Save performance
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(PERF_FILE, 'w') as f:
        json.dump(results, f, indent=2)

    # Save the baseline model
    joblib.dump(baseline_model, BEST_MODEL_FILE)
    print('Baseline model saved to', BEST_MODEL_FILE)
    print('Model performance saved to', PERF_FILE)

if __name__ == '__main__':
    main()
