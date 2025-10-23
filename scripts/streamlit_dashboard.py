"""
Module 6: Real-Time Dashboard and Alerts (Streamlit)

This script is a Streamlit app skeleton to show predictions, maps and alerts.
Run: streamlit run scripts/streamlit_dashboard.py
"""

import streamlit as st
import pandas as pd
import joblib
import os
from streamlit_folium import st_folium
import folium

DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data')
MODELS_DIR = os.path.join(os.path.dirname(__file__), '..', 'models')
LABELED_FILE = os.path.join(DATA_DIR, 'labeled_data.csv')
MODEL_FILE = os.path.join(MODELS_DIR, 'best_model.joblib')

st.set_page_config(layout='wide', page_title='EnviroScan Dashboard')

st.title('EnviroScan - Pollution Source Prediction')

if os.path.exists(LABELED_FILE):
    df = pd.read_csv(LABELED_FILE)
    st.sidebar.write('Data loaded')
else:
    st.sidebar.write('Labeled data not found. Run Module 3')
    df = pd.DataFrame()

if os.path.exists(MODEL_FILE):
    model = joblib.load(MODEL_FILE)
else:
    model = None

col1, col2 = st.columns([1,2])
with col1:
    st.header('Controls')
    city = st.text_input('City / Location filter')
    min_pm25 = st.slider('Min PM2.5', 0, 500, 0)
    if st.button('Filter'):
        st.write('Filter applied (client-side)')

with col2:
    st.header('Map')
    if not df.empty:
        center = [df['latitude'].mean(), df['longitude'].mean()]
        m = folium.Map(location=center, zoom_start=10)
        # add markers for top 200 points
        sample = df.head(200)
        for idx,row in sample.iterrows():
            folium.CircleMarker(location=[row['latitude'], row['longitude']], radius=3,
                                color='blue', fill=True).add_to(m)
        st_folium(m, width=700, height=500)
    else:
        st.info('No data to display')

st.header('Model Predictions & Alerts')
if model is not None and not df.empty:
    st.write('Model is loaded. You can display prediction summaries here.')
else:
    st.write('No model loaded.')

st.header('Data Preview')
if not df.empty:
    st.dataframe(df.head(50))
else:
    st.write('No labeled data available')
