import streamlit as st
import pandas as pd
import numpy as np
import requests
import plotly.express as px
import plotly.graph_objects as go
import asyncio
import aiohttp
from concurrent.futures import ThreadPoolExecutor


@st.cache_data
def load_data(file):
    df = pd.read_csv(file, parse_dates=['timestamp'])
    return df

@st.cache_data
def moving_average(df, window=30):
    df['rolling_mean'] = df['temperature'].rolling(window=window).mean()
    df['rolling_std'] = df['temperature'].rolling(window=window).std()
    return df

@st.cache_data
def detect_anomalies(df):
    df['upper_bound'] = df['rolling_mean'] + 2 * df['rolling_std']
    df['lower_bound'] = df['rolling_mean'] - 2 * df['rolling_std']
    df['anomaly'] = (df['temperature'] > df['upper_bound']) | (df['temperature'] < df['lower_bound'])
    return df

def fetch_weather_sync(city, api_key):
    url = f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units=metric"
    response = requests.get(url)
    return response.json()

async def fetch_weather_async(city, api_key):
    url = f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units=metric"
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            return await response.json()

st.title("Анализ температуры и мониторинг погоды")

uploaded_file = st.file_uploader("Загрузите CSV-файл с историческими данными", type=["csv"])

if uploaded_file:
    df = load_data(uploaded_file)
    cities = df['city'].unique()
    city = st.selectbox("Выберите город", cities)
    df_city = df[df['city'] == city].copy()
    
    df_city = moving_average(df_city)
    df_city = detect_anomalies(df_city)
    
    fig = px.line(df_city, x='timestamp', y='temperature', title=f'Температура в {city}')
    fig.add_trace(go.Scatter(x=df_city['timestamp'], y=df_city['upper_bound'], mode='lines', name='Upper Bound'))
    fig.add_trace(go.Scatter(x=df_city['timestamp'], y=df_city['lower_bound'], mode='lines', name='Lower Bound'))
    fig.add_trace(go.Scatter(x=df_city['timestamp'][df_city['anomaly']],
                             y=df_city['temperature'][df_city['anomaly']],
                             mode='markers', name='Anomalies', marker=dict(color='red')))
    st.plotly_chart(fig)
    
    api_key = st.text_input("Введите API-ключ OpenWeatherMap")
    if api_key:
        method = st.radio("Выберите метод запроса", ["Синхронный", "Асинхронный"])
        if st.button("Получить текущую температуру"):
            if method == "Синхронный":
                weather_data = fetch_weather_sync(city, api_key)
            else:
                weather_data = asyncio.run(fetch_weather_async(city, api_key))
            
            if "main" in weather_data:
                current_temp = weather_data["main"]["temp"]
                st.write(f"Текущая температура в {city}: {current_temp}°C")
                season_mean = df_city[df_city['season'] == df_city['season'].mode()[0]]['temperature'].mean()
                season_std = df_city[df_city['season'] == df_city['season'].mode()[0]]['temperature'].std()
                lower_bound = season_mean - 2 * season_std
                upper_bound = season_mean + 2 * season_std
                if lower_bound <= current_temp <= upper_bound:
                    st.success("Температура в пределах нормы.")
                else:
                    st.error("Аномальная температура!")
            
