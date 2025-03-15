import streamlit as st
import pandas as pd
import numpy as np
import requests
import plotly.express as px
import plotly.graph_objects as go
import asyncio
import aiohttp
from concurrent.futures import ThreadPoolExecutor

# Function to load data
@st.cache_data
def load_data(file):
    try:
        df = pd.read_csv(file, parse_dates=['timestamp'])
        required_columns = {'city', 'timestamp', 'temperature', 'season'}
        if not required_columns.issubset(df.columns):
            st.error(f"Error: The file must contain columns {required_columns}")
            return None
        return df
    except Exception as e:
        st.error(f"Data loading error: {e}")
        return None

# Function to calculate moving average
@st.cache_data
def moving_average(df, window=30):
    df['rolling_mean'] = df['temperature'].rolling(window=window).mean()
    df['rolling_std'] = df['temperature'].rolling(window=window).std()
    return df

# Function to detect anomalies
@st.cache_data
def detect_anomalies(df):
    df['upper_bound'] = df['rolling_mean'] + 2 * df['rolling_std']
    df['lower_bound'] = df['rolling_mean'] - 2 * df['rolling_std']
    df['anomaly'] = (df['temperature'] > df['upper_bound']) | (df['temperature'] < df['lower_bound'])
    return df

# Function to fetch weather data (synchronous request)
def fetch_weather_sync(city, api_key):
    url = f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units=metric"
    try:
        response = requests.get(url, timeout=5)
        data = response.json()
        if response.status_code != 200:
            st.error(f"API Error: {data.get('message', 'Unknown error')}")
            return None
        return data
    except requests.exceptions.RequestException as e:
        st.error(f"API request error: {e}")
        return None

# Function to fetch weather data (asynchronous request)
async def fetch_weather_async(city, api_key):
    url = f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units=metric"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=5) as response:
                data = await response.json()
                if response.status != 200:
                    st.error(f"API Error: {data.get('message', 'Unknown error')}")
                    return None
                return data
    except asyncio.TimeoutError:
        st.error("Error: API request timeout")
        return None
    except aiohttp.ClientError as e:
        st.error(f"Connection error with API: {e}")
        return None

# Streamlit interface
st.title("Temperature Analysis and Weather Monitoring")

uploaded_file = st.file_uploader("Upload CSV file with historical data", type=["csv"])

if uploaded_file:
    df = load_data(uploaded_file)
    if df is not None:
        cities = df['city'].unique()
        city = st.selectbox("Select city", cities)
        df_city = df[df['city'] == city].copy()
        
        df_city = moving_average(df_city)
        df_city = detect_anomalies(df_city)
        
        if len(df_city) > 10:
            fig = px.line(df_city, x='timestamp', y='temperature', title=f'Temperature in {city}')
            fig.add_trace(go.Scatter(x=df_city['timestamp'], y=df_city['upper_bound'], mode='lines', name='Upper Bound'))
            fig.add_trace(go.Scatter(x=df_city['timestamp'], y=df_city['lower_bound'], mode='lines', name='Lower Bound'))
            fig.add_trace(go.Scatter(x=df_city['timestamp'][df_city['anomaly']],
                                     y=df_city['temperature'][df_city['anomaly']],
                                     mode='markers', name='Anomalies', marker=dict(color='red')))
            st.plotly_chart(fig)
        else:
            st.warning("Not enough data to plot a graph")
        
        # API key input
        api_key = "7c69345ec0c70bca1ae9847979e7cac1"
        if api_key:
            method = st.radio("Select request method", ["Synchronous", "Asynchronous"])
            if st.button("Get current temperature"):
                if method == "Synchronous":
                    weather_data = fetch_weather_sync(city, api_key)
                else:
                    weather_data = asyncio.run(fetch_weather_async(city, api_key))
                
                if weather_data and "main" in weather_data:
                    current_temp = weather_data["main"]["temp"]
                    st.write(f"Current temperature in {city}: {current_temp}°C")
                    most_common_season = df_city['season'].mode()[0] if not df_city['season'].mode().empty else None
                    if most_common_season:
                        season_mean = df_city[df_city['season'] == most_common_season]['temperature'].mean()
                        season_std = df_city[df_city['season'] == most_common_season]['temperature'].std()
                        lower_bound = season_mean - 2 * season_std
                        upper_bound = season_mean + 2 * season_std
                        if lower_bound <= current_temp <= upper_bound:
                            st.success("Temperature is within normal range.")
                        else:
                            st.error("Anomalous temperature!")
                else:
                    st.error("Error retrieving data. Check API key and city name.")
