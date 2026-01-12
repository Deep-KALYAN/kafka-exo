import streamlit as st
import sqlite3
import pandas as pd

DB = "weather_dw.db"

st.set_page_config(page_title="Weather Analytics Dashboard", layout="wide")
st.title("🌦️ Weather Analytics Dashboard")

@st.cache_data(ttl=10)
def load_data():
    conn = sqlite3.connect(DB)
    query = """
    SELECT f.event_time, l.country, l.city,
           f.temperature, f.windspeed,
           f.wind_alert_level, f.heat_alert_level
    FROM fact_weather f
    JOIN dim_location l ON f.city_id = l.city_id
    """
    df = pd.read_sql(query, conn)
    conn.close()
    df["event_time"] = pd.to_datetime(df["event_time"])
    return df

df = load_data()

# KPIs
col1, col2, col3 = st.columns(3)

col1.metric("🌡️ Avg Temperature (°C)", round(df["temperature"].mean(), 2))
col2.metric("💨 Avg Wind Speed (km/h)", round(df["windspeed"].mean(), 2))
col3.metric("⚠️ Total Alerts", int((df["wind_alert_level"] > 0).sum()))

st.divider()

# Sélecteurs
country = st.selectbox("Country", df["country"].unique())
city = st.selectbox(
    "City",
    df[df["country"] == country]["city"].unique()
)

filtered = df[(df["country"] == country) & (df["city"] == city)]

# Graphiques
st.subheader(f"📈 Temperature Over Time — {city}")
st.line_chart(filtered.set_index("event_time")["temperature"])

st.subheader(f"💨 Wind Speed Over Time — {city}")
st.line_chart(filtered.set_index("event_time")["windspeed"])

st.subheader("⚠️ Alert Levels Distribution")
st.bar_chart(
    filtered["wind_alert_level"].value_counts().sort_index()
)
