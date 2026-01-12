import sqlite3

conn = sqlite3.connect("weather_dw.db")
cur = conn.cursor()

cur.execute("""
CREATE TABLE IF NOT EXISTS dim_location (
    city_id INTEGER PRIMARY KEY AUTOINCREMENT,
    city TEXT,
    country TEXT,
    UNIQUE(city, country)
)
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS dim_time (
    time_id INTEGER PRIMARY KEY AUTOINCREMENT,
    date TEXT,
    hour INTEGER,
    minute INTEGER,
    UNIQUE(date, hour, minute)
)
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS fact_weather (
    event_time TEXT,
    city_id INTEGER,
    time_id INTEGER,
    temperature REAL,
    windspeed REAL,
    wind_alert_level INTEGER,
    heat_alert_level INTEGER,
    FOREIGN KEY(city_id) REFERENCES dim_location(city_id),
    FOREIGN KEY(time_id) REFERENCES dim_time(time_id)
)
""")

conn.commit()
conn.close()

print("Data Warehouse schema created")
