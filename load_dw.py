import json
import sqlite3
import pandas as pd
from pathlib import Path

DB = "weather_dw.db"
BASE_DIR = Path("hdfs-data")

conn = sqlite3.connect(DB)
cur = conn.cursor()

def get_or_create_location(city, country):
    cur.execute(
        "INSERT OR IGNORE INTO dim_location (city, country) VALUES (?, ?)",
        (city, country)
    )
    conn.commit()
    cur.execute(
        "SELECT city_id FROM dim_location WHERE city=? AND country=?",
        (city, country)
    )
    return cur.fetchone()[0]

def get_or_create_time(dt):
    date = dt.date().isoformat()
    hour = dt.hour
    minute = dt.minute

    cur.execute(
        "INSERT OR IGNORE INTO dim_time (date, hour, minute) VALUES (?, ?, ?)",
        (date, hour, minute)
    )
    conn.commit()
    cur.execute(
        "SELECT time_id FROM dim_time WHERE date=? AND hour=? AND minute=?",
        (date, hour, minute)
    )
    return cur.fetchone()[0]

for file_path in BASE_DIR.rglob("alerts.json"):
    country = file_path.parents[1].name
    city = file_path.parents[0].name

    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            data = json.loads(line)
            dt = pd.to_datetime(data["event_time"])

            city_id = get_or_create_location(city, country)
            time_id = get_or_create_time(dt)

            cur.execute("""
                INSERT INTO fact_weather
                (event_time, city_id, time_id, temperature, windspeed, wind_alert_level, heat_alert_level)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                data["event_time"],
                city_id,
                time_id,
                data["temperature"],
                data["windspeed"],
                int(data["wind_alert_level"].replace("level_", "")),
                int(data["heat_alert_level"].replace("level_", ""))
            ))

conn.commit()
conn.close()

print("Data loaded into Data Warehouse")
