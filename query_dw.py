import sqlite3
import pandas as pd

conn = sqlite3.connect("weather_dw.db")

query = """
SELECT l.country, l.city,
       AVG(f.temperature) AS avg_temp,
       AVG(f.windspeed) AS avg_wind
FROM fact_weather f
JOIN dim_location l ON f.city_id = l.city_id
GROUP BY l.country, l.city
"""

df = pd.read_sql(query, conn)
print(df)

conn.close()
