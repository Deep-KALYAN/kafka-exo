import json
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

BASE_DIR = Path("hdfs-data")

records = []

# Parcours du HDFS-like
for file_path in BASE_DIR.rglob("alerts.json"):
    country = file_path.parents[1].name
    city = file_path.parents[0].name

    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            data = json.loads(line)
            data["country"] = country
            data["city"] = city
            records.append(data)

df = pd.DataFrame(records)

print("Dataset loaded:")
print(df.head())

# Conversion date
df["event_time"] = pd.to_datetime(df["event_time"])

# Alert levels → numérique
df["wind_alert_level_num"] = df["wind_alert_level"].str.replace("level_", "").astype(int)
df["heat_alert_level_num"] = df["heat_alert_level"].str.replace("level_", "").astype(int)

print("\nStats générales:")
print(df.describe())

print("\nMoyennes par ville:")
print(
    df.groupby(["country", "city"])[["temperature", "windspeed"]].mean()
)

print("\nNombre d’alertes vent par niveau:")
print(df["wind_alert_level"].value_counts())

print("\nNombre d’alertes chaleur par niveau:")
print(df["heat_alert_level"].value_counts())


plt.figure()
df["temperature"].plot(title="Température dans le temps")
plt.xlabel("Index")
plt.ylabel("°C")
plt.show()

plt.figure()
df["windspeed"].plot(title="Vitesse du vent dans le temps")
plt.xlabel("Index")
plt.ylabel("km/h")
plt.show()

plt.figure()
df["wind_alert_level_num"].value_counts().sort_index().plot(kind="bar", title="Alertes vent")
plt.xlabel("Niveau")
plt.ylabel("Nombre")
plt.show()

