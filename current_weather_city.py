import sys
import time
import json
import requests
from kafka import KafkaProducer
from datetime import datetime

if len(sys.argv) < 3:
    print("Usage: python current_weather_city.py <city> <country>")
    sys.exit(1)

city = sys.argv[1]
country = sys.argv[2]

KAFKA_TOPIC = "weather_stream"
KAFKA_BOOTSTRAP = "localhost:9092"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# 1️⃣ Geocoding
geo_url = "https://geocoding-api.open-meteo.com/v1/search"
geo_resp = requests.get(
    geo_url,
    params={
        "name": city,
        "count": 1,
        "language": "en",
        "format": "json"
    },
    timeout=10
).json()

if "results" not in geo_resp or len(geo_resp["results"]) == 0:
    print("City not found")
    sys.exit(1)

location = geo_resp["results"][0]
latitude = location["latitude"]
longitude = location["longitude"]
resolved_country = location["country"]

print(f"Resolved {city}, {resolved_country} → lat={latitude}, lon={longitude}")

# 2️⃣ Weather producer loop
weather_url = "https://api.open-meteo.com/v1/forecast"

while True:
    try:
        weather_resp = requests.get(
            weather_url,
            params={
                "latitude": latitude,
                "longitude": longitude,
                "current_weather": "true"
            },
            timeout=10
        ).json()

        weather = weather_resp.get("current_weather")

        if weather:
            message = {
                "event_time": datetime.utcnow().isoformat() + "Z",
                "city": city,
                "country": resolved_country,
                "latitude": latitude,
                "longitude": longitude,
                "temperature": weather["temperature"],
                "windspeed": weather["windspeed"],
                "weathercode": weather["weathercode"]
            }

            producer.send(KAFKA_TOPIC, message)
            producer.flush()

            print("Sent:", message)

    except Exception as e:
        print("Error:", e)

    time.sleep(10)
