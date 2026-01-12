import sys
import time
import json
import requests
from kafka import KafkaProducer
from datetime import datetime

if len(sys.argv) < 3:
    print("Usage: python current_weather.py <latitude> <longitude>")
    sys.exit(1)

latitude = sys.argv[1]
longitude = sys.argv[2]

KAFKA_TOPIC = "weather_stream"
KAFKA_BOOTSTRAP = "localhost:9092"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

API_URL = "https://api.open-meteo.com/v1/forecast"

print(f"Starting weather producer for lat={latitude}, lon={longitude}")

while True:
    try:
        response = requests.get(
            API_URL,
            params={
                "latitude": latitude,
                "longitude": longitude,
                "current_weather": "true"
            },
            timeout=10
        )

        data = response.json()
        weather = data.get("current_weather")

        if weather:
            message = {
                "event_time": datetime.utcnow().isoformat() + "Z",
                "latitude": float(latitude),
                "longitude": float(longitude),
                "temperature": weather["temperature"],
                "windspeed": weather["windspeed"],
                "weathercode": weather["weathercode"]
            }

            producer.send(KAFKA_TOPIC, message)
            producer.flush()

            print("Sent:", message)

        else:
            print("No weather data received")

    except Exception as e:
        print("Error:", e)

    time.sleep(10)  # toutes les 10 secondes
