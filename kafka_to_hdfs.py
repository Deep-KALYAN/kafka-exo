import json
import os
from kafka import KafkaConsumer

KAFKA_TOPIC = "weather_transformed"
KAFKA_BOOTSTRAP = "localhost:9092"
BASE_DIR = "hdfs-data"

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP,
    auto_offset_reset="latest",
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

print("Kafka → HDFS consumer started")

def resolve_location(lat, lon):
    if lat is None or lon is None:
        return "unknown_country", "unknown_city"

    if abs(lat - 48.8566) < 0.01 and abs(lon - 2.3522) < 0.01:
        return "France", "Paris"

    return "unknown_country", "unknown_city"

for message in consumer:
    data = message.value

    country, city = resolve_location(
        data.get("latitude"),
        data.get("longitude")
    )
    # country = data.get("country", "unknown_country")
    # city = data.get("city", "unknown_city")

    # Création du chemin HDFS-like
    dir_path = os.path.join(BASE_DIR, country, city)
    os.makedirs(dir_path, exist_ok=True)

    file_path = os.path.join(dir_path, "alerts.json")

    # Append JSON line
    with open(file_path, "a", encoding="utf-8") as f:
        f.write(json.dumps(data) + "\n")

    print(f"Saved alert → {file_path}")
