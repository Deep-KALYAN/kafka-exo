# consumer.py corrigé
import sys
from kafka import KafkaConsumer
import json

if len(sys.argv) < 2:
    print("Usage: python consumer.py <topic>")
    sys.exit(1)

topic = sys.argv[1]

consumer = KafkaConsumer(
    topic,
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")) if v else None
)

print(f"Listening to topic: {topic}")

for message in consumer:
    if message.value is not None:
        print(message.value)
    else:
        print(f"Empty or invalid message at offset {message.offset}")


# import sys
# from kafka import KafkaConsumer
# import json

# if len(sys.argv) < 2:
#     print("Usage: python consumer.py <topic>")
#     sys.exit(1)

# topic = sys.argv[1]

# consumer = KafkaConsumer(
#     topic,
#     bootstrap_servers="localhost:9092",
#     auto_offset_reset="earliest",
#     value_deserializer=lambda v: json.loads(v.decode("utf-8"))
# )

# print(f"Listening to topic: {topic}")

# for message in consumer:
#     print(message.value)
