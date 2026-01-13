## 🌤️ Kafka Weather Streaming Project

### 📋 Overview

A complete real-time weather streaming system using Kafka, Spark, and Python to collect, transform, analyze, and store meteorological data.

### 🏗️ Architecture
Plaintext
```
Open-Meteo API
      ↓
Python Producer (current_weather.py)
      ↓
Kafka Topic: weather_stream
      ↓
Spark Streaming (Transformation + Alertes)
      ↓
Kafka Topic: weather_transformed
      ↓
├── Spark Aggregates (Exercice 5)
├── HDFS Storage (Exercice 7)
└── Real-time Consumers

📁 Project Structure
kafka-weather/
├── docker-compose.yml          # Kafka + Zookeeper infrastructure
├── current_weather.py          # Weather Producer (Ex. 3)
├── current_weather_city.py     # City-based Producer (Ex. 6)
├── consumer.py                 # Kafka Consumer (Ex. 2)
├── spark_weather_alerts.py     # Spark Transformation (Ex. 4)
├── spark_weather_aggregates.py # Spark Aggregates (Ex. 5)
├── kafka_to_hdfs.py           # HDFS Storage (Ex. 7)
│
├── hdfs-data/                 # Simulation HDFS (Data Lake)
│   └── [country]/[city]/      # Data partitioned by location
│
├── analyze_weather.py          # Data Analysis & Matplotlib (Ex. 8)
├── create_dw.py                # DW Schema Creation (Ex. 9)
├── load_dw.py                  # ETL: Data Lake to Warehouse (Ex. 9)
├── query_dw.py                 # SQL Analytics Queries (Ex. 9)
├── weather_dw.db               # SQLite Database (Data Warehouse)
├── dashboard.py                # Streamlit BI Dashboard (Ex. 10)
│
├── .gitignore                  # Git exclusion rules
├── requirements.txt            # Python dependencies
└── README.md                   # Full Project Documentation (Ex. 11-13)

```

## 🚀 Installation & Configuration

Prerequisites
- Docker & Docker Compose
- Python 3.9+
- Java 8 or 11 (Required for Spark)
- PySpark

### Quick Start
Bash
#### 1. Clone/Initialize the project
mkdir kafka-weather
cd kafka-weather

#### 2. Install Python dependencies
pip install kafka-python requests pyspark

#### 3. Start Kafka with Docker
docker compose up -d

#### 4. Verify installation
docker ps | findstr kafka

## 📝 Completed Exercises

🔹 Exercice 1 & 2: Kafka Basics

✅ Created Kafka topics.

✅ Built simple producer/consumer scripts.

✅ Established Python ↔ Kafka communication.

🔹 Exercice 3: Real-time Weather Producer

✅ Integrated Open-Meteo API.

✅ Automated continuous data transmission.

✅ Implemented standardized JSON schema.

🔹 Exercice 4: Spark Transformation

✅ Real-time stream reading from Kafka.

✅ Alert Calculation: Automated logic for wind and temperature.

✅ Implemented levels: level_0, level_1, level_2.

✅ Streamed results to a new output topic.

🔹 Exercice 5: Real-time Aggregates

✅ Implemented Sliding Windows (5min/1min).

✅ Stats: Avg/Min/Max temperature.

✅ Alert counting by type.

🔹 Exercice 6: Geocoding Integration

✅ Resolved Lat/Long automatically via City/Country names.

✅ Data enrichment with geographic metadata.

🔹 Exercice 7: Structured HDFS Storage

✅ Consumed data from Kafka for persistence.

✅ Organised folder structure by /country/city/.

✅ Saved in JSON-line format compatible with real HDFS clusters.

📈 Exercice 8 : Analyse & Visualisation

✅ Script analyze_weather.py utilisant Pandas et Matplotlib.

✅ Chargement des données depuis le Data Lake (hdfs-data).

✅ Visualisation des tendances de température et distribution des alertes.

🏛️ Exercice 9 : Data Warehouse (Modélisation en Étoile)

✅ Création d'une base analytique weather_dw.db (SQLite).

✅ Schéma en Étoile :

fact_weather (Table de faits)

dim_location & dim_time (Dimensions)

✅ Pipeline ETL pour transformer le JSON brut en données SQL structurées.

🖼️ Exercice 10 : Dashboard Interactif

✅ Interface développée avec Streamlit.

✅ KPIs en temps réel : Température moyenne, vent, compteur d'alertes.

✅ Sélecteurs par Pays/Ville avec graphiques dynamiques.

🛡️ Phase 4 : Industrialisation & Validation (Ex. 11 - 13)

✅ Exercice 11 : Supervision du Pipeline

Monitoring Kafka : Vérification des offsets et du LAG des consommateurs.

Qualité des Données : Validation du schéma JSON et intégrité du Data Lake.

Spark UI : Surveillance des micro-batches et des performances de traitement.

🚀 Exercice 12 : Bonnes Pratiques de Production

Scalabilité : Stratégie de partitionnement Kafka (3-6 partitions en production).

Fiabilité : Concept d'idempotence des producteurs et gestion des duplications.

Sécurité : Introduction théorique à SSL/TLS et SASL pour Kafka.

🏁 Exercice 13 : Synthèse & Soutenance

Vue d'ensemble : Maîtrise du pipeline de bout-en-bout (End-to-End).

Justification technique : Pourquoi avoir choisi Spark (traitement distribué) et Kafka (découplage).

Auto-critique : Identification des limites (simulated HDFS) et axes d'amélioration.

## 🎯 Alert Rules

### 🌡️ Temperature Alerts

Level        Threshold          Description

level_0      < 25°C             Normal

level_1      25-35°C            Moderate Heat

level_2      > 35°C             Heatwave

### 💨 Wind Alerts

Level        Threshold          Description

level_0      < 10 m/s           Low

level_1      10-20 m/s          Moderate

level_2      > 20 m/s           Strong/Gale


### 🛠️ Useful Commands

Kafka Management

Bash

- List all topics

docker exec -it kafka-weather-kafka-1 kafka-topics --list --bootstrap-server localhost:9092

- Consume a topic manually

docker exec -it kafka-weather-kafka-1 kafka-console-consumer --topic weather_stream --from-beginning --bootstrap-server localhost:9092

- Produce a message manually

docker exec -it kafka-weather-kafka-1 kafka-console-producer --topic weather_stream --bootstrap-server localhost:9092

Running Services

Bash

- Start Weather Producer (Example: Paris)

python current_weather.py 48.8566 2.3522

- Start City Producer

python current_weather_city.py Paris France

- Start Spark Transformation

python spark_weather_alerts.py

- Start Spark Aggregates

python spark_weather_aggregates.py

- Start HDFS Storage script

python kafka_to_hdfs.py

- Start simple consumer for verification

python consumer.py weather_transformed

- Lancer l'analyse statique (Exercice 8)

python analyze_weather.py

- Préparer le Data Warehouse (Exercice 9)

python create_dw.py

python load_dw.py

- Lancer le Dashboard final (Exercice 10)

streamlit run dashboard.py


### 🔧 Troubleshooting

Kafka doesn't start: 

Run 

docker compose down 

then 

docker compose up -d.

Python-Kafka Connection error: 

Ensure Kafka is listening on localhost:9092. 

Check logs: docker logs kafka-weather-kafka-1.

Spark Errors: 

Verify JAVA_HOME points to Java 8 or 11 and SPARK_HOME is correctly set in environment variables.

### 📊 Data Format
```
Raw Input (weather_stream)JSON{
  "event_time": "2026-01-12T10:00:00Z",
  "city": "Paris",
  "country": "France",
  "latitude": 48.8566,
  "longitude": 2.3522,
  "temperature": 17.3,
  "windspeed": 12.4,
  "weathercode": 3
}

Transformed Output (weather_transformed)JSON{
  "event_time": "2026-01-12T10:00:00",
  "city": "Paris",
  "country": "France",
  "latitude": 48.8566,
  "longitude": 2.3522,
  "temperature": 17.3,
  "windspeed": 12.4,
  "wind_alert_level": "level_1",
  "heat_alert_level": "level_0"
}
```

