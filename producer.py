"""Продуцирует текущую погоду Open-Meteo в Kafka (топик weather)."""
import json
import time
from datetime import datetime, timezone

import requests
from kafka import KafkaProducer

CITIES = [
    ("Moscow", 55.7558, 37.6173),
    ("Saint Petersburg", 59.9343, 30.3351),
    ("Kazan", 55.7887, 49.1221),
    ("Yekaterinburg", 56.8389, 60.6057),
    ("Novosibirsk", 55.0084, 82.9357),
]

API = "https://api.open-meteo.com/v1/forecast"
BOOTSTRAP = "host.docker.internal:9092"
TOPIC = "weather"
INTERVAL = 10  # секунд между опросами

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

print(f"[producer] пишу в {TOPIC} каждые {INTERVAL} с (Kafka: {BOOTSTRAP})", flush=True)

while True:
    fetched_at = datetime.now(timezone.utc).isoformat()
    for city, lat, lon in CITIES:
        try:
            r = requests.get(API, params={
                "latitude": lat, "longitude": lon,
                "current": "temperature_2m,relative_humidity_2m,wind_speed_10m",
            }, timeout=10)
            r.raise_for_status()
            cur = r.json()["current"]
            msg = {
                "city": city,
                "latitude": lat,
                "longitude": lon,
                "temperature_2m": cur["temperature_2m"],
                "relative_humidity_2m": cur["relative_humidity_2m"],
                "wind_speed_10m": cur["wind_speed_10m"],
                "observed_at": cur["time"],
                "fetched_at": fetched_at,
            }
            producer.send(TOPIC, value=msg)
            print(f"[producer] {city}: {cur['temperature_2m']}°C, "
                  f"влажность {cur['relative_humidity_2m']}%", flush=True)
        except Exception as e:
            print(f"[producer] ошибка по {city}: {e}", flush=True)
    producer.flush()
    time.sleep(INTERVAL)
