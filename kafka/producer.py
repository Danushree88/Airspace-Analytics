import requests
import json
import time
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

URL = "https://opensky-network.org/api/states/all"
TOPIC = "flight_stream"

def fetch_data():
    try:
        response = requests.get(URL, timeout=10)
        return response.json().get("states", [])
    except Exception as e:
        print("Error:", e)
        return []

def process(state):
    try:
        return {
            "icao24": state[0],
            "callsign": state[1].strip() if state[1] else None,
            "country": state[2],
            "timestamp": state[3],
            "longitude": state[5],
            "latitude": state[6],
            "altitude": state[7],
            "velocity": state[9],
            "vertical_rate": state[11]
        }
    except:
        return None

print("Producer started...")

while True:
    states = fetch_data()
    count = 0

    for s in states:
        record = process(s)
        if record and record["latitude"] and record["longitude"]:
            producer.send(TOPIC, value=record)
            count += 1

    producer.flush()
    print(f"Sent {count} records")
    time.sleep(10)