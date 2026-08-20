import pandas as pd
from kafka import KafkaProducer

import json
import time

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

df = pd.read_parquet("yellow_tripdata_2025-01.parquet")

events = []

for i in range(5):
    trip = df.iloc[i]

    trip_started = {
        "event_type": "trip_started",
        "event_time": trip["tpep_pickup_datetime"].isoformat(),
        "pickup_location_id": int(trip["PULocationID"])
    }

    trip_duration_minutes = (
        trip["tpep_dropoff_datetime"] - trip["tpep_pickup_datetime"]
    ).total_seconds() / 60

    trip_completed = {
        "event_type": "trip_completed",
        "event_time": trip["tpep_dropoff_datetime"].isoformat(),
        "dropoff_location_id": int(trip["DOLocationID"]),
        "trip_distance": float(trip["trip_distance"]),
        "trip_duration_minutes": trip_duration_minutes,
        "fare_amount": float(trip["fare_amount"]),
        "tolls_amount": float(trip["tolls_amount"]),
        "payment_type": int(trip["payment_type"]),
        "total_amount": float(trip["total_amount"])
    }

    events.append(trip_started)
    events.append(trip_completed)

print("\n--- 시간순 정렬 ---")

events.sort(key=lambda event: event["event_time"])

for event in events:
    print(event)

print("\n--- 실시간 재생 테스트 ---")

speed = 100

for i in range(len(events) - 1):

    current_event = events[i]
    next_event = events[i + 1]

    current_time = pd.to_datetime(current_event["event_time"])
    next_time = pd.to_datetime(next_event["event_time"])

    time_diff = (next_time - current_time).total_seconds()

    wait_time = time_diff / speed

    print(current_event)
    producer.send("taxi-trip-events", value=current_event)
    print("다음 이벤트까지 대기:", wait_time, "초")

    time.sleep(wait_time)

last_event = events[-1]

print(last_event)
producer.send("taxi-trip-events", value=last_event)

producer.flush()



