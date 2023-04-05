import os, json
import time, datetime
from faker import Faker
from kafka import KafkaProducer

# WEATHER_API_URL = "https://api.open-meteo.com/v1/forecast?latitude=26.92&longitude=75.79&hourly=temperature_2m,relativehumidity_2m,rain,visibility"
# response = requests.get(url=WEATHER_API_URL)

# define const variables
KAFKA_SOURCE_TOPIC = "topic-a"
KAFKA_SINK_TOPIC = "topic-b"
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS")

# Producing data every 15 seconds
def producer():
    print(f"Producing data for Kafka topic {KAFKA_SOURCE_TOPIC} and server {KAFKA_BOOTSTRAP_SERVERS}")
    faker = Faker('en_IN')
    producer = KafkaProducer(bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS])
    while True:
        data = {
            "customer_name": faker.name(),
            "customer_city": faker.city(),
            "customer_address": faker.address(),
            "customer_contact": faker.phone_number(),
            "DOB": faker.date(),
            "timestamp": str(datetime.datetime.now())
        }
        print("\n", data, "\n")
        resp = producer.send(KAFKA_SOURCE_TOPIC, json.dumps(data).encode('utf-8'))
        print(resp.get().topic)
        time.sleep(15)