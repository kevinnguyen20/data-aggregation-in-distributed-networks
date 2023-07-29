import json
import random
import time
from kafka import KafkaProducer

def generate_data(kafka_bootstrap_servers, producer_topic):
    producer = KafkaProducer(
        bootstrap_servers=kafka_bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    product_names = ["Apple", "Banana", "Lemon", "Cherry", "Melon", "Peach", "Grapefruit"]
    i=0
    while True:
        record = {
            "id": i,
            "name": random.choice(product_names),
            "price": round(random.uniform(0.5, 1.8), 2)
        }

        producer.send(producer_topic, record)
        i+=1

if __name__ == '__main__':
    kafka_bootstrap_servers = 'localhost:9092'
    producer_topic = 'flink-kafka-topic-2'

    generate_data(kafka_bootstrap_servers, producer_topic)
