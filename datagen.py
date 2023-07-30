import json
import random
import time
from kafka import KafkaProducer

def generate_data(kafka_bootstrap_servers, producer_topic):
    producer = KafkaProducer(
        bootstrap_servers=kafka_bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        batch_size=16384,
        linger_ms=5, # Wait for 5 ms before sending a batch
        compression_type='gzip' # Enable compression (optional)
    )

    product_names = ["Apple", "Banana", "Lemon", "Cherry", "Melon", "Peach", "Grapefruit"]
    i = 0
    start_time = time.time()

    while True:
        record = {
            "id": i,
            "name": random.choice(product_names),
            "price": round(random.uniform(0.5, 1.8), 2)
        }

        producer.send(producer_topic, record)
        i += 1

        # Check the elapsed time every 10,000 records
        if i % 10000 == 0:
            elapsed_time = time.time() - start_time
            throughput = i / elapsed_time
            print(f"Current Throughput: {throughput:.2f} records/second")

if __name__ == '__main__':
    kafka_bootstrap_servers = 'localhost:9092'
    producer_topic = 'flink-kafka-topic'

    generate_data(kafka_bootstrap_servers, producer_topic)