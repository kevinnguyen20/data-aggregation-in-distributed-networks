import random
import time
import json
from kafka import KafkaProducer

def generate_data(kafka_bootstrap_servers, producer_topic, total_records):
    producer = KafkaProducer(
        bootstrap_servers=kafka_bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        batch_size=16384,
        linger_ms=5, # Wait for 5 ms before sending a batch
        compression_type='gzip' # Enable compression (optional)
    )

    product_names = ["Apple", "Banana", "Lemon", "Cherry", "Melon", "Peach", "Grapefruit"]
    start_time = time.time()

    for i in range(total_records):
        record = {
            "id": i,
            "name": random.choice(product_names),
            "price": round(random.uniform(0.5, 1.8), 2)
        }

        producer.send(producer_topic, record)

    elapsed_time = time.time() - start_time
    final_throughput = total_records / elapsed_time
    print(f"Final Throughput: {final_throughput:.2f} records/second")

if __name__ == '__main__':
    kafka_bootstrap_servers = 'localhost:9092'
    producer_topic = 'flink-kafka-topic-2'
    total_records = 10000000

    generate_data(kafka_bootstrap_servers, producer_topic, total_records)
