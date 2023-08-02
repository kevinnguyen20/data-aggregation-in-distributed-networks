import random
import json
import time
import sys
import os
from kafka import KafkaProducer

os.chdir(os.path.dirname(os.path.abspath(__file__)))

def extract_data(line_index):
    with open("../delays.txt", "r") as file:
        lines = file.readlines()

    if line_index>=0 and line_index<len(lines):
        line = lines[line_index].strip()
        parts = line.split(", ")
        return float(parts[2]), float(parts[3]), float(parts[4]), float(parts[5])
    else:
        print("Invalid line index.")
        return None

def generate_data(kafka_bootstrap_servers, producer_topic, line_index):
    producer = KafkaProducer(
        bootstrap_servers=kafka_bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        batch_size=16384,
        linger_ms=5, # Wait for 5 ms before sending a batch
        compression_type='gzip' # Enable compression (optional)
    )

    product_names = ["Apple", "Banana", "Lemon", "Cherry", "Melon", "Peach", "Grapefruit"]
    i=0

    while True:
        record = {
            "id": i,
            "name": random.choice(product_names),
            "price": round(random.uniform(0.5, 1.8), 2)
        }

        min_delay, avg_delay, max_delay, mdev_delay = extract_data(line_index)
        delay = random.gauss(avg_delay, mdev_delay)
        delay = max(min_delay, min(delay, max_delay))
        time.sleep(delay/2 /1000) # Simplified uni-directional delay (RTTs were saved in the file)

        producer.send(producer_topic, record)
        i+=1

if __name__ == '__main__':
    if len(sys.argv)!=2:
        print("Usage: python3 script_name.py <line_index>")
        sys.exit(1)

    try:
        line_index = int(sys.argv[1])
    except ValueError:
        print("Invalid line index. Please provide an integer.")
        sys.exit(1)

    kafka_bootstrap_servers = 'localhost:9092'
    producer_topic = 'flink-kafka-topic-2'

    generate_data(kafka_bootstrap_servers, producer_topic, line_index)
