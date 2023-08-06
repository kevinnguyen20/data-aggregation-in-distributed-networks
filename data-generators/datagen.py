import random
import time
import json
import sys
#from kafka import KafkaProducer #uncomment whatever works for you
from kafka3 import KafkaProducer

def generate_data(kafka_bootstrap_servers, producer_topic, total_records, number_of_cluster):
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
    print(f"Elapsed time : {elapsed_time:.2f} seconds")
    print(f"Final Throughput Cluster {number_of_cluster}: {final_throughput:.2f} records/second")

def validate_number_of_cluster(number_of_cluster):
    if number_of_cluster < 1 or number_of_cluster > 2:
        print("Invalid number of cluster. Please choose between 1 and 2")
        sys.exit(1)

if __name__ == '__main__':
    try:
        number_of_cluster = int(sys.argv[1])
        validate_number_of_cluster(number_of_cluster)
    except ValueError:
        print("Invalid argument. Please provide an integer.")
        sys.exit(1)

    kafka_bootstrap_servers = 'localhost:9092'
    producer_topic = 'flink-kafka-topic' if number_of_cluster == 1 else 'flink-kafka-topic-2'
    total_records = 10000000

    generate_data(kafka_bootstrap_servers, producer_topic, total_records, number_of_cluster)
