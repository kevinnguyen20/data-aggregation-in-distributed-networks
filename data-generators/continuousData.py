import random
import json
import time
import sys
import os
from kafka3 import KafkaProducer

os.chdir(os.path.dirname(os.path.abspath(__file__)))

def extract_data(line_index, lines):
    line = lines[line_index].strip()
    parts = line.split(", ")
    return float(parts[2]), float(parts[3]), float(parts[4]), float(parts[5])

def generate_data(kafka_bootstrap_servers, producer_topic, line_index, lines, number_of_cluster):
    producer = KafkaProducer(
        bootstrap_servers=kafka_bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        batch_size=16384,
        linger_ms=5, # Wait for 5 ms before sending a batch
        compression_type='gzip' # Enable compression (optional)
    )

    product_names = ["Apple", "Banana", "Lemon", "Cherry", "Melon", "Peach", "Grapefruit"]
    window_size = 10000 # Number of records sent between each measurement of current throughput 

    # Define delay according to the server's location
    min_delay, avg_delay, max_delay, mdev_delay = extract_data(line_index, lines)
    delay = random.gauss(avg_delay, mdev_delay)
    delay = max(min_delay, min(delay, max_delay))
    time_for_sleep = delay / 2000
    print("Delay for cluster {0} - {1} seconds".format(number_of_cluster, time_for_sleep))

    start_time = time.time()
    i=1

    while True:
        record = {
            "id": i,
            "name": random.choice(product_names),
            "price": round(random.uniform(0.5, 1.8), 2)
        }

        producer.send(producer_topic, record)

        # Output regulary important information
        if i % window_size == 0:
            # elapsed_time = time.time() - start_time
            # current_throughput = i / elapsed_time
            # print(f"Elapsed time : {elapsed_time:.2f} seconds")
            # print(f"Current Throughput Cluster {number_of_cluster}: {current_throughput:.2f} records/second")
            time.sleep(time_for_sleep) # Simplified uni-directional delay (RTTs were saved in the file)

        i+=1

def validate_number_of_cluster(number_of_cluster):
    if number_of_cluster < 1 or number_of_cluster > 2:
        print("Invalid number of cluster. Please choose between 1 and 2")
        sys.exit(1)

def validate_line_index(line_index):
    with open("../delays.txt", "r") as file:
        lines = file.readlines()

    if line_index < 1 or line_index >= len(lines):
        print("Invalid line index. Please choose a number between 1 and {0}".format(len(lines) - 1))
        sys.exit(1)
    else:
        return lines

if __name__ == '__main__':
    if len(sys.argv)!=3:
        print("Usage: python3 script_name.py <number_of_cluster> <line_index>")
        sys.exit(1)

    try:
        number_of_cluster = int(sys.argv[1])
        validate_number_of_cluster(number_of_cluster)
        line_index = int(sys.argv[2])
        lines = validate_line_index(line_index)
    except ValueError:
        print("Invalid argument. Please provide an integer.")
        sys.exit(1)

    kafka_bootstrap_servers = 'localhost:9092'
    producer_topic = 'flink-kafka-topic' if number_of_cluster == 1 else 'flink-kafka-topic-2'

    generate_data(kafka_bootstrap_servers, producer_topic, line_index, lines, number_of_cluster)
