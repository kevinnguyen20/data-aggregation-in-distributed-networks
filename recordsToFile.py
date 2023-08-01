import sys
import random
import json

def generate_data(record_count, output_file):
    product_names = ["Apple", "Banana", "Lemon", "Cherry", "Melon", "Peach", "Grapefruit"]
    i = 0
    with open(output_file, 'w') as file:
        while i < record_count:
            record = {
                "id": i,
                "name": random.choice(product_names),
                "price": round(random.uniform(0.5, 1.8), 2)
            }
            file.write(json.dumps(record) + '\n')
            file.flush()

            i += 1

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: python3 recordsToFile.py <output_file> <record_count>")
        sys.exit(1)

    output_file = sys.argv[1]

    try:
        record_count = int(sys.argv[2])
    except ValueError:
        print("Invalid record count. Please provide an integer.")
        sys.exit(1)

    generate_data(record_count, output_file)
