# Data Aggregation in Distributed Networks

In this project, we are using Apache Flink for executing queries in different networks, whose result we then transfer to a data aggregator. In the links between the networks and the aggregator, we emulate bad network conditions using Pumba in order to measure the impact of these on the whole execution time.

# Steps to run the application

## Prerequisites

### Software

- Java SE 11.0.19: https://www.oracle.com/java/technologies/javase/jdk11-archive-downloads.html
- Apache Kafka 2.12-3.4.1: https://kafka.apache.org/downloads
- Apache Flink 1.17.1: https://flink.apache.org/downloads/
- Apache Maven: https://maven.apache.org/download.cgi
- Docker: https://docs.docker.com/get-docker/

Download Kafka binary `kafka_2.13-3.4.0` and Apache Flink binary `flink-1.17.1`, unzip them, and don't change anything in the Flink config directory.

### Libraries

Download the following libraries required by the C Kafka client and JSON-formatted string processing:

```
sudo apt update
sudo apt install librdkafka-dev
sudo apt install libcjson-dev
```

### Python (Optional)

- Python 3.10.12: https://www.python.org/downloads/
- pip: https://pip.pypa.io/en/stable/installation/

```
pip install kafka-python3
```

## Run the application

1. You have to create a `config.sh` file from the `config.sh.template` and put the paths for your pc and place it in the root directory.
2. Use the command `./standalone.sh start` to start everything. (the first time you execute it another flink directory for the second cluster will be created locally)
3. At the end don't forget to execute the command `./standalone.sh stop` to stop everything.
