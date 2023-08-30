# Data Aggregation in Distributed Networks

In this project, we are using Apache Flink as a stream-processing framework to execute queries in a multi-cluster network, the results of which we then join and aggregate. We aim to emulate various network conditions by manipulating different network parameters such as delays, window sizes, and buffer sizes, which allows us to measure the impact of these on throughput and latency.

For further information, check out the [Wiki](https://github.com/kevinnguyen20/data-aggregation-in-distributed-networks/wiki) pages.

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

1. Clone the repository. If you need help, follow these [instructions](https://docs.github.com/en/repositories/creating-and-managing-repositories/cloning-a-repository).
2. You have to create a `config.sh` file from the `config.sh.template` and put the paths for your pc and place it in the `shell-scripts/` directory.
3. In the ```docker-app``` folder you need a folder `jobs` where the jars for both clusters will be stored when using Docker.

**Note**: Before starting the app, make sure that you have set the correct value (`false` or `true`) for the variable ```DOCKER_DEPLOYMENT``` in the ```config.sh``` file depending on what environment you want to work with.

From now on, it is only necessary to use two commands which are shown below:

```
./start.sh
```
calls a script that starts the whole system. The first time you execute it another flink directory for the second cluster will be created locally.

```
./stop.sh
```
stops everything (Flink, Kafka, Zookeeper, ...).

# Team

Developers:

* [Georgi Kotsev](https://github.com/gogokotsev00)
* [Kevin Nguyen](https://github.com/kevinnguyen20)
* [Krutarth Parwal](https://github.com/krutarth4)
* [Momchil Petrov](https://github.com/Smoothex)

Supervisor:

* [Dr. Habib Mostafaei](https://www.tue.nl/en/research/researchers/habib-mostafaei/)
