# Data Aggregation in Distributed Networks

In this project we are using Apache Flink for executing queries in different networks, whose result we then transfer to a data aggregator. In the links between the networks and the aggregator we emulate bad network conditions using Pumba in order to measure the impact of these on the whole execution time.

# Steps to run the aplication
Prerequisites:
* Download Kafka binary `kafka_2.13-3.4.0` and Apache Flink binary `flink-1.17.1`, unzip them and don't change anything in the Flink config directory.

1. You have to create a `config.sh` file from the `config.sh.template` and put the paths for your pc and place it in the root directory.
2. Use the command `./standalone.sh start` to start everything. (the first time you execute it another flink directory for the second cluster will be created locally)
3. At the end don't forget to execute command `./standalone.sh stop` to stop everything.
