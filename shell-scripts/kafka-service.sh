#!/bin/bash

source config.sh

cd "$KAFKA_HOME" || exit

if [[ "$1" = "start" ]]; then
    # Start Zookeeper
    bin/zookeeper-server-start.sh config/zookeeper.properties > /dev/null 2>&1 &

    # Start the Kafka broker
    bin/kafka-server-start.sh config/server.properties > /dev/null 2>&1 &

    # Create a topic
    bin/kafka-topics.sh --create --topic "$CONSUMER_TOPIC" --partitions "$PARALLELISM" --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" > /dev/null 2>&1 &
    bin/kafka-topics.sh --create --topic "$CONSUMER_TOPIC_2" --partitions "$PARALLELISM" --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" > /dev/null 2>&1 &
    bin/kafka-topics.sh --create --topic "$CONSUMER_TOPIC_CLUSTERS" --partitions "$PARALLELISM" --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" > /dev/null 2>&1 &
fi

if [[ "$1" = "stop" ]]; then
    # Stop Zookeeper and the Kafka broker
    bin/zookeeper-server-stop.sh
    bin/kafka-server-stop.sh

    rm -rf /tmp/kafka-logs /tmp/zookeeper /tmp/kraft-combined-logs
fi
