#!/bin/bash

# Read variables from config file
source config.sh

if [[ "$1" = "start" ]]; then
    cd "$PROJECT_HOME/flink-aggregation-java"
    mvn clean package

    # Start Zookeeper and the Kafka broker
    cd "$PROJECT_HOME"
    ./kafka-service.sh start

    # Start the Flink cluster
    "$FLINK_HOME/bin/start-cluster.sh" > /dev/null 2>&1 &
    # "$FLINK_HOME_2/bin/start-cluster.sh" > /dev/null 2>&1 &

    # Submit the job to the Flink cluster
    "$FLINK_HOME/bin/flink" run "$FLINK_JOB_DIRECTORY/cluster1-1.0-SNAPSHOT.jar" > /dev/null 2>&1 &
    # "$FLINK_HOME_2/bin/flink" run "$FLINK_JOB_DIRECTORY_2/cluster2-1.0-SNAPSHOT.jar" > /dev/null 2>&1 &
fi

if [[ "$1" = "stop" ]]; then
    # Stop the Flink cluster
    "$FLINK_HOME/bin/stop-cluster.sh"
    # "$FLINK_HOME_2/bin/stop-cluster.sh"

    # Stop Zookeeper and the Kafka broker
    ./kafka-service.sh stop
fi
