#!/bin/bash

# Read variables from config file
source config.sh

adjustConfigForSecondCluster() {
    sed -i '1s/:[0-9]\{4\}$/:8091/' "$FLINK_HOME_2/conf/masters"

    configFile="$FLINK_HOME_2/conf/flink-conf.yaml"
    sed -i 's/jobmanager.rpc.port: [0-9]\{4\}$/jobmanager.rpc.port: 6124/' "$configFile"
    sed -i "/taskmanager.host: localhost/a \\
\\
# Manual entries \\
taskmanager.data.port: 42500 \\
taskmanager.rpc.port: 42501-42520" "$configFile"
    sed -i 's/#rest.port: 8081$/rest.port: 8091/' "$configFile"
    sed -i 's/#rest.bind-port: 8080-8090$/rest.bind-port: 8091-8101/' "$configFile"
}

copyAndRenameFile() {
    if [[ ! -d "$FLINK_HOME_2" ]]; then
        cp -r "$FLINK_HOME" "$FLINK_HOME_2"
        adjustConfigForSecondCluster
    fi
}

if [[ "$1" = "start" ]]; then
    cd "$PROJECT_HOME/flink-aggregation-java" || exit
    mvn clean package

    # Start Zookeeper and the Kafka broker
    cd "$PROJECT_HOME" || exit
    ./kafka-service.sh start

    # Start the Flink cluster
    "$FLINK_HOME/bin/start-cluster.sh" > /dev/null 2>&1 & # Start the first cluster
    copyAndRenameFile
    "$FLINK_HOME_2/bin/start-cluster.sh" > /dev/null 2>&1 & # Start the second cluster

    # Submit the job to the Flink cluster
    "$FLINK_HOME/bin/flink" run "$FLINK_JOB_DIRECTORY/cluster1-1.0-SNAPSHOT.jar" > /dev/null 2>&1 &
    "$FLINK_HOME_2/bin/flink" run "$FLINK_JOB_DIRECTORY_2/cluster2-1.0-SNAPSHOT.jar" > /dev/null 2>&1 &

# Uncomment if you are too lazy to open the links by yourself
   sleep 5

   xdg-open "http://localhost:8081" > /dev/null 2>&1 &
   xdg-open "http://localhost:8091" > /dev/null 2>&1 &
fi

if [[ "$1" = "stop" ]]; then
    # Stop xdg-open
    # pkill -f "xdg-open"

    # Stop the Flink cluster
    "$FLINK_HOME/bin/stop-cluster.sh"
    "$FLINK_HOME_2/bin/stop-cluster.sh"

    # Stop Zookeeper and the Kafka broker
    ./kafka-service.sh stop
fi
