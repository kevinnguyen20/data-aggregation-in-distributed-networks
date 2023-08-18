#!/bin/bash

# Read variables from config file
source config.sh

setParallelism() {
    configFile="$FLINK_HOME/conf/flink-conf.yaml"
    sed -i "s/taskmanager.numberOfTaskSlots: [0-9]*$/taskmanager.numberOfTaskSlots: $PARALLELISM/" "$configFile"
    sed -i "s/parallelism.default: [0-9]*$/parallelism.default: $PARALLELISM/" "$configFile"
}

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

startDataGenerators() {
    cd "$PROJECT_HOME"
    # python3 ./data-generators/datagen.py 1 &
    # python3 ./data-generators/datagen.py 2 &
    # python3 ./data-generators/continuousData.py 1 2 &
    # python3 ./data-generators/continuousData.py 2 3 &
    gcc -o ./data-generators/continuousData ./data-generators/continuousData.c -lrdkafka -lcjson -lm
    ./data-generators/continuousData 1 4 & # Change these numbers also below
    ./data-generators/continuousData 2 7 &
}

stopDataGenerators() {
    cd "$PROJECT_HOME"
    # pkill -f "python3 ./data-generators/datagen.py 1"
    # pkill -f "python3 ./data-generators/datagen.py 2"
    # pkill -f "./data-generators/continuousData.py 1 2"
    # pkill -f "./data-generators/continuousData.py 2 3"
    pkill -f "./data-generators/continuousData 1 4"
    pkill -f "./data-generators/continuousData 2 7"
}

if [[ "$1" = "start" ]]; then
    cd "$PROJECT_HOME/flink-aggregation-java" || exit
    mvn clean package

    # Start Zookeeper and the Kafka broker
    cd "$PROJECT_HOME/shell-scripts" || exit
    ./kafka-service.sh start
    # source ./delay.sh start

    # Start the data generators
    sleep 10
    startDataGenerators

    # Start the Flink cluster
    setParallelism
    "$FLINK_HOME/bin/start-cluster.sh" > /dev/null 2>&1 & # Start the first cluster
    # Uncomment in case the datagen.py (written in Python) is used as data
    # generator
    # sleep 5
    copyAndRenameFile
    "$FLINK_HOME_2/bin/start-cluster.sh" > /dev/null 2>&1 &

    # Submit the job to the Flink cluster
    # "$FLINK_HOME/bin/flink" run
    # "$FLINK_JOB_DIRECTORY/cluster1-1.0-SNAPSHOT.jar" > /dev/null 2>&1 &
    "$FLINK_HOME/bin/flink" run "$FLINK_JOB_DIRECTORY/cluster1-1.0-SNAPSHOT.jar" > /dev/null 2>&1 &
    # "$FLINK_HOME_2/bin/flink" run
    # "$FLINK_JOB_DIRECTORY_2/cluster2-1.0-SNAPSHOT.jar" > /dev/null 2>&1 &
    "$FLINK_HOME_2/bin/flink" run "$FLINK_JOB_DIRECTORY_2/cluster2-1.0-SNAPSHOT.jar" > /dev/null 2>&1 &

    # Uncomment if you are too lazy to open the links by yourself
    # sleep 5

    # xdg-open "http://localhost:8081" > /dev/null 2>&1 &
    # xdg-open "http://localhost:8091" > /dev/null 2>&1 &
fi

if [[ "$1" = "stop" ]]; then
    # Stop xdg-open
    # pkill -f "xdg-open"

    # Stop the Flink cluster
    "$FLINK_HOME/bin/stop-cluster.sh"
    "$FLINK_HOME_2/bin/stop-cluster.sh"

    # Stop the data generators
    stopDataGenerators

    # Stop Zookeeper and the Kafka broker
    # source ./delay.sh stop
    cd "$PROJECT_HOME/shell-scripts" || exit
    ./kafka-service.sh stop
fi
