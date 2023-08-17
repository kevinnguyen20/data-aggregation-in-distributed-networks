#!/bin/bash

source config.sh

# Change port numbers
cd "$PROJECT_HOME"
sed -i "s/localhost:9092/kafka:29092/" "./shell-scripts/config.sh"
sed -i "s/localhost:9092/kafka:29092/" "./data-generators/continuousData.c"
sed -i "s/localhost:9092/kafka:29092/" "./flink-aggregation-java/cluster1/src/main/resources/flink.properties"
sed -i "s/localhost:9092/kafka:29092/" "./flink-aggregation-java/cluster2/src/main/resources/flink.properties"

# Create the jar files
cd "$PROJECT_HOME/flink-aggregation-java" || exit
mvn clean package

# Move the files to Docker dir
cd "$FLINK_JOB_DIRECTORY"
mv -u "cluster1-1.0-SNAPSHOT.jar" "$PROJECT_HOME/docker-app/jobs/docker-flink-job-1.jar"
cd "$FLINK_JOB_DIRECTORY_2"
mv -u "cluster2-1.0-SNAPSHOT.jar" "$PROJECT_HOME/docker-app/jobs/docker-flink-job-2.jar"

# Undo the port number replacements
cd "$PROJECT_HOME"
sed -i "s/kafka:29092/localhost:9092/" "./shell-scripts/config.sh"
sed -i "s/kafka:29092/localhost:9092/" "./data-generators/continuousData.c"
sed -i "s/kafka:29092/localhost:9092/" "./flink-aggregation-java/cluster1/src/main/resources/flink.properties"
sed -i "s/kafka:29092/localhost:9092/" "./flink-aggregation-java/cluster2/src/main/resources/flink.properties"
