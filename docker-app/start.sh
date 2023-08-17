#!/bin/bash

docker compose up -d

sleep 10

# Copy the JAR file into the jobmanager container
docker cp ./jobs/docker-flink-job-1.jar flink-jobmanager-1:/opt/flink
docker cp ./jobs/docker-flink-job-2.jar flink-jobmanager-2:/opt/flink
docker cp delays.txt flink-jobmanager-1:/opt/flink
docker cp delays.txt flink-jobmanager-2:/opt/flink

# Submit the job to the runnning Flink cluster
docker exec -it flink-jobmanager-1 /opt/flink/bin/flink run -d /opt/flink/docker-flink-job-1.jar
docker exec -it flink-jobmanager-2 /opt/flink/bin/flink run -d /opt/flink/docker-flink-job-2.jar
