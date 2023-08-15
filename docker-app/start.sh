#!/bin/bash

docker-compose up -d

sleep 10

# Copy the JAR file into the jobmanager container
sudo docker cp ./jobs/cluster1-1.0-SNAPSHOT.jar flink-jobmanager1:/opt/flink
sudo docker cp ./jobs/cluster2-1.0-SNAPSHOT.jar flink-jobmanager2:/opt/flink
sudo docker cp delays.txt flink-jobmanager1:/opt/flink
sudo docker cp delays.txt flink-jobmanager2:/opt/flink

# Submit the job to the runnning Flink cluster
sudo docker exec -it flink-jobmanager1 /opt/flink/bin/flink run -d /opt/flink/cluster1-1.0-SNAPSHOT.jar
sudo docker exec -it flink-jobmanager2 /opt/flink/bin/flink run -d /opt/flink/cluster2-1.0-SNAPSHOT.jar
