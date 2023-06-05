#!/bin/bash

source config.sh


for ((i=1; i<=100; i++))
do
  JSON="{\"id\": $i, \"name\": \"Apple\", \"price\": 0.85}"

  echo $JSON | "$KAFKA_HOME/bin/kafka-console-producer.sh" --bootstrap-server $KAFKA_BOOTSTRAP_SERVERS --topic $CONSUMER_TOPIC
done
