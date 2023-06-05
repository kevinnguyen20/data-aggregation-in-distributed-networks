#!/bin/bash

source config.sh

start_time=$(date +%s%N)

for ((i=1; i<=25; i++))
do
  JSON="{\"id\": $i, \"name\": \"Apple\", \"price\": 0.85}"

  echo $JSON | "$KAFKA_HOME/bin/kafka-console-producer.sh" --bootstrap-server $KAFKA_BOOTSTRAP_SERVERS --topic $CONSUMER_TOPIC
done

end_time=$(date +%s%N)

elapsed_time=$((($end_time - $start_time) / 1000000))

echo "Elapsed time: $elapsed_time ms"
