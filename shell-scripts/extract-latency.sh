#!/bin/bash

source config.sh

if [[ "$DOCKER_DEPLOYMENT" = "false" ]]; then
    # Specify the log file path
    log_file=$(ls -t "$FLINK_HOME_2/log/" | grep '\.out$' | head -n 1)

    # Number of latest records to consider
    num_records=48

    # Extract the latest records and calculate the mean
    mean_value=$(tail -n $num_records "$FLINK_HOME_2/log/$log_file" | grep -o 'Time Difference: [0-9.]* milliseconds' | awk '{ sum += $3; count++ } END { if (count > 0) print sum / count; else print 0 }')

    # Print the mean value
    echo "Mean value of the latest $num_records event time lags: $mean_value milliseconds"
fi

if [[ "$DOCKER_DEPLOYMENT" = "true" ]]; then
    cd "$PROJECT_HOME"
    log_file="result.txt"

    # Number of latest records to consider
    num_records=48

    # Extract the latest records and calculate the mean
    mean_value=$(tail -n $num_records "$PROJECT_HOME/$log_file" | grep -o 'Time Difference: [0-9.]* milliseconds' | awk '{ sum += $3; count++ } END { if (count > 0) print sum / count; else print 0 }')

    # Print the mean value
    echo "Mean value of the latest $num_records event time lags: $mean_value milliseconds"
fi
