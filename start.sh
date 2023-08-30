#!/bin/bash

cd "$PWD/shell-scripts/"
source config.sh

if [[ "$DOCKER_DEPLOYMENT" = "false" ]]; then
    cd "$PROJECT_HOME/shell-scripts"
    ./standalone.sh start
fi

if [[ "$DOCKER_DEPLOYMENT" = "true" ]]; then
    # Comment out the following line if you do not want to rebuild the jar job
    cd "$PROJECT_HOME/shell-scripts/" && ./create-jobs.sh

    cd "$PROJECT_HOME/docker-app" && sudo ./start.sh
fi

# For testing only

# Comment out the following lines to extract the throughput

# sleep 300

# if [[ "$DOCKER_DEPLOYMENT" = "false" ]]; then
#     cd "$PROJECT_HOME/shell-scripts" && ./extract-throughput.sh
#     cd "$PROJECT_HOME" && ./stop.sh
# fi

# if [[ "$DOCKER_DEPLOYMENT" = "true" ]]; then
#     cd "$PROJECT_HOME"
#     sudo docker logs flink-taskmanager-2 > result.txt
#     cd "$PROJECT_HOME/shell-scripts" && ./extract-throughput.sh
#     cd "$PROJECT_HOME" && ./stop.sh
# fi

# Comment out the following lines to extract the end-to-end latency

# sleep 300

# if [[ "$DOCKER_DEPLOYMENT" = "false" ]]; then
#     cd "$PROJECT_HOME/shell-scripts" && ./extract-latency.sh
#     cd "$PROJECT_HOME" && ./stop.sh
# fi

# if [[ "$DOCKER_DEPLOYMENT" = "true" ]]; then
#     cd "$PROJECT_HOME"
#     sudo docker logs flink-taskmanager-2 > result.txt
#     cd "$PROJECT_HOME/shell-scripts" && ./extract-latency.sh
#     cd "$PROJECT_HOME" && ./stop.sh
# fi
