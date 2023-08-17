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
