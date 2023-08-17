#!/bin/bash

cd "$PWD/shell-scripts/"
source config.sh

if [[ "$DOCKER_DEPLOYMENT" = "false" ]]; then
    cd "$PROJECT_HOME/shell-scripts"
    ./standalone.sh stop
fi

if [[ "$DOCKER_DEPLOYMENT" = "true" ]]; then
    cd "$PROJECT_HOME/docker-app" && sudo ./stop.sh
fi
