#!/bin/bash

sudo docker rmi $(sudo docker images -q "data-aggregation-in-distributed-networks*") -f
sudo docker-compose down
sudo docker image prune -f
sudo docker-compose pull
