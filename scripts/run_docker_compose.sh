#!/usr/bin/env bash

export CONTAINER_NAME=ml-airflow
echo -e "\nSet docker container name as ${CONTAINER_NAME}\n"

echo -e "\nStop running Docker containers with container name ${CONTAINER_NAME}...\n"
docker stop $(docker ps -a | grep ${CONTAINER_NAME} | awk '{print $1}')

echo -e "\nStop and delete MySQL containers... \n"
docker stop $(docker ps -a | grep mysql | awk '{print $1}')
docker rm $(docker ps -a | grep mysql | awk '{print $1}')

chmod +x docker/initialize_airflow.sh

# Variables that needs to be set for docker images
export DB_LOGIN=${DB_LOGIN}
export DB_PASSWORD=${DB_PASSWORD}
export DB_SCHEMA=${DB_SCHEMA}
export DB_CONN_ID=${DB_CONN_ID}

echo -e "\nStart Docker Compose...\n"
docker-compose -f docker/docker-compose.yml up --force-recreate --abort-on-container-exit