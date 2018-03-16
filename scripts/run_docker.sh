#!/usr/bin/env bash

CONTAINER_NAME=ml-airflow
echo -e "\nSet docker container name as ${CONTAINER_NAME}\n"
IMAGE_NAME=${CONTAINER_NAME}:dev
echo -e "\nSet docker image name as ${IMAGE_NAME}\n"
PORT=8080
echo -e "Set docker image PORT to ${PORT}\n"
AIRFLOW_HOME=/airflow
echo -e "Airflow home is set to ${AIRFLOW_HOME}\n"

echo -e "\nStop running Docker containers with image tag ${CONTAINER_NAME}...\n"
docker stop $(docker ps -a | grep ${CONTAINER_NAME} | awk '{print $1}')

chmod +x docker/initialize_airflow.sh

echo -e "\nDocker build image with name ${IMAGE_NAME}...\n"
docker build -t ${IMAGE_NAME} -f docker/Dockerfile .

echo -e "\nStart Docker container of the image ${IMAGE_NAME} with name ${CONTAINER_NAME}...\n"
docker run --rm -i \
    -p ${PORT}:${PORT} \
    -e DB_HOST=${DB_HOST} \
    -e DB_LOGIN=${DB_LOGIN} \
    -e DB_PASSWORD=${DB_PASSWORD} \
    -e DB_SCHEMA=${DB_SCHEMA} \
    -e DB_CONN_ID=${DB_CONN_ID} \
    --name ${CONTAINER_NAME} \
    ${IMAGE_NAME}