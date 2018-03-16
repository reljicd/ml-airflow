#!/usr/bin/env bash

airflow initdb

# Adding connections:
#MySQL
airflow connections -a \
          --conn_id ${DB_CONN_ID} \
          --conn_type mysql \
          --conn_host ${DB_HOST} \
          --conn_login ${DB_LOGIN} \
          --conn_password ${DB_PASSWORD} \
          --conn_schema ${DB_SCHEMA}

airflow unpause training_dag
airflow unpause testing_dag