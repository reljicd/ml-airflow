#!/usr/bin/env bash

export AIRFLOW_HOME=$(pwd)/airflow_home
echo -e "\nSetting AIRFLOW_HOME to ${AIRFLOW_HOME}...\n"
export PYTHONPATH=${PYTHONPATH}:${AIRFLOW_HOME}

# Make Airflow working dir if it already doesn't exists
mkdir -p ${AIRFLOW_HOME}

# Make symbolic link of dags folder in AIRFLOW_HOME
ln -sF $(pwd)/dags ${AIRFLOW_HOME}
# Make symbolic link of plugins folder in AIRFLOW_HOME
ln -sF $(pwd)/plugins ${AIRFLOW_HOME}
# Copy config file to AIRFLOW_HOME and substitute old AIRFLOW_HOME path in cfg file with the new one
cp -f $(pwd)/config/airflow.cfg ${AIRFLOW_HOME}/airflow.cfg
sed -Eie "s#([^(:3306)])/airflow([^.db|^:|^_]|$)#\1${AIRFLOW_HOME}\2#g" ${AIRFLOW_HOME}/airflow.cfg

# Activate virtual env
source venv/bin/activate

# Recreate db
airflow resetdb -y

# Adding connections:
# SQLite
airflow connections -a \
          --conn_id ${DB_CONN_ID} \
          --conn_type sqlite \
          --conn_host ${DB_HOST}

# Unpause our DAGs
airflow unpause training_dag
airflow unpause testing_dag