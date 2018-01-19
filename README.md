# Airflow Playground

## About

This is a project for experimenting with implementation of different Airflow DAGs.

Airflow is run by starting Airflow webserver inside Docker container.

## Airflow configuration

Configuration file is found in **/config/airflow.cfg**

This file is copied to docker image during build.

## How to run

### Docker

It is possible to run Airflow using Docker:

Build the Docker image:
```bash
$ docker build -t reljicd/airflow-playground -f docker\Dockerfile .
```

Run the Docker container:
```bash
$ docker run --rm -i -p 8080:8080 -v dags:/airflow/dags reljicd/airflow-playground
```

This will also mount **/dags** folder inside docker container in order to facilitate real time experimentation with DAGs.

#### Helper script

It is possible to run all of the above with helper script:

```bash
$ chmod +x scripts/run_docker.sh
$ scripts/run_docker.sh
```

