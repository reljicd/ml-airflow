# ML Airflow

## About

This is generalized project for running Airflow DAGs, with possibility of skipping tasks already done for some set of input parameters.

Metadata about DAGs (i.e. parameters set: parameter_1...)
and tasks (datetime_started, datetime_finished, output_path...) 
is saved in **ml_airflow** DB schema. SQL files (DDL and test data set) for MySQL and SQLite are in folder ***sql***.

Based on this metadata from DB, already done tasks (for some set of input parameters and samples) will be skipped.

When running DAGs (training or testing) for each of the tasks python logic will first do some pre processing (write some metadata in db for example), 
check if task is already done, skip it if it is (based on the datetime_finished in task's table), or run it if it is not.
If it is run, or rerun, python logic will first start task by writing datetime_started, call Bash using proper parameters for
that task, after that finish task by writing datetime_finished and do some additional post processing if there is need.

Customization of Bash parameters for each task, as well customization of output paths for each tasks is explained later in this document

Airflow is being run by starting Airflow web server and scheduler inside Docker container.

## Customization

Each of the tasks can have its own Python class, that is a subclass of 
**dags.subdags.base_subdag.MLTaskSubDag**:

* Common Task 1 - **dags.subdags.common_task_1_subdag.CommonTask1SubDag**

**dags.subdags.base_subdag.MLTaskSubDag** has one abstract method: 

* **_parameters_provider** - overridden in each task subclass, which should provide Bash parameters for that task

By customizing this methods in each task subclass it is possible to construct parameters programmatically 
using metadata from DB or some calculated info.

## Airflow configuration

Configuration file is found in **config/airflow.cfg**

This file is copied to Docker image during build, and modified and copied to **airflow_home** folder for local installation.

### Configuration parameters

Configuration parameters are passed through environment variables:

* **DB_HOST** - DB host

* **DB_LOGIN** - DB login username

* **DB_PASSWORD** - DB password

* **DB_SCHEMA** - DB schema

* **DB_CONN_ID** - DB connection ID

## Prerequisites

\[Optional\] Install virtual environment:

```bash
$ python -m virtualenv venv
```

\[Optional\] Activate virtual environment:

On macOS and Linux:
```bash
$ source venv/bin/activate
```

On Windows:
```bash
$ .\venv\Scripts\activate
```

Install dependencies:
```bash
$ pip install -r requirements.txt
```

## How to run

### Docker

It is possible to run Airflow using Docker:

Build Docker image:
```bash
$ docker build -t reljicd/ml-airflow -f docker\Dockerfile .
```

Run Docker container:
```bash
$ docker run --rm -i -p 8080:8080 reljicd/ml-airflow
```

#### Helper script

It is possible to run all of the above with helper script:

```bash
$ chmod +x scripts/run_docker.sh
$ scripts/run_docker.sh
```

### Docker Compose

Docker Compose file **docker/docker-compose.yml** is written to facilitate running of both properly initialized test MySQL DB, 
as well of the Airflow inside Docker containers.

#### Helper script

It is possible to run all of the above with helper script:

```bash
$ chmod +x scripts/run_docker_compose.sh
$ scripts/run_docker_compose.sh
```

## Airflow CLI in Docker

Since we named Docker container **"ml-airflow"** in our **run_docker.sh** script, we can run
any [Airflow CLI command](https://airflow.apache.org/cli.html) inside of Docker container as:

```bash
$ docker exec $(docker ps -aqf "name=ml-airflow") airflow [COMMAND]
```

For example:

```bash
$ docker exec $(docker ps -aqf "name=ml-airflow") airflow list_dags
```

## Triggering DAG Runs

### CLI in Docker

Example:

```bash
$ docker exec $(docker ps -aqf "name=ml-airflow") airflow trigger_dag -c CONF dag_id
```

Where CONF is JSON string that gets pickled into the DagRun’s conf attribute, i.e.: '{"foo":"bar"}'

#### ML Testing DAG

```bash
$ docker exec $(docker ps -aqf "name=ml-airflow") airflow trigger_dag -c '{"parameter_1":"parameter_1","parameter_3":"parameter_3"}' ml_testing_dag
```

#### ML Training DAG

```bash
$ docker exec $(docker ps -aqf "name=ml-airflow") airflow trigger_dag -c '{"parameter_1":"parameter_1","parameter_2":"parameter_2"}' ml_training_dag
```

### REST API

It is possible to trigger dags using GET HTTP method and proper URLs (basically copy pasting these URLs in browser's URL field, and calling it just like any other URL)

```
http://{AIRFLOW_HOST}:{AIRFLOW_PORT}/admin/rest_api/api?api=trigger_dag&dag_id=value&conf=value
```

Where CONF is JSON string that gets pickled into the DagRun’s conf attribute, i.e.: {"foo":"bar"}

#### ML Testing DAG

```
http://{AIRFLOW_HOST}:{AIRFLOW_PORT}/admin/rest_api/api?api=trigger_dag&dag_id=ml_testing_dag&conf={"parameter_1":"parameter_1","parameter_3":"parameter_3"}
```

#### ML Training DAG

```
http://{AIRFLOW_HOST}:{AIRFLOW_PORT}/admin/rest_api/api?api=trigger_dag&dag_id=ml_trainig_dag&conf={"parameter_1":"parameter_1","parameter_2":"parameter_2"}
```

## Testing DAG Tasks

It is possible to test DAG task instances locally with **airflow test** command. This command outputs their log to stdout (on screen), 
doesnt bother with dependencies, and doesnt communicate state (running, success, failed, …) to the database. 
It simply allows testing a single task instance.

For example:

```bash
$ docker exec $(docker ps -aqf "name=ml-airflow") airflow test dag_id task_id 2015-06-01
```

## Airflow Web UI

After starting Docker container, it is possible to access Airflow's Web UI on [http://localhost:8080/admin/](http://localhost:8080/admin/)

## Airflow REST API

It is possible to use all the Airflow's CLI commands through REST API.

A [plugin](https://github.com/teamclairvoyant/airflow-rest-api-plugin) for Apache Airflow that exposes REST endpoints for the Command Line Interfaces listed in the airflow documentation:

http://airflow.incubator.apache.org/cli.html

The plugin also includes other custom REST APIs.

Once you deploy the plugin and restart the web server, you can start to use the REST API. Bellow you will see the endpoints that are supported. In addition, you can also interact with the REST API from the Airflow Web Server. When you reload the page, you will see a link under the Admin tab called "REST API". Clicking on the link will navigate you to the following URL:

```
http://{AIRFLOW_HOST}:{AIRFLOW_PORT}/admin/rest_api/
```

This web page will show the Endpoints supported and provide a form for you to test submitting to them.

## Docker 

Folder **docker** contains Dockerfiles:

* **docker/docker-compose.yml** - Docker Compose file. Instructions for running Docker containers of **ml-airflow**
as well of **MySQL** Docker container, with proper mounting of sql files from **sql** folder, so that MySQL is initialized with
ml_airflow schema properly.

* **docker/Dockerfile** - Docker build file for ml-airflow

* **docker/initialize_airflow.sh** - Airflow initialization script (making DB connection and unpausing ml_training_dag and ml_testing_dag). This script
is run in **ml-airflow** during starting of Airflow scheduler.

* **docker/supervisord.conf** - Supervisor's config file

## Util Scripts

* **scripts/init_airflow.sh** - util script for initialization of local Airflow installation.

* **scripts/run_docker.sh** - util script for building Docker image, and running Docker container with passing to it
of proper env variables.

* **scripts/run_docker_compose.sh** - util script for running Docker Compose with export of proper env variables.

## Tests

Tests can be run by executing following command from the root of the project (dependencies for the project need to be installed, of course):

```bash
$ python -m pytest
```