FROM python:3.6

EXPOSE 8080

ENV AIRFLOW_HOME /airflow

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

RUN useradd -ms /bin/bash -d ${AIRFLOW_HOME} airflow
RUN chown -R airflow ${AIRFLOW_HOME}
USER airflow

RUN airflow initdb

COPY config/airflow.cfg $AIRFLOW_HOME/airflow.cfg
COPY dags $AIRFLOW_HOME/dags

ENTRYPOINT ["airflow", "webserver"]