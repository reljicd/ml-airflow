FROM python:3.6

EXPOSE 8080

ENV AIRFLOW_HOME /airflow
ENV PYTHONPATH ${PYTHONPATH}:${AIRFLOW_HOME}

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY config/airflow.cfg $AIRFLOW_HOME/airflow.cfg
COPY dags $AIRFLOW_HOME/dags
COPY plugins $AIRFLOW_HOME/plugins
COPY docker/initialize_airflow.sh /initialize_airflow.sh
COPY docker/supervisord.conf /etc/supervisor/conf.d/supervisord.conf

ARG AIRFLOW_REST_API_VERSION=1.0.4
RUN useradd -ms /bin/bash -d ${AIRFLOW_HOME} airflow \
        && chown -R airflow ${AIRFLOW_HOME} \
        && apt-get update -y \
        && apt-get install -y unzip \
        && apt-get install -y supervisor \
        && rm -rf /var/lib/apt/lists/* \
        && mkdir /log \
        && chmod 777 /log \
        && wget https://github.com/teamclairvoyant/airflow-rest-api-plugin/archive/v$AIRFLOW_REST_API_VERSION.zip \
        && unzip v$AIRFLOW_REST_API_VERSION.zip \
        && cp -r ./airflow-rest-api-plugin-$AIRFLOW_REST_API_VERSION/plugins $AIRFLOW_HOME/plugins

USER airflow

ENTRYPOINT ["/usr/bin/supervisord", "--configuration=/etc/supervisor/conf.d/supervisord.conf"]