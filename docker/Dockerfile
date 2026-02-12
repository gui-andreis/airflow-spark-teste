FROM apache/airflow:2.9.3-python3.10

USER root

RUN apt-get update && apt-get install -y \
    openjdk-17-jdk \
    && apt-get clean

USER airflow

COPY --chown=airflow:0 docker/requirements.txt /requirements.txt

# Instala garantindo que o airflow core não seja tocado (--upgrade-strategy only-if-needed)
RUN pip install --no-cache-dir -r /requirements.txt
