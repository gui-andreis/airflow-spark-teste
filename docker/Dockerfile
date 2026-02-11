FROM apache/airflow:2.9.3-python3.10

USER root

RUN apt-get update && apt-get install -y \
    openjdk-17-jdk \
    && apt-get clean

USER airflow

COPY docker/requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
