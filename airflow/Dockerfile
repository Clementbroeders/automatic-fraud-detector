FROM apache/airflow:2.8.3

USER root
RUN apt-get update && apt-get install -y libgomp1

USER airflow
WORKDIR /opt/airflow
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt