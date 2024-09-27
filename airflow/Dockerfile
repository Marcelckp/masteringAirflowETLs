FROM apache/airflow:latest
ADD requirements.txt .
RUN apt-get update && apt-get install -y gosu
RUN pip install apache-airflow==latest -r requirements.txt
RUN mkdir -p /opt/airflow/data_lake/raw
RUN mkdir -p /opt/airflow/data_lake/refined
RUN mkdir -p /opt/airflow/data_collection