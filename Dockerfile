
FROM apache/airflow:2.10.5-python3.11

RUN pip install --no-cache-dir \
    "apache-airflow-providers-mysql==5.7.4" \
    "apache-airflow-providers-common-sql==1.21.0"
