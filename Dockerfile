FROM apache/airflow:2.8.1
COPY airflow/requirements.txt /
RUN pip install --default-timeout=1000 --retries 10 --no-cache-dir -r /requirements.txt
