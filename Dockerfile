FROM apache/airflow:2.8.1
COPY airflow/requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt
