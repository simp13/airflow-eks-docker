FROM apache/airflow:2.1.3

#LABEL version="1.0.0"

RUN pip install --user pytest

COPY dags/ /opt/airflow/dags
COPY unittests.cfg /opt/airflow/unittests.cfg
COPY unittests/ /opt/airflow/unittests
COPY integrationtests /opt/airflow/integrationtests
