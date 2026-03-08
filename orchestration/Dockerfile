FROM apache/airflow:2.10.0

USER root
# Installation des dépendances système
RUN apt-get update \
  && apt-get install -y --no-install-recommends freetds-dev git \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

USER airflow
COPY requirements.txt /

# 1. On installe les providers Airflow normalement
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt

# 2. LA SOLUTION : On crée un environnement virtuel dédié pour dbt
# Cela évite les conflits avec Airflow
RUN python -m venv /home/airflow/dbt_venv && \
    /home/airflow/dbt_venv/bin/pip install --no-cache-dir dbt-snowflake