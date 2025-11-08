FROM apache/airflow:2.9.3-python3.12

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends build-essential git \
 && rm -rf /var/lib/apt/lists/*

USER airflow

ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1

ARG AIRFLOW_VERSION=2.9.3
ARG PYTHON_VERSION=3.12

ENV AIRFLOW_CONSTRAINTS_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

COPY requirements.txt /tmp/requirements.txt

RUN pip install --no-cache-dir --constraint "$AIRFLOW_CONSTRAINTS_URL" \
      "apache-airflow-providers-snowflake==5.6.0" \
      "snowflake-connector-python>=3.10,<4" \
      "pandas>=2.0,<2.3" \
  \
 && pip install --no-cache-dir \
      "dbt-core==1.8.8" \
      "dbt-snowflake==1.8.4" \
      "yfinance==0.2.66" \
      "statsmodels==0.14.5" \
      "sentence-transformers==5.1.2" \
      "pinecone==7.3.0" \
  \
 && pip install --no-cache-dir -r /tmp/requirements.txt \
 \
 && dbt --version


