import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import (
    SparkKubernetesOperator,
)

with DAG(
    "spark_pi_yunikorn",
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    description="Submissão da SparkApplication Pi usando Spark Operator no YuniKorn",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    template_searchpath=str(Path(__file__).parent.resolve()),
    tags=["spark", "yunikorn"],
    ) as dag:
    submit_spark_pi = SparkKubernetesOperator(
        task_id="submit_spark_pi",
        namespace="spark-operator",
        application_file="pi-on-yunikorn.yaml",
        do_xcom_push=False,
        get_logs=True,
        params={
            "APP_ID": "spark-pi-{{ ds_nodash }}"
        },
    )

    submit_spark_pi
