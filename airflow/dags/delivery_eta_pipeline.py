"""
wait_for_orders  — ждёт, пока генератор наполнит delivery.orders (gate с retry).
train_eta_model  — распределённый Spark ML job на 3 воркерах: читает заказы -> линейная регрессия -> пишет коэффициенты в CH.
notify_service   — имитирует нестабильный внешний сервис уведомлений:
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

CH_HOST = os.getenv("CH_HOST", "clickhouse")
CH_PORT = int(os.getenv("CH_PORT", "8123"))

SPARK_MASTER = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")
DRIVER_HOST = os.getenv("AIRFLOW_DRIVER_HOST", "airflow")
JOB_PATH = "/opt/spark_jobs/train_eta_model.py"


def _wait_for_orders(**_):
    import clickhouse_connect

    client = clickhouse_connect.get_client(host=CH_HOST, port=CH_PORT)
    count = client.command("SELECT count() FROM delivery.orders")
    print(f"[wait_for_orders] заказов в delivery.orders: {count}")
    if int(count) < 500:
        raise AirflowException(f"Заказов пока мало ({count}), ждём генератор...")
    return int(count)


def _notify_service(**context):
    # Нестабильный внешний сервис уведомлений: успешен только с 3-й попытки.
    try_number = context["ti"].try_number
    print(f"[notify_service] попытка №{try_number} отправки уведомления")
    if try_number < 3:
        raise AirflowException(
            f"Сервис уведомлений недоступен (попытка {try_number}) — Airflow выполнит retry"
        )
    print("[notify_service] уведомление отправлено на попытке 3")
    return "ok"


default_args = {
    "owner": "delivery-team",
    "retries": 5,
    "retry_delay": timedelta(seconds=10),
}

with DAG(
    dag_id="delivery_eta_ml_pipeline",
    description="ETA-модель доставки: Spark + ClickHouse, с парой задач и retry",
    default_args=default_args,
    schedule=None,            # запускаем вручную через Trigger DAG
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["delivery", "spark", "clickhouse", "ml"],
) as dag:

    wait_for_orders = PythonOperator(
        task_id="wait_for_orders",
        python_callable=_wait_for_orders,
    )

    train_eta_model = BashOperator(
        task_id="train_eta_model",
        bash_command=(
            # spark-submit берём из пакета pyspark, чтобы не зависеть от PATH
            "SPARK_SUBMIT=$(python -c "
            "\"import os, pyspark; "
            "print(os.path.join(os.path.dirname(pyspark.__file__), 'bin', 'spark-submit'))\") "
            "&& \"$SPARK_SUBMIT\" "
            f"--master {SPARK_MASTER} "
            f"--conf spark.driver.host={DRIVER_HOST} "
            "--conf spark.driver.bindAddress=0.0.0.0 "
            "--conf spark.cores.max=3 "
            "--conf spark.executor.cores=1 "
            "--conf spark.executor.memory=1g "
            f"{JOB_PATH}"
        ),
        env={
            "CH_HOST": CH_HOST,
            "CH_PORT": str(CH_PORT),
            "PATH": os.environ.get("PATH", ""),
            "JAVA_HOME": os.environ.get("JAVA_HOME", ""),
        },
        append_env=True,
    )

    notify_service = PythonOperator(
        task_id="notify_service",
        python_callable=_notify_service,
    )

    wait_for_orders >> [train_eta_model, notify_service]
