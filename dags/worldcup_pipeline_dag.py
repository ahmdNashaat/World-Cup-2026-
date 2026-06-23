"""
worldcup_pipeline_dag.py
DAG يربط 3 خطوات الـ pipeline:
extract (سحب من API) -> transform (تنظيف + Data Quality) -> load (PostgreSQL)

يعمل run تلقائي يوميًا، ويمكن تشغيله يدويًا (Trigger DAG) في أي وقت من الـ UI.
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# مجلد pipeline متاح للـ container عن طريق volume mount في docker-compose.yml
sys.path.insert(0, "/opt/airflow/pipeline")

from extract import fetch_matches, save_raw_snapshot  # noqa: E402
from transform import transform_matches  # noqa: E402
from load import load_all  # noqa: E402


default_args = {
    "owner": "ahmed",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


def task_extract(**context) -> str:
    """يسحب المباريات من الـ API ويحفظ raw snapshot، يرجع مسار الملف عبر XCom."""
    data = fetch_matches()
    filepath = save_raw_snapshot(data)
    return str(filepath)


def task_transform(**context) -> dict:
    """يقرأ الملف الخام من الـ task السابقة (عبر XCom) ويحوّله."""
    ti = context["ti"]
    raw_path = ti.xcom_pull(task_ids="extract")

    with open(raw_path, encoding="utf-8") as f:
        raw_data = json.load(f)

    valid_rows, pending_rows, quarantined_rows = transform_matches(raw_data)

    # XCom بيخزن JSON بسيط - مناسب لحجم بياناتنا (104 صف كحد أقصى)
    return {
        "valid_rows": valid_rows,
        "pending_rows": pending_rows,
        "quarantined_rows": quarantined_rows,
    }


def task_load(**context) -> None:
    """يستقبل البيانات المُحوّلة من الـ task السابقة ويحمّلها في PostgreSQL."""
    ti = context["ti"]
    transformed = ti.xcom_pull(task_ids="transform")

    load_all(
        transformed["valid_rows"],
        transformed["pending_rows"],
        transformed["quarantined_rows"],
    )


with DAG(
    dag_id="worldcup_etl_pipeline",
    description="Extract-Transform-Load pipeline لمباريات كأس العالم 2026",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2026, 6, 20),
    catchup=False,
    tags=["worldcup", "etl", "portfolio"],
) as dag:

    extract = PythonOperator(
        task_id="extract",
        python_callable=task_extract,
    )

    transform = PythonOperator(
        task_id="transform",
        python_callable=task_transform,
    )

    load = PythonOperator(
        task_id="load",
        python_callable=task_load,
    )

    extract >> transform >> load