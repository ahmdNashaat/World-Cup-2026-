"""
load.py
يحمّل الصفوف المُحوّلة (من transform.py) إلى PostgreSQL.

مبدأ الـ Idempotency: لو شغّلنا هذا السكريبت 10 مرات على نفس البيانات،
النتيجة في الجدول تكون متطابقة دايمًا - مفيش صفوف مكررة.
بنحقق ده عن طريق UPSERT (INSERT ... ON CONFLICT DO UPDATE) باستخدام
match_id كـ primary key، مش INSERT عشوائي.

كل run بيسجل نفسه في جدول pipeline_logs: كام صف دخل، كام وقت استغرق،
هل فيه فشل - ده بيوضح "observability" بسيط للـ pipeline.
"""

import os
import time
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timezone

DB_CONFIG = {
    "host": os.environ.get("PIPELINE_POSTGRES_HOST", "localhost"),
    "port": os.environ.get("PIPELINE_POSTGRES_PORT", "5432"),
    "dbname": os.environ.get("PIPELINE_POSTGRES_DB", "worldcup_db"),
    "user": os.environ.get("PIPELINE_POSTGRES_USER", "worldcup_user"),
    "password": os.environ.get("PIPELINE_POSTGRES_PASSWORD", "worldcup_pass"),
}

DDL_STATEMENTS = """
CREATE TABLE IF NOT EXISTS matches (
    match_id        BIGINT PRIMARY KEY,
    utc_date        TIMESTAMPTZ NOT NULL,
    status          TEXT NOT NULL,
    matchday        INTEGER,
    stage           TEXT NOT NULL,
    match_group     TEXT,
    home_team_name  TEXT NOT NULL,
    home_team_tla   TEXT,
    away_team_name  TEXT NOT NULL,
    away_team_tla   TEXT,
    home_score      INTEGER,
    away_score      INTEGER,
    winner          TEXT,
    last_updated    TIMESTAMPTZ,
    loaded_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS pending_matches (
    match_id    BIGINT PRIMARY KEY,
    utc_date    TIMESTAMPTZ,
    status      TEXT NOT NULL,
    stage       TEXT NOT NULL,
    match_group TEXT,
    loaded_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS quarantine (
    id              SERIAL PRIMARY KEY,
    match_id        BIGINT,
    reason          TEXT NOT NULL,
    raw_payload     JSONB,
    quarantined_at  TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS pipeline_logs (
    id                  SERIAL PRIMARY KEY,
    run_started_at      TIMESTAMPTZ NOT NULL,
    run_finished_at     TIMESTAMPTZ NOT NULL,
    valid_rows_count    INTEGER NOT NULL,
    pending_rows_count  INTEGER NOT NULL,
    quarantined_count   INTEGER NOT NULL,
    duration_seconds    NUMERIC NOT NULL,
    status              TEXT NOT NULL
);
"""


def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def ensure_schema(conn) -> None:
    """ينشئ الجداول لو مش موجودة - safe للتشغيل المتكرر."""
    with conn.cursor() as cur:
        cur.execute(DDL_STATEMENTS)
    conn.commit()


def upsert_matches(conn, rows: list[dict]) -> None:
    """
    يحمّل المباريات الكاملة بطريقة idempotent.
    ON CONFLICT (match_id) DO UPDATE يعني: لو الـ match_id موجود قبل كده،
    حدّث القيم بدل ما تضيف صف جديد مكرر.
    """
    if not rows:
        return

    columns = list(rows[0].keys())
    values = [[row[col] for col in columns] for row in rows]

    update_clause = ", ".join(f"{col} = EXCLUDED.{col}" for col in columns if col != "match_id")

    query = f"""
        INSERT INTO matches ({", ".join(columns)})
        VALUES %s
        ON CONFLICT (match_id) DO UPDATE SET {update_clause}
    """

    with conn.cursor() as cur:
        execute_values(cur, query, values)
    conn.commit()


def upsert_pending_matches(conn, rows: list[dict]) -> None:
    """نفس مبدأ upsert_matches لكن لجدول pending_matches."""
    if not rows:
        return

    columns = list(rows[0].keys())
    values = [[row[col] for col in columns] for row in rows]

    update_clause = ", ".join(f"{col} = EXCLUDED.{col}" for col in columns if col != "match_id")

    query = f"""
        INSERT INTO pending_matches ({", ".join(columns)})
        VALUES %s
        ON CONFLICT (match_id) DO UPDATE SET {update_clause}
    """

    with conn.cursor() as cur:
        execute_values(cur, query, values)
    conn.commit()


def insert_quarantine(conn, rows: list[dict]) -> None:
    """
    يسجّل الصفوف المرفوضة. هنا عمدًا بنعمل INSERT بسيط مش UPSERT،
    لأن كل run جديد للـ pipeline قد يلاقي نفس الأخطاء تاني (لو الـ API
    لسه فيه نفس مشكلة البيانات) ونريد نتتبع تاريخ كل مرة حصل فيها كذا.
    """
    if not rows:
        return

    import json as json_lib

    query = """
        INSERT INTO quarantine (match_id, reason, raw_payload, quarantined_at)
        VALUES %s
    """
    values = [
        (r["match_id"], r["reason"], json_lib.dumps(r["raw_payload"]), r["quarantined_at"])
        for r in rows
    ]

    with conn.cursor() as cur:
        execute_values(cur, query, values)
    conn.commit()


def log_pipeline_run(conn, run_started_at: datetime, valid_count: int,
                      pending_count: int, quarantined_count: int,
                      status: str) -> None:
    """يسجل ملخص هذا الـ run في جدول pipeline_logs."""
    run_finished_at = datetime.now(timezone.utc)
    duration = (run_finished_at - run_started_at).total_seconds()

    query = """
        INSERT INTO pipeline_logs
            (run_started_at, run_finished_at, valid_rows_count,
             pending_rows_count, quarantined_count, duration_seconds, status)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    with conn.cursor() as cur:
        cur.execute(query, (
            run_started_at, run_finished_at, valid_count,
            pending_count, quarantined_count, duration, status
        ))
    conn.commit()


def load_all(valid_rows: list[dict], pending_rows: list[dict],
              quarantined_rows: list[dict]) -> None:
    """نقطة الدخول الرئيسية - تحمّل كل البيانات وتسجل الـ run."""
    run_started_at = datetime.now(timezone.utc)
    conn = get_connection()

    try:
        ensure_schema(conn)
        upsert_matches(conn, valid_rows)
        upsert_pending_matches(conn, pending_rows)
        insert_quarantine(conn, quarantined_rows)

        log_pipeline_run(
            conn, run_started_at,
            len(valid_rows), len(pending_rows), len(quarantined_rows),
            status="success",
        )
        print(
            f"[load] تم تحميل {len(valid_rows)} مباراة، "
            f"{len(pending_rows)} pending، {len(quarantined_rows)} في quarantine"
        )
    except Exception as exc:
        log_pipeline_run(
            conn, run_started_at,
            len(valid_rows), len(pending_rows), len(quarantined_rows),
            status=f"failed:{type(exc).__name__}",
        )
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    import json
    import sys
    from pathlib import Path
    from transform import transform_matches  # noqa: E402

    if len(sys.argv) < 2:
        print("استخدام: python load.py <path_to_raw_json>")
        sys.exit(1)

    raw_path = Path(sys.argv[1])
    with open(raw_path, encoding="utf-8") as f:
        raw_data = json.load(f)

    valid_rows, pending_rows, quarantined_rows = transform_matches(raw_data)
    load_all(valid_rows, pending_rows, quarantined_rows)