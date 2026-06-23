"""
extract.py
يسحب بيانات مباريات كأس العالم 2026 من football-data.org API
ويحفظها كـ raw JSON (بدون أي تعديل) في ملف محلي.

مبدأ مهم: الـ extract step بييجي ببيانات خام فقط، من غير transformation.
ده بيسهّل لو حصل خطأ تقدر ترجع للـ raw data وتعرف هل المشكلة في
السحب نفسه ولا في معالجة البيانات بعدين.
"""

import os
import json
import time
import requests
from datetime import datetime, timezone
from pathlib import Path

API_BASE_URL = "https://api.football-data.org/v4"
COMPETITION_CODE = "WC"  # World Cup
RAW_DATA_DIR = Path(__file__).parent / "raw_data"


def get_api_token() -> str:
    """يقرأ الـ API token من environment variable، يفشل بوضوح لو غير موجود."""
    token = os.environ.get("FOOTBALL_API_TOKEN")
    if not token:
        raise RuntimeError(
            "FOOTBALL_API_TOKEN غير موجود في environment variables. "
            "تأكد إنك عملت: export FOOTBALL_API_TOKEN=your_token "
            "أو حمّلت ملف .env قبل تشغيل الكود."
        )
    return token


# القيم المسموحة لـ status حسب التوثيق الرسمي - هنستخدمها كمان في الـ validation بعدين
VALID_MATCH_STATUSES = {
    "SCHEDULED", "TIMED", "IN_PLAY", "PAUSED", "EXTRA_TIME",
    "PENALTY_SHOOTOUT", "FINISHED", "SUSPENDED", "POSTPONED",
    "CANCELLED", "AWARDED",
}

VALID_GROUPS = {f"GROUP_{c}" for c in "ABCDEFGHIJKL"}


def fetch_matches(status: str | None = None) -> dict:
    """
    يسحب مباريات كأس العالم 2026 من الـ API.
    status: فلتر اختياري (مثلاً "FINISHED" أو "SCHEDULED") - لازم يكون من VALID_MATCH_STATUSES.
    يحترم rate limiting عن طريق قراءة response headers.
    """
    if status is not None and status not in VALID_MATCH_STATUSES:
        raise ValueError(f"status='{status}' غير صالح. القيم المسموحة: {VALID_MATCH_STATUSES}")

    token = get_api_token()
    headers = {"X-Auth-Token": token}
    url = f"{API_BASE_URL}/competitions/{COMPETITION_CODE}/matches"
    params = {"status": status} if status else {}

    response = requests.get(url, headers=headers, params=params, timeout=15)

    # نقرا الـ rate limit headers ونطبعها - ده اللي نبهنا عليه داني في الإيميل
    remaining = response.headers.get("X-RequestsAvailable")
    reset_seconds = response.headers.get("X-RequestCounter-Reset")
    if remaining is not None:
        print(f"[rate-limit] متبقي {remaining} requests، يعاد الضبط بعد {reset_seconds} ثانية")

    if response.status_code == 429:
        # Too Many Requests - ننتظر ونحاول تاني مرة واحدة فقط
        print("[rate-limit] تجاوزنا الحد، بننتظر 60 ثانية...")
        time.sleep(60)
        response = requests.get(url, headers=headers, timeout=15)

    response.raise_for_status()  # يرمي exception واضح لو فيه أي خطأ HTTP
    return response.json()


def save_raw_snapshot(data: dict) -> Path:
    """يحفظ نسخة خام من الداتا مع timestamp - مهم جدًا لـ traceability."""
    RAW_DATA_DIR.mkdir(exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    filepath = RAW_DATA_DIR / f"matches_{timestamp}.json"

    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"[extract] تم حفظ {len(data.get('matches', []))} مباراة في {filepath}")
    return filepath


if __name__ == "__main__":
    # هنسحب كل المباريات (بدون فلتر) عشان نغطي SCHEDULED و FINISHED معًا
    data = fetch_matches()
    save_raw_snapshot(data)