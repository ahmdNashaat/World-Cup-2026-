"""
transform.py
يحوّل بيانات المباريات الخام (raw JSON من football-data.org) لصفوف "مسطّحة"
(flat dicts) جاهزة للتحميل في PostgreSQL.

في نفس الوقت بيطبّق Data Quality validation: أي صف بيانات مش مستوفي
الشروط الأساسية (مثلاً status خارج الـ enum المعروف، أو id مفقود)
يروح لقائمة "quarantine" منفصلة بدل ما يوقف الـ pipeline كله.

مبدأ: pipeline قوي لا يفشل بالكامل بسبب صف واحد فيه بيانات غريبة.
بيعزل المشكلة، يسجّلها، ويكمل شغله على باقي البيانات الصحيحة.
"""

from typing import Any
from datetime import datetime, timezone

# نفس الـ enums الرسمية اللي أكدنا عليها من التوثيق
VALID_MATCH_STATUSES = {
    "SCHEDULED", "TIMED", "IN_PLAY", "PAUSED", "EXTRA_TIME",
    "PENALTY_SHOOTOUT", "FINISHED", "SUSPENDED", "POSTPONED",
    "CANCELLED", "AWARDED",
}

VALID_GROUPS = {f"GROUP_{c}" for c in "ABCDEFGHIJKL"}

VALID_STAGES = {
    "FINAL", "THIRD_PLACE", "SEMI_FINALS", "QUARTER_FINALS",
    "LAST_16", "LAST_32", "LAST_64", "GROUP_STAGE",
}


class ValidationResult:
    """نتيجة فاليديشن صف واحد - واحدة من 3 حالات:
    1. صف صالح (is_valid=True, category='matches')
    2. مباراة منتظرة فريقين (is_valid=True, category='pending') - حالة عمل طبيعية، مش خطأ
    3. صف فيه خطأ بيانات حقيقي (is_valid=False) - يروح للـ quarantine
    """

    def __init__(self, is_valid: bool, row: dict | None = None,
                 raw_match: dict | None = None, reason: str | None = None,
                 category: str = "matches"):
        self.is_valid = is_valid
        self.row = row
        self.raw_match = raw_match
        self.reason = reason
        self.category = category  # 'matches' أو 'pending'


def validate_match(match: dict) -> ValidationResult:
    """
    يتحقق من صحة مباراة واحدة قبل التحويل.
    يرجع ValidationResult بدل ما يرمي exception - عشان نقدر نجمع
    كل الأخطاء بدون ما نوقف معالجة باقي المباريات.
    """
    match_id = match.get("id")
    if match_id is None:
        return ValidationResult(False, raw_match=match, reason="missing_id")

    status = match.get("status")
    if status not in VALID_MATCH_STATUSES:
        return ValidationResult(
            False, raw_match=match,
            reason=f"invalid_status:{status}"
        )

    stage = match.get("stage")
    if stage not in VALID_STAGES:
        return ValidationResult(
            False, raw_match=match,
            reason=f"invalid_stage:{stage}"
        )

    # group ممكن يكون None قانونيًا في أدوار خروج المغلوب - ده مش خطأ
    group = match.get("group")
    if group is not None and group not in VALID_GROUPS:
        return ValidationResult(
            False, raw_match=match,
            reason=f"invalid_group:{group}"
        )

    home_team = match.get("homeTeam") or {}
    away_team = match.get("awayTeam") or {}
    utc_date = match.get("utcDate")

    # حالة خاصة: مباراة محجوزة في الجدول (تاريخ، دور، ملعب معروف)
    # لكن الفريقين لسه معلّقين على نتائج تصفيات سابقة - مش خطأ بيانات،
    # دي حالة عمل طبيعية في مراحل الإقصاء المبكرة.
    is_pending = (not home_team.get("name")) and (not away_team.get("name"))
    if is_pending:
        pending_row = {
            "match_id": match_id,
            "utc_date": utc_date,
            "status": status,
            "stage": stage,
            "match_group": group,
        }
        return ValidationResult(True, row=pending_row, category="pending")

    # من هنا أي فريق ناقص اسمه يُعتبر خطأ بيانات حقيقي (واحد موجود والتاني لأ)
    if not home_team.get("name") or not away_team.get("name"):
        return ValidationResult(False, raw_match=match, reason="missing_team_name")

    if not utc_date:
        return ValidationResult(False, raw_match=match, reason="missing_utc_date")

    # لو المباراة FINISHED، لازم تكون فيها نتيجة فعلية (مش null)
    score = match.get("score") or {}
    full_time = score.get("fullTime") or {}
    if status == "FINISHED":
        if full_time.get("home") is None or full_time.get("away") is None:
            return ValidationResult(
                False, raw_match=match,
                reason="finished_match_missing_score"
            )

    # كل الفحوصات عدّت - نبني الصف النظيف
    row = {
        "match_id": match_id,
        "utc_date": utc_date,
        "status": status,
        "matchday": match.get("matchday"),
        "stage": stage,
        "match_group": group,  # group اسم محجوز في SQL، فهنسميه match_group
        "home_team_name": home_team.get("name"),
        "home_team_tla": home_team.get("tla"),
        "away_team_name": away_team.get("name"),
        "away_team_tla": away_team.get("tla"),
        "home_score": full_time.get("home"),
        "away_score": full_time.get("away"),
        "winner": score.get("winner"),
        "last_updated": match.get("lastUpdated"),
    }
    return ValidationResult(True, row=row)


def transform_matches(raw_data: dict) -> tuple[list[dict], list[dict], list[dict]]:
    """
    يحوّل كل المباريات من raw_data.
    يرجع tuple: (valid_rows, pending_rows, quarantined_rows)
    """
    matches = raw_data.get("matches", [])
    valid_rows = []
    pending_rows = []
    quarantined_rows = []

    for match in matches:
        result = validate_match(match)
        if result.is_valid and result.category == "matches":
            valid_rows.append(result.row)
        elif result.is_valid and result.category == "pending":
            pending_rows.append(result.row)
        else:
            quarantined_rows.append({
                "match_id": match.get("id"),
                "reason": result.reason,
                "raw_payload": match,
                "quarantined_at": datetime.now(timezone.utc).isoformat(),
            })

    print(
        f"[transform] صفوف صالحة: {len(valid_rows)} | "
        f"مباريات منتظرة فريقين: {len(pending_rows)} | "
        f"صفوف في الـ quarantine: {len(quarantined_rows)}"
    )
    return valid_rows, pending_rows, quarantined_rows


if __name__ == "__main__":
    import json
    import sys
    from pathlib import Path

    if len(sys.argv) < 2:
        print("استخدام: python transform.py <path_to_raw_json>")
        sys.exit(1)

    raw_path = Path(sys.argv[1])
    with open(raw_path, encoding="utf-8") as f:
        raw_data = json.load(f)

    valid_rows, pending_rows, quarantined_rows = transform_matches(raw_data)

    if quarantined_rows:
        print("\n[تفاصيل المباريات المرفوضة - أخطاء بيانات حقيقية]:")
        for q in quarantined_rows:
            print(f"  - match_id={q['match_id']} | السبب: {q['reason']}")