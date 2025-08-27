from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from sqlalchemy import text

# Monday=0 ... Sunday=6 (Python weekday() 규약)
def _today_dow_kst() -> int:
    now = datetime.now(ZoneInfo("Asia/Seoul"))
    return now.weekday()  # 0~6

def _pack_slots_row(row) -> list[int]:
    # row: {"slot1":..., "slot2":..., ..., "slot9":...}
    return [int(row[f"slot{i}"]) for i in range(1, 10)]

def fetch_slots_for_users(db, user_ids: list[int], dow: int) -> dict[int, list[int]]:
    """
    timetable_bit에서 오늘 요일(dow)의 slot1~slot9만 user_id IN (...)으로 조회.
    return: {user_id: [slot1..slot9]}
    """
    if not user_ids:
        return {}
    # SQLAlchemy IN 바인딩
    placeholders = ", ".join([":u"+str(i) for i in range(len(user_ids))])
    params = {("u"+str(i)): int(uid) for i, uid in enumerate(user_ids)}
    params["dow"] = int(dow)

    q = text(f"""
      SELECT user_id, slot1, slot2, slot3, slot4, slot5, slot6, slot7, slot8, slot9
      FROM timetable_bit
      WHERE day_of_week = :dow
        AND user_id IN ({placeholders})
    """)
    rows = db.execute(q, params).mappings().all()
    return {int(r["user_id"]): _pack_slots_row(r) for r in rows}

def _slots_to_bits_288(slots9: list[int]) -> list[int]:
    bits = []
    for val in slots9:
        for i in range(32):
            bits.append((val >> i) & 1)
    return bits[:288]

def has_meal_window_twoday(
    today9: list[int],
    next9: list[int] | None,
    lookahead_min: int = 30,  # 예: 120분
    need_min: int = 30,        # 연속 30분
    empty_is: int = 0          # 1=비어있음(공강)인 환경이면 1, 1=바쁨이면 0으로 바꾸세요
) -> bool:
    """오늘 288비트 + (필요시) 내일 288비트를 이어 붙여 연속 공강을 판단."""
    if not today9:
        return False
    today_bits = _slots_to_bits_288(today9)
    next_bits = _slots_to_bits_288(next9) if next9 else [0]*288

    now = datetime.now(ZoneInfo("Asia/Seoul"))
    start_idx = (now.hour * 60 + now.minute) // 5  # 5분 단위 슬롯 인덱스
    H = max(1, lookahead_min // 5)
    need = max(1, need_min // 5)

    # 오늘 뒤쪽 + 내일 앞쪽으로 이어 붙여서 최대 576비트에서 창을 본다
    tail_today = today_bits[start_idx:]
    span = tail_today + next_bits  # 길이: (288-start_idx) + 288

    # 연속 empty_is 길이 검사
    run = 0
    for b in span[:H]:
        run = run + 1 if b == empty_is else 0
        if run >= need:
            return True
    return False
