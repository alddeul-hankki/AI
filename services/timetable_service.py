from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from sqlalchemy import text

KST = ZoneInfo("Asia/Seoul")

def anchor_to_10min_kst(dt: datetime | None = None,
                        grace_before_next_sec: int = 120,  # 다음 틱 직전 2분 이내면 다음 틱으로
                        grace_after_prev_sec: int = 120):  # 틱 직후 2분 이내면 그 틱으로
    """
    :00/:10/... '정각 10분 간격'으로 스냅.
    - 실행이 약간 빠른 59:58 → 00:00으로 '올림'
    - 실행이 약간 늦은 00:00:30 → 00:00으로 '고정'
    - 그 외 구간은 가까운 틱으로 반올림
    """
    if dt is None:
        dt = datetime.now(KST)
    dt = dt.astimezone(KST)

    m_floor = (dt.minute // 10) * 10
    lower = dt.replace(minute=m_floor, second=0, microsecond=0)
    next_tick = lower + timedelta(minutes=10)

    to_next = (next_tick - dt).total_seconds()
    from_lower = (dt - lower).total_seconds()

    # 다음 틱 아주 직전이면 다음 틱으로 스냅 (ceil)
    if 0 < to_next <= grace_before_next_sec:
        return next_tick
    # 틱 직후면 그 틱으로 스냅
    if 0 <= from_lower <= grace_after_prev_sec:
        return lower
    # 그 외에는 가까운 쪽으로 반올림
    return next_tick if to_next < from_lower else lower

# Monday=0 ... Sunday=6 (Python weekday() 규약)
def _today_dow_kst() -> int:
    now = datetime.now(KST)
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
    lookahead_min: int = 120,   # 예: 120분
    need_min: int = 30,         # 연속 30분
    empty_is: int = 0,          # 1=비어있음(공강), 1=바쁨이면 0으로 바꾸세요
    ref_time: datetime | None = None  # ← 추가: 공강 판단 기준 시각(앵커)
) -> bool:
    """오늘 288비트 + (필요시) 내일 288비트를 이어 붙여, ref_time 기준으로 연속 공강을 판단."""
    if not today9:
        return False

    today_bits = _slots_to_bits_288(today9)
    next_bits = _slots_to_bits_288(next9) if next9 else [0]*288

    # ✅ now 대신 '정각 기준 10분 앵커 시간'을 사용
    if ref_time is None:
        ref_time = anchor_to_10min_kst()
    else:
        ref_time = anchor_to_10min_kst(ref_time)

    start_idx = (ref_time.hour * 60 + ref_time.minute) // 5  # 5분 슬롯 인덱스
    H = max(1, lookahead_min // 5)
    need = max(1, need_min // 5)

    tail_today = today_bits[start_idx:]
    span = tail_today + next_bits  # 길이: (288-start_idx) + 288

    run = 0
    for b in span[:H]:
        run = run + 1 if b == empty_is else 0
        if run >= need:
            return True
    return False
