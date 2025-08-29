from datetime import datetime, timedelta, time
from zoneinfo import ZoneInfo
from typing import Dict, List, Tuple, Optional
from sqlalchemy import text

KST = ZoneInfo("Asia/Seoul")

SLOTS_PER_DAY = 288  # 5분 * 24시간
SLOT_MIN = 5

def _unpack_9x32_to_288(slots_9: List[int]) -> List[int]:
    """
    MySQL timetable_bit의 slot1..slot9(각 32bit)를 288비트 배열로 언팩.
    가정:
      - slot1이 하루의 시작(00:00~02:40), slot9이 끝(21:20~24:00 직전)
      - 각 slotN 정수의 LSB가 더 이른 시간 슬롯
      - bit=1 이면 '수업(바쁨)', bit=0 이면 '공강(비어있음)'
    반환: 길이 288 리스트(0/1)
    """
    bits: List[int] = []
    for n in range(9):  # slot1..slot9
        val = int(slots_9[n] or 0)
        for b in range(32):  # LSB -> MSB
            bits.append(1 if ((val >> b) & 1) else 0)
    # 혹시라도 288 초과/미만 방어
    if len(bits) < SLOTS_PER_DAY:
        bits.extend([0] * (SLOTS_PER_DAY - len(bits)))
    elif len(bits) > SLOTS_PER_DAY:
        bits = bits[:SLOTS_PER_DAY]
    return bits

def _normalize_bits(bits: Optional[List[int]]) -> List[int]:
    if not isinstance(bits, list):
        return [0] * SLOTS_PER_DAY
    n = len(bits)
    out = [1 if x else 0 for x in bits]
    if n < SLOTS_PER_DAY:
        out.extend([0] * (SLOTS_PER_DAY - n))
    elif n > SLOTS_PER_DAY:
        out = out[:SLOTS_PER_DAY]
    return out

def fetch_allweek_slots_for_users(db, user_ids: List[int]) -> Dict[int, List[List[int]]]:
    """
    사용자별로 [dow0_bits, ..., dow6_bits] 형태로 7일치 슬롯을 모두 반환.
    각 bits는 길이 288(5분 단위)의 0/1 리스트라고 가정.
    반환 형태: { user_id: [bits_d0, bits_d1, ..., bits_d6] }

    'fetch_slots_for_users(db, user_ids, dow)' 함수가 이미 존재한다고 가정한다.
    해당 함수의 반환은 { uid: bits(길이 288 리스트) } 형태여야 한다.
    """
    from services.timetable_service import fetch_slots_for_users  # 순환 임포트 방지용 내부 임포트

    result: Dict[int, List[List[int]]] = {int(uid): [None] * 7 for uid in user_ids}
    for d in range(7):
        day_map = fetch_slots_for_users(db, user_ids, d)  # {uid: bits(list or None)}
        for uid, bits in day_map.items():
            result[int(uid)][d] = _normalize_bits(bits)

    # None 구멍 채우기 (어떤 요일에도 데이터가 없을 수 있음)
    for uid, week in result.items():
        for d in range(7):
            if week[d] is None:
                week[d] = [0] * SLOTS_PER_DAY
            else:
                # 혹시라도 fetch가 잘못 준 경우를 다시 한 번 방어
                week[d] = _normalize_bits(week[d])
    return result

def meal_anchor_or_last_end_allweek(
    bits_by_dow: List[List[int]],
    *,
    ref_time: datetime,
    lookahead_min: int,
    need_min: int,
    empty_is: int = 0,
) -> Tuple[int, time] | Tuple[int, int]:
    """
    ref_time 기준 lookahead_min 내에 need_min 이상 연속 '빈 슬롯(empty_is)'이 '시작'되면,
    그 공강 시작 '직전'에 끝난 마지막 강의의 (요일:int, 종료시각:datetime.time)을 반환.
    없으면 (-1, -1).
    월=0 … 일=6 (datetime.weekday와 동일).
    """
    if not isinstance(bits_by_dow, list) or len(bits_by_dow) != 7:
        return (-1, -1)

    bits_by_dow = [_normalize_bits(day_bits) for day_bits in bits_by_dow]

    dow_today = ref_time.weekday()
    dow_next = (dow_today + 1) % 7
    today_bits = bits_by_dow[dow_today]
    next_bits = bits_by_dow[dow_next]

    # ref_time을 슬롯 인덱스로
    start_idx = (ref_time.hour * 60 + ref_time.minute) // SLOT_MIN
    if start_idx < 0:
        start_idx = 0
    if start_idx >= SLOTS_PER_DAY:
        start_idx = SLOTS_PER_DAY - 1  # 이론상 23:59 → 287

    H = max(1, lookahead_min // SLOT_MIN)        # 탐색할 최대 슬롯 수
    need = max(1, need_min // SLOT_MIN)          # 필요한 연속 빈 슬롯 수

    tail_today = today_bits[start_idx:]
    span = tail_today + next_bits

    # 공강 anchor 찾기
    run = 0
    anchor_span_idx = -1
    limit = min(H, len(span))
    for i in range(limit):
        if span[i] == empty_is:
            run += 1
        else:
            run = 0
        if run >= need:
            anchor_span_idx = i - need + 1
            break

    if anchor_span_idx < 0:
        return (-1, -1)

    if anchor_span_idx < len(tail_today):
        anchor_dow = dow_today
        anchor_idx = start_idx + anchor_span_idx
    else:
        anchor_dow = dow_next
        anchor_idx = anchor_span_idx - len(tail_today)

    def _bit_at(dow: int, idx: int) -> int:
        # 안전 접근: 범위를 벗어나면 0으로 처리
        if 0 <= idx < SLOTS_PER_DAY:
            return bits_by_dow[dow][idx]
        return 0

    def _next_pos(dow: int, idx: int) -> tuple[int, int]:
        if idx >= SLOTS_PER_DAY - 1:
            return ((dow + 1) % 7, 0)
        return (dow, idx + 1)

    # anchor 직전에서 역탐색: 1→0 경계를 찾으면 (j+1)*5분이 '종료 시각'
    cur_dow = anchor_dow
    cur_idx = anchor_idx - 1
    steps = 0
    MAX_STEPS = 7 * SLOTS_PER_DAY

    while steps < MAX_STEPS:
        if cur_idx < 0:
            cur_dow = (cur_dow - 1) % 7
            cur_idx = SLOTS_PER_DAY - 1
            steps += 1
            continue

        b = _bit_at(cur_dow, cur_idx)
        ndow, nidx = _next_pos(cur_dow, cur_idx)
        nb = _bit_at(ndow, nidx)

        if b == 1 and nb == 0:
            end_minutes = ((cur_idx + 1) % SLOTS_PER_DAY) * SLOT_MIN
            return (cur_dow, time(hour=end_minutes // 60, minute=end_minutes % 60))

        cur_idx -= 1
        steps += 1

    # 극단 케이스: 일주일 내 경계 없음 → 유효한 기본값
    return (cur_dow, time(0, 0))

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


def fetch_slots_for_users(db, user_ids: List[int], dow: int) -> Dict[int, List[int]]:
    """
    DB에서 특정 요일의 slot1..slot9를 가져와 288비트로 언팩해 반환.
    반환: { user_id: [0/1]*288 }  (1=수업중, 0=공강)
    """
    if not user_ids:
        return {}

    sql = text("""
        SELECT user_id, slot1, slot2, slot3, slot4, slot5, slot6, slot7, slot8, slot9
        FROM timetable_bit
        WHERE day_of_week = :dow AND user_id IN :uids
    """)

    rows = db.execute(sql, {"dow": dow, "uids": tuple(user_ids)}).fetchall()

    out: Dict[int, List[int]] = {}
    for row in rows:
        uid = int(row[0])
        slots_9 = [int(row[i]) for i in range(1, 10)]  # 1..9
        bits = _unpack_9x32_to_288(slots_9)
        out[uid] = bits
    return out

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
