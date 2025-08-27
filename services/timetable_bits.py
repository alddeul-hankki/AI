from datetime import time
from typing import List, Dict

SLOT_MIN = 5
SLOTS_PER_DAY = 24 * 60 // SLOT_MIN  # 288
DAYS = 7  # 0=Mon ... 6=Sun

class Row:
    """
    시간표 한 행
    - day_of_week: 0=Mon ... 6=Sun
    - start, end: datetime.time
    """
    def __init__(self, day_of_week: int, start: time, end: time):
        self.day_of_week = day_of_week
        self.start = start
        self.end = end


def to_slot_index(t: time) -> int:
    """LocalTime -> 5분 슬롯 index (floor)"""
    return (t.hour * 60 + t.minute) // SLOT_MIN


def floor_start(t: time) -> int:
    return to_slot_index(t)


def ceil_end(t: time) -> int:
    minutes = t.hour * 60 + t.minute
    return (minutes + SLOT_MIN - 1) // SLOT_MIN


def clamp(idx: int) -> int:
    return max(0, min(SLOTS_PER_DAY, idx))


def mark_busy(bits: List[int], start: time, end: time):
    """[start,end) 구간을 비트 1로 마킹"""
    if end <= start:
        return  # 0길이 or 역전 구간 무시

    s = clamp(floor_start(start))
    e = clamp(ceil_end(end))
    for i in range(s, e):
        bits[i] = 1


def build_bits_per_day(rows: List[Row]) -> List[List[int]]:
    """
    uid 하나의 시간표 -> 요일별 비트 리스트 반환
    - 반환: [ [월요일 비트 288개], ..., [일요일 비트 288개] ]
    """
    per_day = [[0] * SLOTS_PER_DAY for _ in range(DAYS)]
    for r in rows:
        if not (0 <= r.day_of_week <= 6):
            continue
        mark_busy(per_day[r.day_of_week], r.start, r.end)
    return per_day


def to_nine_ints(bits: List[int]) -> List[int]:
    """
    288비트 -> 9개 int 직렬화 (LSB=낮은 index 슬롯)
    """
    out = [0] * 9
    for i, b in enumerate(bits):
        if b == 1:
            block = i // 32
            offset = i % 32
            out[block] |= (1 << offset)
    return out


def from_nine_ints(nine: List[int]) -> List[int]:
    """
    9개 int -> 288비트 복원
    """
    if len(nine) != 9:
        raise ValueError("need exactly 9 ints")
    bits = [0] * SLOTS_PER_DAY
    for block, v in enumerate(nine):
        for offset in range(32):
            if (v >> offset) & 1:
                i = block * 32 + offset
                if i < SLOTS_PER_DAY:
                    bits[i] = 1
    return bits