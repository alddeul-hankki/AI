from typing import List, Dict
from services.timetable_bits import SLOTS_PER_DAY, SLOT_MIN, to_nine_ints

def intervals_to_nine_ints(intervals: List[Dict[str, int]]) -> List[int]:
    bits = [0] * SLOTS_PER_DAY
    for iv in intervals:
        s = max(0, min(SLOTS_PER_DAY, iv["start_min"] // SLOT_MIN))
        e = max(0, min(SLOTS_PER_DAY, (iv["end_min"] + SLOT_MIN - 1) // SLOT_MIN))
        if e <= s:
            continue
        for i in range(s, e):
            bits[i] = 1
    return to_nine_ints(bits)
