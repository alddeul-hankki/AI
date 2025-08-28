# services/backend_client.py
from typing import List, Dict, Any, DefaultDict
from collections import defaultdict
import requests
from core.config import settings  # settings.BACKEND_API_BASE 사용
from services.timetable_bits import SLOTS_PER_DAY, SLOT_MIN
import math

def _hhmmss_to_min(hhmmss: str) -> int:
    # "09:00:00" -> 540
    h, m, s = hhmmss.split(":")
    return int(h) * 60 + int(m) + (int(s) // 60)

def _clamp_minute(m: int) -> int:
    return max(0, min(SLOTS_PER_DAY * SLOT_MIN, m))

def _merge_intervals(intervals: List[Dict[str, int]]) -> List[Dict[str, int]]:
    """
    같은 요일 내에서 겹치거나 인접(예: 10:00~10:30, 10:30~11:00)한 구간을 병합하여
    비트 채우기 부담을 줄인다.
    """
    if not intervals:
        return []
    ints = sorted(intervals, key=lambda x: x["start_min"])
    merged = [ints[0]]
    for cur in ints[1:]:
        prev = merged[-1]
        if cur["start_min"] <= prev["end_min"]:  # 겹침 또는 인접(끝==시작) 허용하려면 < 대신 <=
            prev["end_min"] = max(prev["end_min"], cur["end_min"])
        else:
            merged.append(cur)
    return merged

def get_intervals_bulk(user_ids: List[int]) -> Dict[int, Dict[int, list]]:
    """
    POST /api/timetable/users
    Body: [1,2,3]
    Response: { success, message, timetables: [ { userId, lectures:[{dayOfWeek, startTime, endTime}, ...] }, ... ] }

    반환 형식: { uid: { dow: [ {start_min, end_min}, ... ] } }
    """
    if not user_ids:
        return {}

    url = f"{settings.BACKEND_API_BASE}/api/timetable/users"
    headers = {"Accept": "application/json"}
    if getattr(settings, "BACKEND_API_KEY", ""):
        headers["Authorization"] = f"Bearer {settings.BACKEND_API_KEY}"

    resp = requests.post(url, json=user_ids, headers=headers, timeout=getattr(settings, "BACKEND_TIMEOUT", 5))
    resp.raise_for_status()
    data: Dict[str, Any] = resp.json() or {}
    timetables = data.get("timetables") or []

    # uid → dow → intervals
    buckets: DefaultDict[int, DefaultDict[int, list]] = defaultdict(lambda: defaultdict(list))

    for item in timetables:
        uid = item.get("userId")
        lectures = item.get("lectures") or []
        if uid is None:
            continue

        per_dow: DefaultDict[int, list] = defaultdict(list)
        for lec in lectures:
            dow = lec.get("dayOfWeek")
            st = lec.get("startTime")
            et = lec.get("endTime")
            if dow is None or not st or not et:
                continue

            s_min = _clamp_minute(_hhmmss_to_min(st))
            e_min = _clamp_minute(_hhmmss_to_min(et))
            if e_min <= s_min:
                continue

            per_dow[dow].append({"start_min": s_min, "end_min": e_min})

        # 병합해서 넣기
        for dow, ivs in per_dow.items():
            buckets[uid][dow] = _merge_intervals(ivs)

    # dict로 변환
    out: Dict[int, Dict[int, list]] = {}
    for uid, per_dow in buckets.items():
        out[uid] = dict(per_dow)
    return out
