from typing import List, Dict

def get_intervals_bulk(user_ids: List[int]) -> Dict[int, Dict[int, list]]:
    # TODO: 실제 백엔드로 교체
    # 기대: {"9001":{"0":[{"start_min":540,"end_min":630}], ...}, ...}
    return {}

def get_user_preferences(user_ids: List[int]) -> Dict[int, Dict[str, float]]:
    return {uid: {} for uid in user_ids}

def get_upcoming_lecture_coords(user_ids: List[int]) -> Dict[int, Dict[str, float]]:
    return {uid: {} for uid in user_ids}
