from typing import List, Dict

def get_user_preferences(user_ids: List[int]) -> Dict[int, Dict[str, float]]:
    # TODO: 실제 백엔드로 교체
    # 기대:
    # {
    #     9001: {
    #         "korean": 0.4,
    #         "japanese": 0.3,
    #         "western": 0.2,
    #         "vegan": 0.1
    #     },
    #     9002: {
    #         "chicken": 0.5,
    #         "pizza": 0.3,
    #         "dessert": 0.2
    #     }
    # }

    return {uid: {} for uid in user_ids}

def get_upcoming_lecture_coords(user_ids: List[int]) -> Dict[int, Dict[str, float]]:
    # TODO: 실제 백엔드로 교체
    # 기대:
    # {
    #     9001: {"latitude": 37.123, "longitude": 127.456},
    #     9002: {"latitude": 37.129, "longitude": 127.459}
    # }

    return {uid: {} for uid in user_ids}
