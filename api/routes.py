from fastapi import APIRouter, HTTPException, Query
import redis
from core.config import settings

r = redis.Redis(
    host=settings.REDIS_HOST,
    port=settings.REDIS_PORT,
    password=settings.REDIS_PASSWORD,
    decode_responses=True,
)
router = APIRouter(prefix="/campuses", tags=["clusters"])

from pydantic import BaseModel
from fastapi import Body

from pydantic import BaseModel
from fastapi import Body

class ClusterRequest(BaseModel):
    groupId: int  # 일단 받기만 함 (미사용)
    userId: int
    topK: int = 5   # 기본값 5, 유효범위는 1~100으로 검증할 수도 있음

@router.post("/cluster-member/me")
def my_cluster_post(payload: ClusterRequest = Body(...)):
    campus_id = settings.CAMPUS_ID
    user_id = payload.userId
    _group_id = payload.groupId  # 나중에 사용 예정(현재 미사용)
    top_k = payload.topK

    if not (1 <= top_k <= 100):
        raise HTTPException(400, "topK must be between 1 and 100")

    # 1) 활성 run
    run_key = r.get(f"active:campus:{campus_id}")
    if not run_key or not run_key.startswith("run:"):
        raise HTTPException(404, "Active snapshot not found")
    run_id = run_key.split(":")[1]

    # 2) 내 클러스터
    cluster_seq = r.hget(f"cm:run:{run_id}", str(user_id))
    if cluster_seq is None:
        raise HTTPException(404, "User not assigned in this snapshot")

    # 3) 멤버 조회(ZSet/Set)
    cl_key = f"cl:run:{run_id}:cid:{cluster_seq}"
    if r.type(cl_key) == "zset":
        raw = r.zrange(cl_key, 0, -1, withscores=True)
        members = [int(uid) for uid, _ in raw]   # distance 무시
    else:
        members = [int(uid) for uid in r.smembers(cl_key)]

    # 정렬
    members.sort()

    # 본인 제외 + Top-K 적용
    members = [uid for uid in members if uid != user_id]
    if top_k:
        members = members[:top_k]

    return {
            "groupId": _group_id,
            "members": [{"userId": uid} for uid in members],
    }
