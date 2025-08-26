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

@router.get("/{campus_id}/clusters/me")
def my_cluster(
    campus_id: int,
    user_id: int,
    top_k: int | None = Query(default=5, ge=1, le=100)   # ← 기본 Top‑K=5
):
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
        members = [{"user_id": int(uid), "distance": score} for uid, score in raw]
        # 동점 안정화: distance, user_id 순
        members.sort(key=lambda m: (m["distance"], m["user_id"]))
    else:
        raw = [int(uid) for uid in r.smembers(cl_key)]
        raw.sort()
        members = [{"user_id": uid} for uid in raw]

    # 본인 제외 + Top‑K 적용
    members = [m for m in members if m["user_id"] != user_id]
    if top_k:
        members = members[:top_k]

    return {
        "campus_id": campus_id,
        "run_id": int(run_id),
        "cluster_seq": int(cluster_seq),
        "members": members,
    }
