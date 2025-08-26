from typing import Optional, Iterable, Tuple
from sqlalchemy import text
from sqlalchemy.orm import Session
import redis
import json
from core.config import settings
from core.db import SessionLocal

r = redis.Redis(
    host=settings.REDIS_HOST,
    port=settings.REDIS_PORT,
    password=settings.REDIS_PASSWORD,
    decode_responses=True,
)

def create_draft_run(db: Session, campus_id: int, algo: str, param_json: Optional[dict]) -> int:
    if param_json is None:
        # 빈 오브젝트로 저장
        db.execute(text("""
            INSERT INTO run (campus_id, algo, param_json)
            VALUES (:campus_id, :algo, JSON_OBJECT())
        """), {"campus_id": campus_id, "algo": algo})
    else:
        # 꼭 json.dumps()로 더블쿼트 JSON 생성 후 CAST
        dumped = json.dumps(param_json, ensure_ascii=False)
        db.execute(text("""
            INSERT INTO run (campus_id, algo, param_json)
            VALUES (:campus_id, :algo, CAST(:param_json AS JSON))
        """), {"campus_id": campus_id, "algo": algo, "param_json": dumped})

    rid = db.execute(text("SELECT LAST_INSERT_ID()")).scalar_one()
    db.commit()
    return int(rid)

def fetch_cluster_rows(db: Session, run_id: int, batch_size: int = 5000) -> Iterable[Tuple[int,int,Optional[int],Optional[float]]]:
    """
    yield (user_id, cluster_seq, rank_in_cluster, distance_to_center)
    """
    offset = 0
    while True:
        rows = db.execute(text("""
            SELECT user_id, cluster_seq, rank_in_cluster, distance_to_center
            FROM cluster_member
            WHERE run_id = :run_id
            ORDER BY id
            LIMIT :limit OFFSET :offset
        """), {"run_id": run_id, "limit": batch_size, "offset": offset}).all()
        if not rows:
            break
        for (uid, cseq, rank, dist) in rows:
            yield int(uid), int(cseq), (int(rank) if rank is not None else None), (float(dist) if dist is not None else None)
        offset += batch_size

def warmup_to_redis(run_id: int, rows: Iterable[Tuple[int,int,Optional[int],Optional[float]]]) -> None:
    """
    cm:run:{rid}  (Hash) user_id -> cluster_seq
    cl:run:{rid}:cid:{cluster_seq} (ZSet or Set)
    """
    pipe = r.pipeline(transaction=True)
    cm_key = f"cm:run:{run_id}"

    # 성능을 위해 일정 개수마다 EXEC
    BULK = 2000
    count = 0
    for uid, cseq, _rank, dist in rows:
        pipe.hset(cm_key, str(uid), int(cseq))
        cl_key = f"cl:run:{run_id}:cid:{cseq}"
        if dist is not None:
            pipe.zadd(cl_key, {str(uid): float(dist)})
        else:
            pipe.sadd(cl_key, str(uid))
        count += 1
        if count % BULK == 0:
            pipe.execute()
    if count % BULK != 0:
        pipe.execute()

def activate_run(db: Session, campus_id: int, run_id: int) -> None:
    # 별도 begin 컨텍스트 없이 수행 → 마지막에 commit
    status = db.execute(
        text("SELECT status FROM run WHERE run_id = :rid FOR UPDATE"),
        {"rid": run_id}
    ).scalar_one()

    if status not in ("draft", "active"):
        raise ValueError(f"invalid status for activation: {status}")

    db.execute(
        text("UPDATE run SET status='active', activated_at=NOW() WHERE run_id=:rid"),
        {"rid": run_id}
    )
    db.execute(
        text("""
        INSERT INTO campus_latest (campus_id, active_run_id)
        VALUES (:cid, :rid)
        ON DUPLICATE KEY UPDATE
          active_run_id=VALUES(active_run_id),
          updated_at=CURRENT_TIMESTAMP
        """),
        {"cid": campus_id, "rid": run_id}
    )
    db.commit()  # ← 여기서 커밋

    # DB 커밋 후 Redis 스위치
    r.set(f"active:campus:{campus_id}", f"run:{run_id}")
    
def run_stats(db: Session, run_id: int) -> dict:
    total = db.execute(text("SELECT COUNT(*) FROM cluster_member WHERE run_id=:rid"), {"rid": run_id}).scalar_one()
    clusters = db.execute(text("""
        SELECT cluster_seq, COUNT(*) AS members
        FROM cluster_member
        WHERE run_id=:rid
        GROUP BY cluster_seq
        ORDER BY cluster_seq
    """), {"rid": run_id}).all()
    return {
        "run_id": run_id,
        "total_members": int(total),
        "clusters": [{"cluster_seq": int(c), "members": int(m)} for (c, m) in clusters]
    }
