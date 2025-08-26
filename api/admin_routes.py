from fastapi import APIRouter, HTTPException
from sqlalchemy.orm import Session
from core.db import SessionLocal
from services.snapshot_service import create_draft_run, fetch_cluster_rows, warmup_to_redis, activate_run, run_stats
from services.cluster_batch import run_full_cycle
from sqlalchemy import text
from core.db import SessionLocal
from services.dirty_recompute import recompute_dirty_bits

router = APIRouter(prefix="/admin", tags=["admin"])

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@router.post("/campuses/{campus_id}/runs")
def create_run(campus_id: int, algo: str = "baseline-v0", note: str | None = None):
    db: Session = next(get_db())
    try:
        rid = create_draft_run(db, campus_id, algo, {"note": note} if note else None)
        return {"campus_id": campus_id, "run_id": rid, "status": "draft", "algo": algo}
    except Exception as e:
        raise HTTPException(500, f"create draft failed: {e}")

@router.post("/runs/{run_id}/warmup")
def warmup(run_id: int):
    db: Session = next(get_db())
    try:
        warmup_to_redis(run_id, fetch_cluster_rows(db, run_id))
        return {"run_id": run_id, "redis": "warmed"}
    except Exception as e:
        raise HTTPException(500, f"warmup failed: {e}")

@router.post("/campuses/{campus_id}/activate/{run_id}")
def activate(campus_id: int, run_id: int):
    db: Session = next(get_db())
    try:
        activate_run(db, campus_id, run_id)
        return {"campus_id": campus_id, "active_run_id": run_id, "status": "active"}
    except Exception as e:
        raise HTTPException(500, f"activate failed: {e}")

@router.get("/runs/{run_id}/stats")
def stats(run_id: int):
    db: Session = next(get_db())
    try:
        return run_stats(db, run_id)
    except Exception as e:
        raise HTTPException(500, f"stats failed: {e}")

@router.post("/campuses/{campus_id}/autocycle")
def autocycle(campus_id: int, note: str | None = None):
    # 0) dirty 남아 있으면 재계산
    with SessionLocal() as db:
        dirty = db.execute(text("SELECT COUNT(*) FROM timetable_bit WHERE is_dirty=1")).scalar_one()
    if dirty:
        recompute_dirty_bits()

    # 1) 기존 풀사이클 실행
    try:
        rid = run_full_cycle(campus_id, algo="kmeans-v1", note=note)
        return {
            "campus_id": campus_id,
            "active_run_id": rid,
            "status": "active"
        }
    except Exception as e:
        raise HTTPException(500, f"autocycle failed: {e}")