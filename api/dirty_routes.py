from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import List
from sqlalchemy import text
from core.db import SessionLocal

router = APIRouter(tags=["timetable-bit"])

class DirtyRequest(BaseModel):
    user_id: int

class DirtyBulkRequest(BaseModel):
    user_ids: List[int]

@router.post("/dirty")
def mark_dirty(req: DirtyRequest):
    with SessionLocal() as db:
        vals = ",".join(f"({req.user_id},{d},1)" for d in range(7))
        db.execute(text(f"""
          INSERT INTO timetable_bit (user_id, day_of_week, is_dirty)
          VALUES {vals}
          ON DUPLICATE KEY UPDATE is_dirty=1, updated_at=CURRENT_TIMESTAMP
        """))
        db.commit()
    return {"ok": True, "user_id": req.user_id, "days": list(range(7))}

@router.post("/dirty/bulk")
def mark_dirty_bulk(req: DirtyBulkRequest):
    if not req.user_ids:
        return {"ok": True, "user_ids": []}
    with SessionLocal() as db:
        vals = ",".join(f"({uid},{d},1)" for uid in req.user_ids for d in range(7))
        db.execute(text(f"""
          INSERT INTO timetable_bit (user_id, day_of_week, is_dirty)
          VALUES {vals}
          ON DUPLICATE KEY UPDATE is_dirty=1, updated_at=CURRENT_TIMESTAMP
        """))
        db.commit()
    return {"ok": True, "user_ids": req.user_ids, "days": list(range(7))}

@router.get("/bits/{user_id}/{day_of_week}")
def get_bits(user_id: int, day_of_week: int):
    with SessionLocal() as db:
        row = db.execute(text("""
            SELECT slot1,slot2,slot3,slot4,slot5,slot6,slot7,slot8,slot9,is_dirty
            FROM timetable_bit WHERE user_id=:u AND day_of_week=:d
        """), {"u": user_id, "d": day_of_week}).first()
    if not row:
        raise HTTPException(404, "not found")
    return {"user_id": user_id, "day_of_week": day_of_week,
            "slots": list(row[:9]), "is_dirty": row[9]}
