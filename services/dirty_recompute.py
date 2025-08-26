from sqlalchemy import text
from core.db import SessionLocal
from services.backend_stub import get_intervals_bulk
from services.bits_service import intervals_to_nine_ints

def recompute_dirty_bits():
    with SessionLocal() as db:
        users = [r[0] for r in db.execute(
            text("SELECT DISTINCT user_id FROM timetable_bit WHERE is_dirty=1")
        ).all()]
        if not users:
            return
        all_iv = get_intervals_bulk(users)   # {uid:{dow:[...]}} (지금은 {})
        for uid in users:
            per_day = all_iv.get(str(uid)) or all_iv.get(uid) or {}
            for dow in range(7):
                iv = per_day.get(str(dow)) or per_day.get(dow) or []
                nine = intervals_to_nine_ints(iv) if iv else [0]*9
                db.execute(text("""
                  INSERT INTO timetable_bit
                    (user_id, day_of_week, slot1,slot2,slot3,slot4,slot5,slot6,slot7,slot8,slot9, is_dirty)
                  VALUES (:u,:d,:s1,:s2,:s3,:s4,:s5,:s6,:s7,:s8,:s9,0)
                  ON DUPLICATE KEY UPDATE
                    slot1=:s1,slot2=:s2,slot3=:s3,slot4=:s4,slot5=:s5,
                    slot6=:s6,slot7=:s7,slot8=:s8,slot9=:s9,
                    is_dirty=0, updated_at=CURRENT_TIMESTAMP
                """), {"u": uid, "d": dow,
                       "s1": nine[0], "s2": nine[1], "s3": nine[2], "s4": nine[3], "s5": nine[4],
                       "s6": nine[5], "s7": nine[6], "s8": nine[7], "s9": nine[8]})
        db.commit()
