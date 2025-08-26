from fastapi import FastAPI
from apscheduler.schedulers.background import BackgroundScheduler

from api.routes import router as clusters_router
from api.admin_routes import router as admin_router
from api.dirty_routes import router as dirty_router
from core.config import settings
from services.cluster_batch import run_full_cycle
from services.dirty_recompute import recompute_dirty_bits
from sqlalchemy import text
from core.db import SessionLocal

app = FastAPI(title="SOLMEAL API", version="0.1.0")

# 라우터
app.include_router(clusters_router)
app.include_router(admin_router)
app.include_router(dirty_router)

# 10분 오토사이클
sched = BackgroundScheduler(timezone="Asia/Seoul")

def _auto_cycle_tick():
    # 1) 더티 있으면 재계산
    with SessionLocal() as db:
        dirty = db.execute(text("SELECT COUNT(*) FROM timetable_bit WHERE is_dirty=1")).scalar_one()
    if dirty:
        recompute_dirty_bits()
    # 2) 스냅샷 사이클
    run_full_cycle(settings.CAMPUS_ID, algo="kmeans-v1", note="scheduler")

@app.on_event("startup")
def on_startup():
    sched.add_job(_auto_cycle_tick, "interval", minutes=settings.AUTOCYCLE_EVERY_MIN)
    sched.start()

@app.on_event("shutdown")
def on_shutdown():
    sched.shutdown(wait=False)

@app.get("/")
def root():
    return {"message": "SOLMEAL API is running 🚀"}
