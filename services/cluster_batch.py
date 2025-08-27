from typing import Optional
import pandas as pd
from sqlalchemy import text
from sqlalchemy.orm import Session
from core.db import SessionLocal
from services.snapshot_service import create_draft_run, warmup_to_redis, activate_run, fetch_cluster_rows
from services.cluster_job import ClusterParams, run_clustering, to_cluster_member_rows, compute_k
from datetime import datetime
from services.timetable_service import _today_dow_kst, fetch_slots_for_users, has_meal_window_twoday, anchor_to_10min_kst

def fetch_candidates(db: Session, campus_id: int) -> pd.DataFrame:
    # TODO: 이 부분은 실제 데이터로 교체 권장 (slots 9개 int 생성)
    def bits_to_slots(bitstr: str):
        assert len(bitstr) == 288
        slots = []
        for blk in range(9):
            val = 0
            for i in range(32):
                b = 1 if bitstr[blk*32 + i] == '1' else 0
                val |= (b << i)
            slots.append(val)
        return slots

    data = [
        {"user_id": 9001, "latitude": 37.50, "longitude": 127.00, "korean":0.5, "pizza":0.2, "chicken":0.3}, #1
        {"user_id": 9002, "latitude": 37.51, "longitude": 127.01, "korean":1, "pizza":0, "chicken":0}, #2
        {"user_id": 9003, "latitude": 37.49, "longitude": 126.99, "korean":0, "pizza":1, "chicken":0}, #3
        {"user_id": 9004, "latitude": 37.52, "longitude": 127.02, "korean":0, "pizza":1, "chicken":0}, #4
        {"user_id": 9005, "latitude": 37.505,"longitude":127.005, "korean":1,"pizza":0, "chicken":0}, #5
        {"user_id": 9006, "latitude": 37.52, "longitude": 127.02, "korean":1, "pizza":0, "chicken":0}, #6
        {"user_id": 9007, "latitude": 37.51, "longitude": 127.01, "korean":0.6, "pizza":0, "chicken":0.4}, #7
        {"user_id": 9008, "latitude": 37.49, "longitude": 126.99, "korean":0, "pizza":0.3, "chicken":0.7}, #8
        {"user_id": 9009, "latitude": 37.50, "longitude": 127.00, "korean":0, "pizza":0.3, "chicken":0.7}, #9
        {"user_id": 9010, "latitude": 37.505,"longitude":127.005, "korean":0.9,"pizza":0.1, "chicken":0}, #10
    ]
    return pd.DataFrame(data)

def bulk_insert_cluster_member(db: Session, rows):
    db.execute(text("""
        INSERT INTO cluster_member (run_id, cluster_seq, user_id, rank_in_cluster, distance_to_center)
        VALUES (:run_id, :cluster_seq, :user_id, :rank_in_cluster, :distance_to_center)
    """), rows)
    db.commit()

def run_full_cycle(campus_id: int, algo: str = "kmeans-v1", note: Optional[str] = None):
    db = SessionLocal()
    try:
        # 1) 후보 로드
        df = fetch_candidates(db, campus_id)
        user_ids = df["user_id"].astype(int).tolist()

        # ✨ 앵커 시간: '정각 기준 10분'으로
        ref_time = anchor_to_10min_kst()

        dow_today = ref_time.weekday()               # 앵커 기준 요일
        dow_next  = (dow_today + 1) % 7

        slots_today = fetch_slots_for_users(db, user_ids, dow_today)
        slots_next  = fetch_slots_for_users(db, user_ids, dow_next)

        def _ok(uid: int, s_today: list[int] | None) -> bool:
            if not s_today:
                return False
            s_next = slots_next.get(uid)
            return has_meal_window_twoday(
                s_today, s_next,
                lookahead_min=120,
                need_min=30,
                empty_is=0,                 # ← 바쁨=1, 공강=0 환경이면 0으로 설정
                ref_time=ref_time           # ✅ 'now' 대신 앵커 시간 사용
            )

        df = df[df["user_id"].astype(int).apply(lambda uid: _ok(uid, slots_today.get(uid)))]
        if df.empty:
            raise RuntimeError("no candidates after meal-window filter")

        # 2) 파라미터 기록
        params = ClusterParams(min_group_size=3, w_time=1.0, w_loc=0.5, w_cat=1.5, downsample=6)
        param_json = {
            "note": note,
            "min_group_size": params.min_group_size,
            "w_time": params.w_time,
            "w_loc": params.w_loc,
            "w_cat": params.w_cat,
            "downsample": params.downsample,
            "cycle_anchor": ref_time.isoformat()
        }

        run_id = create_draft_run(db, campus_id, algo, param_json)

        # 3.5) k 계산·기록·전달
        n = len(df)
        k = compute_k(n, params.min_group_size, k_min=2)
        db.execute(text("""
          UPDATE run
          SET param_json = JSON_SET(param_json, '$.computed_k', :k)
          WHERE run_id = :rid
        """), {"k": int(k), "rid": run_id})
        db.commit()

        params.force_k = int(k)

        # 4) 클러스터링
        labels, dists, _X = run_clustering(df, params)
        rows = to_cluster_member_rows(run_id, df, labels, dists)

        # 5) 적재
        bulk_insert_cluster_member(db, rows)

        # 6) Redis 워밍업 + 활성화
        warmup_to_redis(run_id, fetch_cluster_rows(db, run_id))
        activate_run(db, campus_id, run_id)

        return run_id
    finally:
        db.close()