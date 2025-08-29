from typing import Optional
import pandas as pd
from sqlalchemy import text
from sqlalchemy.orm import Session
from core.db import SessionLocal
from services.backend_client import post_users_locations
from services.data_util import normalize_user_id
from services.snapshot_service import create_draft_run, warmup_to_redis, activate_run, fetch_cluster_rows
from services.cluster_job import ClusterParams, run_clustering, to_cluster_member_rows, compute_k
from services.timetable_service import anchor_to_10min_kst
from typing import List, Dict

def fetch_candidates() -> pd.DataFrame:
    data = [
        {"user_id": 1, "korean":0.5, "pizza":0.2, "chicken":0.3}, #1
        {"user_id": 22, "korean":0.6, "pizza":0, "chicken":0.4}, #2
        {"user_id": 23, "korean":0, "pizza":1, "chicken":0}, #3
        {"user_id": 24, "korean":0, "pizza":1, "chicken":0}, #4
        {"user_id": 25, "korean":1,"pizza":0, "chicken":0}, #5
        {"user_id": 26, "korean":1, "pizza":0, "chicken":0}, #6
        {"user_id": 27, "korean":1, "pizza":0, "chicken":0}, #7
        {"user_id": 28, "korean":0.9,"pizza":0.1, "chicken":0}, #8
        {"user_id": 29, "korean":0, "pizza":0.3, "chicken":0.7}, #9
        {"user_id": 30, "korean":0, "pizza":0.3, "chicken":0.7}, #10
    ]
    return pd.DataFrame(data)

def bulk_insert_cluster_member(db: Session, rows):
    db.execute(text("""
        INSERT INTO cluster_member (run_id, cluster_seq, user_id, rank_in_cluster, distance_to_center)
        VALUES (:run_id, :cluster_seq, :user_id, :rank_in_cluster, :distance_to_center)
    """), rows)
    db.commit()

def enrich_df_with_locations(df_candidates: pd.DataFrame, locations: List[Dict]) -> pd.DataFrame:
    """
    locations를 df에 붙여 (user_id, longitude, latitude, 기존 선호도 feature들) 형태로 만든다.
    """
    if df_candidates.empty or not locations:
        # 빈 결과를 안전하게 반환
        cols = list(df_candidates.columns)
        if "longitude" not in cols: cols.append("longitude")
        if "latitude" not in cols: cols.append("latitude")
        return pd.DataFrame(columns=cols)

    df_candidates = normalize_user_id(df_candidates)  # ← 보장
    loc_df = pd.DataFrame(locations).rename(columns={"userId": "user_id"})
    # 혹시 응답이 숫자 문자열이면 안전 캐스팅
    loc_df["user_id"] = loc_df["user_id"].astype(int)

    merged = df_candidates.merge(loc_df, on="user_id", how="inner")
    return merged

def run_full_cycle(campus_id: int, algo: str = "kmeans-v1", note: Optional[str] = None):
    db = SessionLocal()
    try:
        # 1) 후보 로드
        df = fetch_candidates()
        df = normalize_user_id(df)

        # ✨ 앵커 시간: '정각 기준 10분'으로
        ref_time = anchor_to_10min_kst()

        locations = post_users_locations(db, df, ref_time)

        # ⑤ 위치를 df에 붙여: (user_id, longitude, latitude, 선호도 feature들)
        df = enrich_df_with_locations(df, locations)

        if df.empty:
            raise RuntimeError("no candidates after location merge")

        # 2) 파라미터 기록
        params = ClusterParams(min_group_size=3, w_time=1.0, w_loc=0.5, w_pref=1.5, downsample=6)
        param_json = {
            "note": note,
            "min_group_size": params.min_group_size,
            "w_time": params.w_time,
            "w_loc": params.w_loc,
            "w_pref": params.w_pref,
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