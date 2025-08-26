from __future__ import annotations
from dataclasses import dataclass
from typing import List, Optional, Tuple
import numpy as np
import pandas as pd
from sklearn.cluster import MiniBatchKMeans
from sklearn.preprocessing import StandardScaler

# slots: 9개 int (각 32비트) → 288비트 → 48차원(기본)으로 다운샘플
def slots_to_vec(slots: List[int], downsample: int = 6) -> np.ndarray:
    bits = []
    for val in slots:
        arr = [(val >> i) & 1 for i in range(32)]
        bits.extend(arr)
    x = np.array(bits, dtype=np.float32)
    if len(x) != 288:
        raise ValueError(f"slots length invalid: {len(x)}")
    return x.reshape(-1, downsample).mean(axis=1).astype(np.float32)

@dataclass
class ClusterParams:
    min_group_size: int = 3
    max_clusters: Optional[int] = None
    random_state: int = 42
    w_time: float = 1.0
    w_loc: float = 1.0
    w_cat: float = 1.0
    downsample: int = 6

def build_feature_matrix(df: pd.DataFrame, params: ClusterParams) -> Tuple[np.ndarray, List[int]]:
    user_ids = df["user_id"].astype(int).tolist()

    # 시간 특징
    time_feats = np.vstack([slots_to_vec(s, params.downsample) for s in df["slots"]])  # (N, 48)
    # 위치
    loc_feats = df[["lat", "lng"]].to_numpy(dtype=np.float32)                           # (N, 2)
    # 카테고리(있으면)
    cat_cols = [c for c in df.columns if c.startswith("cat_")]
    cat_feats = df[cat_cols].to_numpy(dtype=np.float32) if cat_cols else None

    # 스케일 + 가중치
    scaler_t = StandardScaler()
    scaler_l = StandardScaler()
    t = scaler_t.fit_transform(time_feats) * params.w_time
    l = scaler_l.fit_transform(loc_feats)  * params.w_loc
    feats = [t, l]
    if cat_feats is not None:
        scaler_c = StandardScaler()
        c = scaler_c.fit_transform(cat_feats) * params.w_cat
        feats.append(c)

    X = np.hstack(feats).astype(np.float32)
    return X, user_ids

def choose_k(n: int, params: ClusterParams) -> int:
    k = max(1, round(n / max(1, params.min_group_size)))
    if params.max_clusters:
        k = min(k, params.max_clusters)
    return min(k, n)

def run_clustering(df: pd.DataFrame, params: ClusterParams) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    """
    반환:
      labels: (N,) 최종 클러스터(1부터 시작)
      dists:  (N,) 최종 중심거리
      X:      (N,D) 표준화 특징 (사후 재배정용)
    """
    X, _ = build_feature_matrix(df, params)
    n = len(df)
    k = choose_k(n, params)

    kmeans = MiniBatchKMeans(n_clusters=k, random_state=params.random_state, batch_size=1024, n_init='auto')
    raw_labels = kmeans.fit_predict(X)     # 0..k-1
    centers = kmeans.cluster_centers_
    dists = np.linalg.norm(X - centers[raw_labels], axis=1)

    # ── 최소 군집 크기 미만 라벨 재배정 ──
    labels = raw_labels.copy()
    # 라벨별 인덱스
    from collections import defaultdict
    groups = defaultdict(list)
    for i, lab in enumerate(labels):
        groups[int(lab)].append(i)

    # 유지 라벨(충분히 큰 군집), 작은 군집 라벨 구분
    big_labels = {lab for lab, idxs in groups.items() if len(idxs) >= params.min_group_size}
    small_labels = {lab for lab, idxs in groups.items() if len(idxs) <  params.min_group_size}

    if small_labels and big_labels:
        big_centers = np.stack([centers[lab] for lab in sorted(big_labels)])  # (B,D)
        big_list = sorted(big_labels)
        for lab in small_labels:
            idxs = groups[lab]
            # 각 포인트를 가장 가까운 big center로 재배정
            subX = X[idxs]                                                # (m,D)
            # (m,B) 거리
            dd = np.linalg.norm(subX[:, None, :] - big_centers[None, :, :], axis=2)
            nearest_big = dd.argmin(axis=1)
            mapped_label = [big_list[j] for j in nearest_big]
            for p, new_lab in zip(idxs, mapped_label):
                labels[p] = new_lab
                # 거리 갱신
                dists[p] = float(np.linalg.norm(X[p] - centers[new_lab]))

    # 1부터 시작하도록 +1 (API/DB 일관성)
    labels = labels + 1
    return labels, dists, X

def to_cluster_member_rows(run_id: int, df: pd.DataFrame, labels: np.ndarray, dists: np.ndarray):
    tmp = df[["user_id"]].copy()
    tmp["label"] = labels
    tmp["dist"] = dists

    rows = []
    for label, grp in tmp.groupby("label"):
        # 안정 정렬: dist, user_id
        grp_sorted = grp.sort_values(by=["dist", "user_id"], ascending=[True, True]).reset_index(drop=True)
        for idx, row in grp_sorted.iterrows():
            rows.append({
                "run_id": run_id,
                "cluster_seq": int(label),
                "user_id": int(row["user_id"]),
                "rank_in_cluster": int(idx) + 1,
                "distance_to_center": float(row["dist"])
            })
    return rows
