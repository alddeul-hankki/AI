import pandas as pd

def normalize_user_id(df: pd.DataFrame) -> pd.DataFrame:
    """
    df의 사용자 식별 컬럼을 'user_id'로 정규화한다.
    허용 별칭: 'user_id', 'userId', 'uid', 'id', 'userID'
    """
    if "user_id" in df.columns:
        out = df.copy()
    else:
        alias = None
        for cand in ["userId", "uid", "id", "userID"]:
            if cand in df.columns:
                alias = cand
                break
        if alias is None:
            raise ValueError("candidates df must contain 'user_id' (or one of: userId, uid, id, userID)")
        out = df.rename(columns={alias: "user_id"}).copy()

    # 정수형으로 캐스팅(문자열/float로 들어오더라도 정규화)
    out["user_id"] = out["user_id"].astype(int)
    return out
    