# shinhan/core/config.py
import os
from dotenv import load_dotenv
from pydantic import BaseModel, ValidationError

# .env 로드 (운영에선 실제 환경변수로 주입)
load_dotenv()

def _require_str(key: str) -> str:
    v = os.getenv(key)
    if v is None or v.strip() == "":
        raise RuntimeError(f"Missing required environment variable: {key}")
    return v

def _require_int(key: str) -> int:
    v = _require_str(key)
    try:
        return int(v)
    except ValueError:
        raise RuntimeError(f"Invalid integer for {key}: {v}")

class Settings(BaseModel):
    # MySQL
    MYSQL_HOST: str
    MYSQL_PORT: int
    MYSQL_DATABASE: str
    MYSQL_USER: str
    MYSQL_PASSWORD: str

    # Redis
    REDIS_HOST: str
    REDIS_PORT: int
    REDIS_PASSWORD: str

    # 기타
    CAMPUS_ID: int

# ⚠️ 기존 변수명/사용 패턴(settings.MYSQL_HOST 등) 유지
settings = Settings(
    # MySQL (모두 필수)
    MYSQL_HOST=_require_str("MYSQL_HOST"),
    MYSQL_PORT=_require_int("MYSQL_PORT"),
    MYSQL_DATABASE=_require_str("MYSQL_DATABASE"),
    MYSQL_USER=_require_str("MYSQL_USER"),
    MYSQL_PASSWORD=_require_str("MYSQL_PASSWORD"),

    # Redis (호스트/포트 필수)
    REDIS_HOST=_require_str("REDIS_HOST"),
    REDIS_PORT=_require_int("REDIS_PORT"),
    REDIS_PASSWORD=_require_str("REDIS_PASSWORD"),

    # 기타 (운영 로직상 필수)
    CAMPUS_ID=_require_int("CAMPUS_ID"),
)
