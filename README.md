# Solmeal FastAPI Server

이 레포지토리는 Solmeal FastAPI 서버 애플리케이션입니다.  
로컬 환경에서 실행하기 위한 설치 및 실행 방법은 아래 단계를 따라주세요.

---

## 0) 필수 설치물

- Python 3.10+ (또는 프로젝트에 맞는 버전)
- Git
- Docker, Docker Compose
- (선택) conda 또는 mamba

---

## 1) 레포 클론 & 디렉토리 진입

```bash
git clone <YOUR_REPO_URL>.git
cd <REPO_ROOT>
```

---

## 2) 환경변수(.env) 준비

레포에 `.env.example` 파일이 있다면 복사 후 값 채우기:

```bash
cp .env.example .env
```

필수 항목:
```
DATABASE_URL=mysql+pymysql://USER:PASS@HOST:PORT/solmeal
BACKEND_API_BASE=http://localhost:8080  # http:// 또는 https:// 반드시 포함
```

---

## 3-A) conda 환경 사용 시

```bash
conda create -n solmeal-fastapi python=3.10 -y
conda activate solmeal-fastapi
pip install --upgrade pip
pip install -r requirements.txt
```

---

## 3-B) venv 환경 사용 시

```bash
python -m venv .venv
# Windows
. .venv/Scripts/activate
# macOS/Linux
source .venv/bin/activate

pip install --upgrade pip
pip install -r requirements.txt
```

---

## 4) 인프라(docker) 실행

```bash
cd infra
docker compose up -d
cd ..
```

---

## 5) 앱 실행 (FastAPI)

```bash
uvicorn main:app --reload --host 0.0.0.0 --port 8081
```

- 관리 페이지: [http://localhost:8081/admin/...](http://localhost:8081/admin/...)
- OpenAPI 문서: [http://localhost:8081/docs](http://localhost:8081/docs)

---

## 6) 동작 확인

```bash
curl -X POST http://localhost:8081/admin/campuses/1001/autocycle
```

---

## 7) 백엔드 연동 값 점검

`BACKEND_API_BASE` 환경변수가 올바른지 확인하세요.

예:
```
BACKEND_API_BASE=http://192.168.0.10:8080
```

> 반드시 `http://` 또는 `https://` 스킴이 포함되어야 합니다.

---
