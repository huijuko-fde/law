# syntax=docker/dockerfile:1
# law_indexer — data JSON 을 읽어 Weaviate 에 임베딩 적재하는 배치(잡).
FROM ghcr.io/astral-sh/uv:python3.11-bookworm-slim

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never \
    HF_HOME=/cache/hf \
    PYTHONUNBUFFERED=1

WORKDIR /app

# 1) 의존성만 먼저 설치 (코드 안 바꾸면 이 레이어 캐시 재사용)
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

# 2) 프로젝트 코드
COPY . .
RUN uv sync --frozen --no-dev

# arctic 임베딩 모델(~1.2GB)은 런타임에 /cache/hf 볼륨으로 캐시 (이미지에 안 굽는다)
# Weaviate 접속·INPUT_DATA_PATH 등은 compose 의 env 로 주입.
ENTRYPOINT ["uv", "run", "--no-dev", "python", "-m", "law_indexer"]
CMD ["health"]
