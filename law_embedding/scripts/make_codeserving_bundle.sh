#!/usr/bin/env bash
# 코드서빙 800 배포 번들을 만든다 — main.py(shim) + src/law_indexer + lib/(의존성 wheel 해제본).
#
#   scripts/make_codeserving_bundle.sh <out_dir>        예: scripts/make_codeserving_bundle.sh ../../_bundle_cs800
#
# 왜 이렇게 하나
#   · 코드서빙은 repo 루트 main.py 를 `uvicorn main:app` 으로 띄운다(START_COMMAND 불필요). 베이스 venv 에
#     fastapi·uvicorn·python-multipart·httpx·pydantic·python-dotenv·requests·numpy·pillow 가 있다.
#   · 거기 없는 weaviate-client(와 grpcio 등 그 의존)는 사내 PyPI 미러에 없거나 버전이 안 맞아 pip 경로를
#     타지 않는다 — 여기서 cp312/manylinux wheel 을 받아 lib/ 에 풀어 같이 올린다(requirements.txt 없음).
#     init.sh 는 pip 실패를 WARNING 으로 삼키고 다음 부팅에도 재시도하지 않으므로 pip 를 아예 안 타는 쪽이 안전하다.
#   · temporalio·torch·sentence-transformers·pyarrow 는 이 앱이 import 하지 않는다(worker.py 는 안 쓴다,
#     임베딩은 EMBEDDING_BACKEND=remote).
#
# 전달: 결과 폴더를 zip 으로 묶어 `POST …/800/api/v1/law/ingest?kind=ops` 로 올리고(20MB 넘으면 split),
#   코드스페이스에서 풀어 Gitea llmops/800 저장소 내용을 통째로 교체한 뒤 push → 관리자 UI 에서 리비전 추가.
set -euo pipefail
HERE=$(cd "$(dirname "$0")/.." && pwd)
OUT=${1:?사용: $0 <out_dir>}
PY=${PYTHON:-$HERE/.venv/bin/python}
WEAVIATE=${WEAVIATE_CLIENT_VERSION:-4.16.4}
# wheel 내려받기는 pip 가 필요하다. uv 가 만든 venv 에는 pip 가 없으므로 pip 있는 파이썬을 찾는다
# (--python-version 3.12 로 받으므로 실행 파이썬 버전은 상관없다).
PIP_PY=""
for cand in "$PY" "${PIP_PYTHON:-}" python3 python3.12 /usr/bin/python3; do
  [ -n "$cand" ] && "$cand" -m pip --version >/dev/null 2>&1 && { PIP_PY=$cand; break; }
done
[ -n "$PIP_PY" ] || { echo "pip 가 있는 파이썬이 없다 — PIP_PYTHON=<python> 으로 지정"; exit 1; }

rm -rf "$OUT"
mkdir -p "$OUT/src" "$OUT/lib" "$OUT/.wheels"
rsync -a --exclude '__pycache__' --exclude '*.pyc' "$HERE/src/law_indexer" "$OUT/src/"

cat > "$OUT/main.py" <<'PY'
"""코드서빙 800 — 베이스 규약(repo 루트 main.py → uvicorn main:app). 실제 앱은 law_indexer.ingest_app.

sys.path: src/ 를 앞에, lib/(베이스 venv 에 없는 의존성)를 **뒤에** 둔다 — 같은 패키지가 둘 다 있으면 venv 가 이긴다.
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "src"))
sys.path.append(str(ROOT / "lib"))

from law_indexer.ingest_app import app  # noqa: E402,F401
PY

# 운영 노드는 x86_64 리눅스, 코드서빙 베이스는 python 3.12 (template-code-serving).
"$PIP_PY" -m pip download --only-binary=:all: \
  --platform manylinux2014_x86_64 --platform manylinux_2_17_x86_64 --platform manylinux_2_28_x86_64 \
  --python-version 3.12 --implementation cp --abi cp312 --abi abi3 --abi none \
  -d "$OUT/.wheels" "weaviate-client==$WEAVIATE" "python-dotenv" "temporalio==${TEMPORALIO_VERSION:-1.31.0}"
for whl in "$OUT"/.wheels/*.whl; do
  "$PY" -m zipfile -e "$whl" "$OUT/lib/"
done
rm -rf "$OUT/.wheels"
find "$OUT" -name '__pycache__' -type d -prune -exec rm -rf {} +

cat > "$OUT/README.md" <<'MD'
# 코드서빙 800 — 법령 ZIP 수신 + 자동 적재

- `main.py`        베이스 규약 진입점(uvicorn main:app). START_COMMAND·BUILD_COMMAND 비움.
- `src/law_indexer` 적재기 정본 사본(scripts/make_codeserving_bundle.sh 로 만든다 — 손으로 고치지 않는다).
- `lib/`           베이스 venv 에 없는 의존성(weaviate-client 와 그 의존, cp312/manylinux). requirements.txt 없음.

환경변수는 관리자 UI 환경설정 탭에 — `LAW_ROOT` 와 운영 .env 의 WEAVIATE_*/EMBEDDING_*/DOC_PARSER_* (law_embedding/README.md 「운영 가동」 참고).
MD

echo "bundle: $OUT"
du -sh "$OUT/lib" "$OUT/src"
ls "$OUT"
