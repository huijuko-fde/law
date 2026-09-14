import json
import time
import urllib.error
import urllib.request
from typing import List, Optional, Sequence

# 재시도할 일시적 HTTP 상태(그 외 4xx 는 결정적이라 즉시 중단)
_RETRYABLE_HTTP = (429, 500, 502, 503, 504)


def _backoff(attempt: int) -> float:
    """지수 백오프(최대 8초) — 재시도 간 서버를 쉬게 한다."""
    return min(2.0 ** attempt, 8.0)


class LocalEmbedder:
    """SentenceTransformer 하나를 프로세스에 로드해 문서/질의 벡터를 만든다.

    모델은 EMBEDDING_MODEL 로 갈아 끼운다(기본 BAAI/bge-m3). 모델마다 질의 프리픽스 유무가
    다르므로 이 클래스는 **모델이 선언한 것만** 쓴다 — 아래 _encode 주석 참고."""

    def __init__(self, model_name: str, batch_size: int = 16, normalize_embeddings: bool = True):
        from sentence_transformers import SentenceTransformer
        self.model_name = model_name
        self.batch_size = batch_size
        self.normalize_embeddings = normalize_embeddings
        # use_memory_efficient_attention=False 는 Snowflake arctic 체크포인트가 켜 두는
        #   xformers(CUDA 전용) 경로를 끄기 위한 것이다. bge-m3 같은 표준 체크포인트에는
        #   해당 설정이 없어 무시되므로, arctic 으로 되돌릴 때를 위해 그대로 둔다.
        self.model = SentenceTransformer(
            model_name,
            trust_remote_code=True,
            config_kwargs={"use_memory_efficient_attention": False},
        )
        dimension = self.model.get_sentence_embedding_dimension()
        if not dimension:
            raise RuntimeError(f"임베딩 차원을 확인할 수 없습니다: {model_name}")
        self.dimension = int(dimension)

    def _encode(self, texts: Sequence[str], query: bool) -> List[List[float]]:
        """문서/질의에 맞는 SentenceTransformer encode API로 벡터를 만든다.

        질의 프리픽스는 **모델이 실제로 선언한 경우에만** 쓴다:
          · arctic-embed v2 는 'query' 프롬프트를 config 에 넣어 배포한다 → 그걸 쓴다.
          · bge-m3 는 검색용 프리픽스가 없다(질의·문서 모두 원문 그대로) → 안 붙인다.
        무조건 prompt_name="query" 를 넘기면 프롬프트가 없는 모델에서 ValueError 로 죽는다.
        sentence-transformers 5 부터 생긴 encode_query/encode_document 가 있으면 그쪽이 우선."""
        method = getattr(self.model, "encode_query" if query else "encode_document", None)
        kwargs = dict(batch_size=self.batch_size, normalize_embeddings=self.normalize_embeddings,
                      show_progress_bar=len(texts) > self.batch_size, convert_to_numpy=True)
        if method:
            vectors = method(list(texts), **kwargs)
        else:
            if query and "query" in (getattr(self.model, "prompts", None) or {}):
                kwargs["prompt_name"] = "query"
            vectors = self.model.encode(list(texts), **kwargs)
        return vectors.tolist()

    def embed_documents(self, texts: Sequence[str]) -> List[List[float]]:
        """색인 대상 문서 텍스트들을 document embedding으로 변환한다."""
        return self._encode(texts, query=False)

    def embed_query(self, text: str) -> List[float]:
        """검색어 하나를 query embedding으로 변환한다."""
        return self._encode([text], query=True)[0]


class RemoteEmbedder:
    """OpenAI 호환 `/v1/embeddings` 엔드포인트(genos 모델 서빙 등)를 호출하는 임베더.

    LocalEmbedder 와 같은 인터페이스(model_name·dimension·embed_documents·embed_query)를 제공해
    파이프라인이 백엔드를 구분하지 않게 한다. 색인·질의 모두 **같은 엔드포인트를 같은 방식**으로
    호출한다(정규화·프리픽스는 서버가 처리 — document/query 를 나누지 않아야 두 벡터가 같은 공간).
    차원은 첫 응답에서 확정한다. in-mesh 호출이면 api_key 없이(무인증) 동작한다.
    """

    def __init__(self, api_url: str, model_name: str, api_key: Optional[str] = None,
                 batch_size: int = 16, timeout: float = 60.0, max_retries: int = 3):
        self.api_url = api_url                     # 전체 엔드포인트(예: http://gw:8080/rep/serving/10/v1/embeddings)
        self.model_name = model_name
        self.api_key = api_key
        self.batch_size = max(1, batch_size)
        self.timeout = timeout
        self.max_retries = max(0, max_retries)
        self._dimension: Optional[int] = None

    @property
    def dimension(self) -> int:
        """벡터 차원. 아직 모르면 짧은 프로브 1회로 확정한다(컬렉션 검증에 필요)."""
        if self._dimension is None:
            self._embed_batch(["dimension probe"])
        return int(self._dimension)

    def _embed_batch(self, texts: Sequence[str]) -> List[List[float]]:
        """한 배치를 /v1/embeddings 로 보내 벡터 목록을 받는다.

        견고화:
          · **재시도** — 5xx/429/타임아웃/연결오류는 지수 백오프로 max_retries 회 재시도(일시장애 흡수).
            그 외 4xx 는 결정적이라 즉시 중단.
          · **개수 검증** — 응답 벡터 수 ≠ 입력 텍스트 수면 중단한다. (부분 응답을 zip 하면 뒤 텍스트가
            엉뚱한 벡터에 매칭돼 '길이는 맞는데 의미가 틀린' 벡터가 조용히 저장된다.)
          · **결측 가드** — index 로 정렬하되 embedding 누락은 명확한 에러로."""
        texts = list(texts)
        body = json.dumps({"model": self.model_name, "input": texts}).encode("utf-8")
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"

        payload = None
        for attempt in range(self.max_retries + 1):
            try:
                request = urllib.request.Request(self.api_url, data=body, headers=headers, method="POST")
                with urllib.request.urlopen(request, timeout=self.timeout) as response:
                    payload = json.loads(response.read().decode("utf-8"))
                break
            except urllib.error.HTTPError as exc:
                if exc.code in _RETRYABLE_HTTP and attempt < self.max_retries:
                    time.sleep(_backoff(attempt))
                    continue
                raise RuntimeError(f"임베딩 API HTTP {exc.code}: {exc.reason}") from exc
            except (urllib.error.URLError, TimeoutError, ConnectionError, OSError) as exc:
                if attempt < self.max_retries:
                    time.sleep(_backoff(attempt))
                    continue
                raise RuntimeError(f"임베딩 API 연결 실패({self.api_url}): {exc}") from exc

        data = payload.get("data") if isinstance(payload, dict) else None
        if not isinstance(data, list):
            raise RuntimeError(f"임베딩 API 응답에 data 배열이 없습니다: {str(payload)[:200]}")
        items = sorted(data, key=lambda item: item.get("index", 0))
        vectors: List[List[float]] = []
        for item in items:
            vec = (item or {}).get("embedding")
            if vec is None:
                raise RuntimeError("임베딩 API 응답 항목에 embedding 이 없습니다")
            vectors.append(vec)
        if len(vectors) != len(texts):
            raise RuntimeError(
                f"임베딩 개수 불일치: 입력 {len(texts)} != 응답 {len(vectors)} — "
                "부분 응답으로 잘못된 벡터가 매칭될 수 있어 중단")
        if vectors and self._dimension is None:
            self._dimension = len(vectors[0])
        return vectors

    def embed_documents(self, texts: Sequence[str]) -> List[List[float]]:
        """색인 대상 텍스트들을 배치로 나눠 임베딩한다."""
        texts = list(texts)
        result: List[List[float]] = []
        for start in range(0, len(texts), self.batch_size):
            result.extend(self._embed_batch(texts[start:start + self.batch_size]))
        return result

    def embed_query(self, text: str) -> List[float]:
        """검색어 하나를 임베딩한다(문서와 동일 엔드포인트·동일 방식)."""
        return self._embed_batch([text])[0]


def make_embedder(settings):
    """설정(embedding_backend)에 맞는 임베더를 만든다: local=LocalEmbedder / remote=RemoteEmbedder.

    remote 는 EMBEDDING_API_URL(전체 /v1/embeddings 경로)이 필요하다. 색인·질의가 반드시 같은
    백엔드를 써야 벡터가 일치한다(genos 서빙으로 통일 시 law_agent 도 remote 로)."""
    if settings.embedding_backend == "remote":
        if not settings.embedding_api_url:
            raise ValueError("EMBEDDING_BACKEND=remote 인데 EMBEDDING_API_URL 이 비어 있습니다.")
        return RemoteEmbedder(
            settings.embedding_api_url, settings.embedding_model, settings.embedding_api_key,
            settings.embedding_batch_size,
            timeout=getattr(settings, "embedding_timeout", 180.0),
            max_retries=getattr(settings, "embedding_max_retries", 3))
    return LocalEmbedder(settings.embedding_model, settings.embedding_batch_size,
                          settings.normalize_embeddings)
