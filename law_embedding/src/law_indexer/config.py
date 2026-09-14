from dataclasses import dataclass
import os
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv


def _bool(name: str, default: bool) -> bool:
    """환경변수 문자열을 bool 설정값으로 파싱한다."""
    value = os.getenv(name)
    return default if value is None else value.lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class Settings:
    """law_indexer 실행에 필요한 Weaviate·임베딩·입력 경로·Git·전처리기 설정 묶음."""

    weaviate_http_host: str
    weaviate_http_port: int
    weaviate_grpc_host: str
    weaviate_grpc_port: int
    # genos 등 인증 Weaviate 접속용. 비우면 익명 접속(로컬 개발 그대로). secure 는 http/grpc 공통.
    weaviate_api_key: Optional[str]
    # genos 는 VDB 를 컬렉션 단위로 나눠 발급해서 컬렉션별 키가 **서로 다르다**
    # (각 키는 자기 컬렉션만 보인다 — RBAC). 안 주면 법령 키로 폴백(같은 VDB 를 쓰는 로컬 등).
    admrul_weaviate_api_key: Optional[str]
    schlpub_weaviate_api_key: Optional[str]
    weaviate_secure: bool
    # 정의서 값은 복제 3 · 샤드 3(운영 3노드). 복제·샤드는 **노드 수를 넘길 수 없어**
    #   1노드 개발기에서는 3 을 요구하면 컬렉션 생성이 거부된다 → env 로 낮춘다.
    weaviate_replication_factor: int
    weaviate_shard_count: int
    law_collection: str
    # 하이브리드 검색의 BM25 절반을 담당하는 search_text 토크나이저.
    #   kagome_kr(한국어 형태소, 기본) → Weaviate 에 모듈이 없으면 trigram 으로 자동 대체.
    search_text_tokenization: str
    admrul_collection: str
    # 학칙·공단정관·공공기관 전용 컬렉션 — 레포(SCHLPUBRUL)당 컬렉션 하나(3레포=3컬렉션).
    schlpub_collection: str
    embedding_model: str
    embedding_batch_size: int
    normalize_embeddings: bool
    # 임베딩 백엔드: "local"=모델을 프로세스에 로드(기본), "remote"=OpenAI 호환 /v1/embeddings 호출(genos 서빙 등).
    # remote 면 색인·질의가 같은 엔드포인트를 써야 일치. in-mesh 호출은 api_key 불필요(비우면 무인증).
    embedding_backend: str
    embedding_api_url: Optional[str]
    embedding_api_key: Optional[str]
    # 원격 임베딩 호출 타임아웃(초)·재시도 — 게이트웨이가 부하 시 60초를 넘겨 타임아웃 나던 실사례 대응.
    embedding_timeout: float
    embedding_max_retries: int
    input_data_path: Path

    # 법령/행정규칙 데이터 저장소 — 서로 독립 관리(URL·경로·branch 분리)
    law_repo_url: str
    law_repo_path: Path
    law_repo_branch: str
    admrul_repo_url: str
    admrul_repo_path: Path
    admrul_repo_branch: str
    # 학칙·공단정관·공공기관은 별도 레포다(수집기가 SCHLPUB_GIT_EXPORT_REPO 로 내보낸다).
    #   비우면 admrul 레포로 폴백 = 옛 2레포 구성 그대로.
    schlpub_repo_url: str
    schlpub_repo_path: Optional[Path]          # 비어 있으면(None) admrul 레포로 폴백
    schlpub_repo_branch: str
    # 실제 law_data(7GB+)·admrul_data(수 GB) clone/fetch 시간 실측 기준 넉넉히 잡은 기본값.
    # 300초 기본값으로는 최초 clone이 타임아웃난다(실행해서 확인함) — 환경변수로 조정 가능.
    git_sync_timeout: int

    # 첨부용 전처리기(Doc Parser)
    doc_parser_base_url: str
    doc_parser_endpoint_path: str            # 첨부용 — 기본(hwp/hwpx/pdf/docx 등 문서)
    doc_parser_image_endpoint_path: str      # 적재용(intelligent) — **이미지 전용**
    doc_parser_api_key: Optional[str]
    doc_parser_upload: bool
    doc_parser_timeout: float
    doc_parser_image_concurrency: int   # 같은 문서 안 이미지 OCR 동시 호출 수(기본 1=순차)
    doc_parser_max_retries: int
    doc_parser_chunk_size: int
    doc_parser_chunk_overlap: int
    doc_parser_shared_dir: Optional[Path]
    # 호스트/컨테이너 마운트 경로가 다를 때(실제 doc-parser-local 컨테이너로 확인된 경우)만 쓴다.
    # 둘 다 안 주면 doc_parser_shared_dir 값을 그대로 양쪽에 쓴다(같은 경로 마운트 케이스).
    doc_parser_shared_host_dir: Optional[Path]
    doc_parser_shared_container_dir: Optional[str]
    doc_parser_keep_temp_files: bool

    # §6 JSONL package 소비자 옵션 — 9개 subcase 를 흡수하는 소비자의 "켜고 끄는" 축은 딱 둘이다.
    # (record_type 자체는 소비자가 전부 이해한다 — 무엇이 오느냐는 생산자·망 환경이 정한다.)
    #  1) package_preprocess_files: `file` record(원본 첨부)를 내부 전처리기로 돌려 색인할지.
    #     **소비자 자신의 Doc Parser 유무에서 자동 유도**(DOC_PARSER_BASE_URL 있으면 on). 없으면 보류(pending).
    #     (옛 PACKAGE_PREPROCESS_FILES env 로 override 가능 — 하위호환.)
    #  2) package_store_original: `document`/`file` 원문을 내부 저장소에 보관할지(원문 표시용 = subcase 2).
    #     genos 초기처럼 VDB 중심이면 off.
    package_preprocess_files: bool
    #  index_preprocess_files: `index` 명령이 첨부(is_file_only 별표·문서 원문)를 Doc Parser 로 돌릴지.
    #    false 면 본문 JSON 만 색인하고 첨부는 **건너뛴 수(skipped)로 집계**한다.
    #    운영 지침(CLAUDE.md)의 "벌크 색인은 docparser 끄고 → 대상만 켜서 타겟 재색인" 을 코드로 실행하기
    #    위한 스위치. 예전엔 DOC_PARSER_BASE_URL 이 비어도 무조건 클라이언트를 만들어 **첨부마다 실패**했다.
    index_preprocess_files: bool
    package_file_inbox_dir: Optional[Path]      # `file` record 의 transfer_name 이 도착하는 내부 디렉터리
    # 전처리까지 성공한 옆채널(inbox) 파일을 지울지. 안 지우면 전달 영역이 계속 커진다.
    package_delete_consumed_files: bool
    package_store_original: bool
    package_original_dir: Optional[Path]        # 원문(JSON/파일) 내부 저장 루트(평면 구조)
    #  3) package_mirror_repo: 증분마다 **데이터 레포 자체를 최신화**할지(repo 레이아웃 그대로).
    #     초기 pull 로 이미 레포를 가진 내부망에서, 별도 저장 대신 그 레포의 같은 위치를 갱신한다
    #     (document→git_path 위치의 .json, 원본 파일→문서 디렉터리 밑 첨부미러/). off 면 VDB 만.
    package_mirror_repo: bool
    #     package_mirror_push: 미러 후 그 레포를 내부망 git 서버(Gitea/GitLab)로 commit+push 할지.
    #     off(기본) 이면 파일만 최신화(폴더만 가짐). on 이면 수집기와 동일한 커밋 메시지로 commit 후 origin push.
    package_mirror_push: bool
    #  4) package_delete_consumed_package: 소비 완료(무오류) 후 package JSONL 을 지울지.
    #     전송 채널(공유폴더/NFS/MinIO)에 쌓이지 않게. 오류가 있으면 재시도 위해 남긴다.
    package_delete_consumed_package: bool
    #  5) package_nack_dir: 소비 최종 실패(격리) 시 **생산자에게 되돌릴 문서 목록**을 남길 폴더.
    #     생산자는 sink 전달 성공 시점에 '발송 완료' 로 마킹하므로, 여기서 조용히 격리하면 그
    #     문서들이 다음 개정 때까지 색인되지 않는다. nack 을 남기면 생산자가 재발송/재수집으로
    #     되돌린다. 수집기의 PACKAGE_NACK_DIR 과 **같은 경로**여야 한다(기본: 소비 폴더/nack).
    package_nack_dir: Optional[Path]

    def api_key_for(self, source: Optional[str]) -> Optional[str]:
        """그 source 의 컬렉션에 접속할 API 키. genos 는 컬렉션마다 키가 다르다."""
        if source == "admrul":
            return self.admrul_weaviate_api_key
        if source == "schlpub":
            return self.schlpub_weaviate_api_key
        return self.weaviate_api_key

    def collection_for(self, source: Optional[str]) -> str:
        """그 source 의 컬렉션 이름. 레포당 컬렉션 하나(law/admrul/schlpub = 3레포 3컬렉션)."""
        if source == "admrul":
            return self.admrul_collection
        if source == "schlpub":
            return self.schlpub_collection
        return self.law_collection

    # 학칙공단 target — 수집기(SCHLPUB_TARGETS)와 같은 묶음.
    SCHLPUB_TARGETS = ("school", "pi", "public")
    # 학칙공단 문서의 law_id/adm_uid 접두("school:12345" 꼴) — payload 없이 law_id 만 있는
    #   delete record 의 컬렉션 라우팅에 쓴다.
    _SCHLPUB_ID_PREFIXES = tuple(f"{t}:" for t in SCHLPUB_TARGETS)

    def source_for_doc(self, doc_target: Optional[str], law_id: Optional[str] = None,
                       default: str = "admrul") -> str:
        """문서 하나가 들어갈 컬렉션 source 를 정한다.

        수집기 package 는 source 가 law/admrul 둘뿐이고 **학칙공단 문서는 admrul package 에
        doc_target(school/pi/public)으로 섞여 온다** — 컬렉션은 3개라 문서 단위로 갈라야 한다.
        doc_target 이 없으면(delete record) law_id 접두("school:…")로 판별한다."""
        target = (doc_target or "").strip()
        if target in self.SCHLPUB_TARGETS:
            return "schlpub"
        if target in ("eflaw", "law", "법령"):
            return "law"
        if target == "admrul":
            return "admrul"
        if law_id and str(law_id).startswith(self._SCHLPUB_ID_PREFIXES):
            return "schlpub"
        return default

    def repo_for(self, doc_target: Optional[str], source: Optional[str] = None) -> Path:
        """doc_target → 데이터 레포 경로. 레포 3개(LAW/ADMRUL/SCHLPUBRUL) = 컬렉션 3개.

        SCHLPUB_REPO_PATH 를 비우면 admrul 레포로 폴백한다(옛 2레포 구성 유지).
        """
        target = (doc_target or "").strip()
        if target in self.SCHLPUB_TARGETS:
            return self.schlpub_repo_path or self.admrul_repo_path
        if target in ("eflaw", "law", "법령"):
            return self.law_repo_path
        if target == "admrul":
            return self.admrul_repo_path
        src = source or "law"
        if src == "schlpub":
            return self.schlpub_repo_path or self.admrul_repo_path
        return self.law_repo_path if src == "law" else self.admrul_repo_path

    def repo_branch_for(self, doc_target: Optional[str], source: Optional[str] = None) -> str:
        target = (doc_target or "").strip()
        if target in self.SCHLPUB_TARGETS and self.schlpub_repo_path:
            return self.schlpub_repo_branch
        if target in ("eflaw", "law", "법령"):
            return self.law_repo_branch
        if target == "admrul":
            return self.admrul_repo_branch
        src = source or "law"
        if src == "schlpub" and self.schlpub_repo_path:
            return self.schlpub_repo_branch
        return self.law_repo_branch if src == "law" else self.admrul_repo_branch

    @classmethod
    def from_env(cls) -> "Settings":
        """`.env`와 환경변수에서 law_indexer 설정을 읽는다."""
        load_dotenv()
        input_data_path = Path(os.getenv("INPUT_DATA_PATH", "../data"))
        shared_dir = os.getenv("DOC_PARSER_SHARED_DIR")
        shared_host_dir = os.getenv("DOC_PARSER_SHARED_HOST_DIR") or shared_dir
        shared_container_dir = os.getenv("DOC_PARSER_SHARED_CONTAINER_DIR") or shared_dir
        return cls(
            weaviate_http_host=os.getenv("WEAVIATE_HTTP_HOST", "localhost"),
            weaviate_http_port=int(os.getenv("WEAVIATE_HTTP_PORT", "8080")),
            weaviate_grpc_host=os.getenv("WEAVIATE_GRPC_HOST", "localhost"),
            weaviate_grpc_port=int(os.getenv("WEAVIATE_GRPC_PORT", "50051")),
            weaviate_api_key=os.getenv("WEAVIATE_API_KEY") or None,
            admrul_weaviate_api_key=(os.getenv("ADMRUL_WEAVIATE_API_KEY")
                                     or os.getenv("WEAVIATE_API_KEY") or None),
            schlpub_weaviate_api_key=(os.getenv("SCHLPUB_WEAVIATE_API_KEY")
                                      or os.getenv("WEAVIATE_API_KEY") or None),
            weaviate_secure=_bool("WEAVIATE_SECURE", False),
            weaviate_replication_factor=int(os.getenv("WEAVIATE_REPLICATION_FACTOR", "3")),
            weaviate_shard_count=int(os.getenv("WEAVIATE_SHARD_COUNT", "3")),
            # law_agent 등 기존 소비자가 기대하는 기본값(LegalProvisionIndex)을 법령 컬렉션이 그대로 물려받는다.
            law_collection=os.getenv("LAW_COLLECTION", os.getenv("WEAVIATE_COLLECTION", "LegalProvisionIndex")),
            admrul_collection=os.getenv("ADMRUL_COLLECTION", "AdmrulProvisionIndex"),
            schlpub_collection=os.getenv("SCHLPUB_COLLECTION", "SchlPubRulProvisionIndex"),
            search_text_tokenization=os.getenv("SEARCH_TEXT_TOKENIZATION", "kagome_kr").strip().lower(),
            embedding_model=os.getenv("EMBEDDING_MODEL", "BAAI/bge-m3"),
            embedding_batch_size=int(os.getenv("EMBEDDING_BATCH_SIZE", "16")),
            normalize_embeddings=_bool("NORMALIZE_EMBEDDINGS", True),
            embedding_backend=os.getenv("EMBEDDING_BACKEND", "local").strip().lower(),
            embedding_api_url=os.getenv("EMBEDDING_API_URL") or None,
            embedding_api_key=os.getenv("EMBEDDING_API_KEY") or None,
            embedding_timeout=float(os.getenv("EMBEDDING_TIMEOUT", "180")),
            embedding_max_retries=int(os.getenv("EMBEDDING_MAX_RETRIES", "3")),
            input_data_path=input_data_path,
            law_repo_url=os.getenv("LAW_REPO_URL", "https://github.com/genonai/LAW.git"),
            law_repo_path=Path(os.getenv("LAW_REPO_PATH", str(input_data_path / "LAW"))),
            law_repo_branch=os.getenv("LAW_REPO_BRANCH", "main"),
            admrul_repo_url=os.getenv("ADMRUL_REPO_URL", "https://github.com/genonai/ADMRUL.git"),
            admrul_repo_path=Path(os.getenv("ADMRUL_REPO_PATH", str(input_data_path / "ADMRUL"))),
            admrul_repo_branch=os.getenv("ADMRUL_REPO_BRANCH", "main"),
            schlpub_repo_url=os.getenv("SCHLPUB_REPO_URL", "https://github.com/genonai/SCHLPUBRUL.git"),
            # ⚠ 빈 문자열을 Path("") 로 두면 PosixPath(".") 이 되어 **truthy** 라 폴백이 안 걸린다.
            #   "설정 안 함" 은 None 이어야 한다.
            schlpub_repo_path=(Path(_schlpub_path)
                               if (_schlpub_path := os.getenv(
                                   "SCHLPUB_REPO_PATH",
                                   str(input_data_path / "SCHLPUBRUL")).strip()) else None),
            schlpub_repo_branch=os.getenv("SCHLPUB_REPO_BRANCH", "main"),
            git_sync_timeout=int(os.getenv("GIT_SYNC_TIMEOUT", "3600")),
            # 비우면 = 전처리기 없음 → file 레코드/is_file_only 별표는 보류(pending). 있으면 전처리.
            doc_parser_base_url=os.getenv("DOC_PARSER_BASE_URL", ""),
            doc_parser_endpoint_path=os.getenv("DOC_PARSER_ENDPOINT_PATH", "/run"),
            doc_parser_image_endpoint_path=os.getenv("DOC_PARSER_IMAGE_ENDPOINT_PATH", "")
            or os.getenv("DOC_PARSER_ENDPOINT_PATH", "/run"),
            doc_parser_api_key=os.getenv("DOC_PARSER_API_KEY") or None,
            doc_parser_upload=_bool("DOC_PARSER_UPLOAD", False),
            doc_parser_timeout=float(os.getenv("DOC_PARSER_TIMEOUT", "60")),
            doc_parser_max_retries=int(os.getenv("DOC_PARSER_MAX_RETRIES", "2")),
            doc_parser_image_concurrency=int(os.getenv("DOC_PARSER_IMAGE_CONCURRENCY", "1")),
            doc_parser_chunk_size=int(os.getenv("DOC_PARSER_CHUNK_SIZE", "2000")),
            doc_parser_chunk_overlap=int(os.getenv("DOC_PARSER_CHUNK_OVERLAP", "200")),
            doc_parser_shared_dir=Path(shared_dir) if shared_dir else None,
            doc_parser_shared_host_dir=Path(shared_host_dir) if shared_host_dir else None,
            doc_parser_shared_container_dir=shared_container_dir or None,
            doc_parser_keep_temp_files=_bool("DOC_PARSER_KEEP_TEMP_FILES", False),
            # file 레코드를 소비자가 전처리할지 = **소비자 자신의 Doc Parser 유무에서 자동 유도**
            # (생산자 모드는 망 분리라 모름 — 패키지 record_type 이 뭘 할지 알려줌). Doc Parser 있으면
            # 전처리, 없으면 pending. 옛 PACKAGE_PREPROCESS_FILES 로 강제 override 도 가능(하위호환).
            index_preprocess_files=_bool("INDEX_PREPROCESS_FILES", True),
            package_preprocess_files=_bool(
                "PACKAGE_PREPROCESS_FILES", bool(os.getenv("DOC_PARSER_BASE_URL", "").strip())),
            package_file_inbox_dir=(
                Path(os.environ["PACKAGE_FILE_INBOX_DIR"]) if os.getenv("PACKAGE_FILE_INBOX_DIR") else None),
            package_delete_consumed_files=_bool("PACKAGE_DELETE_CONSUMED_FILES", False),
            package_store_original=_bool("PACKAGE_STORE_ORIGINAL", False),
            package_original_dir=(
                Path(os.environ["PACKAGE_ORIGINAL_DIR"]) if os.getenv("PACKAGE_ORIGINAL_DIR") else None),
            package_mirror_repo=_bool("PACKAGE_MIRROR_REPO", False),
            package_mirror_push=_bool("PACKAGE_MIRROR_PUSH", False),
            package_delete_consumed_package=_bool("PACKAGE_DELETE_CONSUMED_PACKAGE", False),
            package_nack_dir=(
                Path(os.environ["PACKAGE_NACK_DIR"]) if os.getenv("PACKAGE_NACK_DIR") else None),
        )
