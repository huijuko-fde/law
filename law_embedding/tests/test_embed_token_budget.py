"""결함 C 회귀: 청크가 임베딩 모델(bge-m3) 토큰 한도를 절대 넘지 않는다.

배경 — 재수집 검증에서 청크 889,980개 중 **136개(ADMRUL 128문서)가 8,192토큰을 넘었다**
(최대 481,484토큰). 임베딩 모델은 넘는 부분을 말없이 버리므로 그 본문은 벡터에 아예 안
들어가고, 잘렸다는 신호도 어디에도 남지 않는다(무음 소실).

원인은 두 갈래였다.
  1) `content` 가 **줄바꿈도 항(①)도 없는 한 줄**이라 구조 경계 분할이 손을 못 댔다
     (`_split_hierarchical` 이 "더 쪼갤 경계 없음"으로 원문을 그대로 돌려줬다).
     실측: 송전선로 주변지역 보상계획 = 1,428,435자가 줄바꿈 0개.
  2) `search_text` **접두**(문서명·종류·장 경로·조번호·제목)만으로 예산을 다 먹은 문서.
     실측: 건설업 관리규정 제22조 = 장 경로 14,243자(아웃라인 파싱이 깨져 장 '본문'이 들어감),
     content 는 1,689자뿐인데 9,145토큰.

두 갈래 모두 여기서 막는다. 분할 뒤에도 provision_id 는 유지되고 chunk_index/chunk_count 만
늘어나야 한다(docs/schema.md §6).
"""
import json
from pathlib import Path

import pytest

from law_indexer.chunking import (
    MAX_CHAPTER_CTX_CHARS, MAX_EMBED_CHARS, EMBED_MAX_TOKENS, MIN_CONTENT_CHARS,
    clamp_search_prefix, enforce_char_limit, max_content_chars, split_prose, split_table_like,
)
from law_indexer.mapper import build_search_text, map_admrul_data

ADMRUL_FIXTURE = Path(__file__).parent / "fixtures" / "admrul.json"
# 실제 결함 문서(데이터 레포). 레포가 없는 환경에서는 건너뛴다 — 읽기 전용으로만 쓴다.
REPO_CASES = [
    ("/Users/huiju.ko/workspace/law_ai/data/ADMRUL/송전선로 주변지역 보상계획/"
     "송전선로 주변지역 보상계획.json", "전문"),
    ("/Users/huiju.ko/workspace/law_ai/data/ADMRUL/건설업 관리규정/건설업 관리규정.json", "제22조"),
]


def _tokenizer():
    """실제 bge-m3 토크나이저. 없으면 None(문자 상한 검사만 한다)."""
    try:
        from transformers import AutoTokenizer
        return AutoTokenizer.from_pretrained("BAAI/bge-m3")
    except Exception:
        return None


def _admrul_payload(content: str, *, chapter_path: str = None) -> dict:
    data = json.loads(ADMRUL_FIXTURE.read_text(encoding="utf-8"))
    article = {
        "provision_id": "admrul:표본행정규칙#JO0001",
        "article_no": "제1조",
        "article_title": "목적",
        "content": content,
    }
    if chapter_path is not None:
        article["chapter"] = chapter_path
        article["chapter_path"] = chapter_path
    data["body"] = {"articles": [article]}
    data["addenda"] = []
    data["appendices"] = []
    data.pop("amendment_text", None)
    data.pop("revision_reason", None)
    return data


def _articles(data: dict):
    return [o for o in map_admrul_data(data, Path("t.json")) if o.unit_type == "ARTICLE"]


# ---------------------------------------------------------------- 분할기 자체의 보장


def test_boundary_free_line_is_split_to_the_hard_limit():
    """줄바꿈·항·호가 하나도 없는 한 줄 — 예전에는 통째로 1청크였다."""
    content = "가나다라마바사아자차" * 60_000          # 600,000자, 줄바꿈 0
    parts = split_prose(content)
    assert len(parts) > 1
    assert max(len(p) for p in parts) <= MAX_EMBED_CHARS
    assert "".join(parts) == content                  # 글자 하나도 잃지 않는다


def test_split_prose_is_lossless_and_bounded_on_a_one_line_table():
    """실제 결함 모양(문자로 그린 표를 줄바꿈 없이 흘려 쓴 고시)."""
    row = "│{i:>6}│서울특별시 영등포구 경인로96길 10-1│보상대상│2,340,000원│"
    content = "".join(row.format(i=i) for i in range(20_000))
    parts = split_prose(content)
    assert max(len(p) for p in parts) <= MAX_EMBED_CHARS
    assert "".join(parts) == content
    assert all(parts), "빈 조각을 만들면 안 된다"


def test_table_splitter_also_respects_the_hard_limit():
    header = "[별표 1] 등급표\n| 등급 | 조건 |"
    giant_row = "| 1급 | " + ("조건" * 30_000) + " |"      # 한 행이 상한을 훌쩍 넘는다
    content = f"{header}\n{giant_row}\n| 2급 | 조건 |"
    parts = split_table_like(content)
    assert max(len(p) for p in parts) <= MAX_EMBED_CHARS
    assert giant_row.replace("\n", "") in "".join(parts).replace("\n", "")


def test_hard_limit_prefers_meaning_boundaries_when_available():
    """상한에 걸려도 가능하면 문장 끝에서 자른다(문장 중간 절단은 최후 수단)."""
    sentence = "이 규정은 건설업의 관리에 필요한 기준을 정한다. "
    content = sentence * 500
    parts = split_prose(content, max_chars=len(content) + 1, hard_max_chars=1000)
    assert max(len(p) for p in parts) <= 1000
    assert "".join(parts) == content
    assert all(p.rstrip().endswith("다.") for p in parts[:-1])


def test_hard_limit_still_cuts_when_no_boundary_exists_at_all():
    """공백도 문장부호도 없는 글자 덩어리 — 그래도 상한은 지킨다."""
    content = "가" * 5000
    parts = split_prose(content, max_chars=len(content) + 1, hard_max_chars=700)
    assert max(len(p) for p in parts) <= 700
    assert "".join(parts) == content


def test_enforce_char_limit_leaves_conforming_parts_untouched():
    """상한 이하 조각은 손대지 않는다 — 기존 청크 경계(chunk_id)가 흔들리면 안 된다."""
    parts = ["짧은 조각", "또 다른 조각", "세 번째"]
    assert enforce_char_limit(parts, MAX_EMBED_CHARS) == parts


def test_normal_sized_content_keeps_its_previous_chunking():
    """평범한 조문은 예전과 똑같이 1청크다(재색인 폭발 방지)."""
    content = "제1조(목적) 이 규칙은 표본을 정한다.\n\n" * 20
    assert split_prose(content) == [content]


# ---------------------------------------------------------------- 문자 상한 → 토큰 상한


def test_char_limit_actually_guarantees_the_token_limit():
    """문자 상한이 토큰 상한을 보장하는 근거를 실제 토크나이저로 확인한다.

    근거: bge-m3 토크나이저는 SentencePiece Unigram + byte_fallback=False 라 토큰 하나가
    원문 1자 이상을 반드시 먹는다(모르는 글자도 <unk> 1개). 원문을 안 먹는 토큰은 특수토큰
    2개와 선두 '▁' 1개뿐이다 → `tokens <= chars + 3`. 이 부등식이 깨지면(토크나이저 교체 등)
    문자 상한만으로는 토큰 상한을 보장할 수 없으므로 여기서 잡는다."""
    import random

    tokenizer = _tokenizer()
    if tokenizer is None:
        pytest.skip("bge-m3 토크나이저를 불러올 수 없음")
    rng = random.Random(20260917)
    adversarial = [
        "가나다라마바사아자차" * 400,                       # 한글 산문
        "".join("가😀" for _ in range(2000)),               # 사전에 없는 글자 교대
        "ㄱㄴㄷㄹㅁㅂㅅㅇ" * 500,                            # 한글 자모(음절 아님)
        "1234567890-.,;:()[]{}" * 200,                      # 숫자·기호
        "".join(chr(0x2000 + (i % 0xBFF)) for i in range(4000)),  # BMP 기호 구간
        "​　\t " * 1000,                           # 폭 없는 공백·전각 공백
    ]

    def _random_char():
        while True:                                   # 서로게이트(0xD800-0xDFFF)는 UTF-8 로 못 쓴다
            code = rng.randrange(0x20, 0x2FFFF)
            if not 0xD800 <= code <= 0xDFFF:
                return chr(code)

    adversarial += ["".join(_random_char() for _ in range(1500)) for _ in range(5)]
    for text in adversarial:
        clipped = text[:MAX_EMBED_CHARS]
        tokens = len(tokenizer(clipped, add_special_tokens=True, truncation=False)["input_ids"])
        assert tokens <= len(clipped) + 3, "토큰 하나가 원문 1자 이상을 먹어야 한다"
        assert tokens <= EMBED_MAX_TOKENS


def test_mapped_chunks_stay_under_the_model_token_limit():
    """매퍼가 낸 청크의 search_text 를 실제 토크나이저로 재 본다."""
    tokenizer = _tokenizer()
    if tokenizer is None:
        pytest.skip("bge-m3 토크나이저를 불러올 수 없음")
    objects = _articles(_admrul_payload("가나다라마바사아자차" * 50_000))
    assert len(objects) > 1
    for obj in objects:
        tokens = len(tokenizer(obj.search_text, add_special_tokens=True, truncation=False)["input_ids"])
        assert tokens <= EMBED_MAX_TOKENS, f"{tokens}토큰 (조각 {obj.chunk_index})"


# ---------------------------------------------------------------- 매퍼 계약(§5·§6)


def test_split_chunks_keep_provision_id_and_number_index_correctly():
    """한 조문이 여러 청크로 나뉘어도 provision_id 는 하나, 순번은 0..n-1 (docs/schema.md §6)."""
    objects = _articles(_admrul_payload("가나다라마바사아자차" * 60_000))
    assert len(objects) > 1
    assert {o.provision_id for o in objects} == {"admrul:표본행정규칙#JO0001"}
    assert [o.chunk_index for o in objects] == list(range(len(objects)))
    assert {o.chunk_count for o in objects} == {len(objects)}
    assert len({o.chunk_id for o in objects}) == len(objects)      # chunk_id 충돌 없음
    assert "".join(o.content for o in objects) == "가나다라마바사아자차" * 60_000


def test_chunk_ids_of_a_split_unit_extend_rather_than_renumber():
    """분할이 늘어나도 앞쪽 chunk_id 는 그대로다 — 재색인이 기존 객체를 덮어쓰고 뒤만 추가한다.

    (chunk_id 가 통째로 갈리면 옛 청크가 고아로 남아 같은 조문이 중복 검색된다.)"""
    short = _articles(_admrul_payload("짧은 조문"))
    long = _articles(_admrul_payload("가나다라마바사아자차" * 60_000))
    assert short[0].chunk_id == long[0].chunk_id


def test_every_split_chunk_repeats_the_search_text_prefix():
    """각 조각에 법령명·종류·조번호·제목 접두가 붙어 문맥이 유지된다(docs/indexer.md §5)."""
    objects = _articles(_admrul_payload("가나다라마바사아자차" * 60_000))
    prefix = build_search_text("표본 행정규칙", "고시", None, "제1조", "목적")
    assert len(objects) > 1
    for obj in objects:
        assert obj.search_text.startswith(prefix + "\n")
        assert obj.content.strip() in obj.search_text
        assert len(obj.search_text) <= MAX_EMBED_CHARS


def test_prefix_length_is_charged_to_the_content_budget():
    """접두가 길면 그만큼 content 예산이 줄어야 한다(접두+본문이 함께 상한을 넘으면 안 됨)."""
    chapter = "제1장 총칙 > 제2절 해고 " * 20                     # 400자대 — 정상 범위
    objects = _articles(_admrul_payload("가나다라마바사아자차" * 60_000, chapter_path=chapter))
    assert all(len(o.search_text) <= MAX_EMBED_CHARS for o in objects)
    prefix = build_search_text("표본 행정규칙", "고시", chapter, "제1조", "목적")
    assert objects[0].search_text.startswith(prefix + "\n")
    assert max(len(o.content) for o in objects) <= max_content_chars(len(prefix))


def test_bloated_chapter_path_cannot_starve_the_content_budget():
    """장 경로가 14,000자짜리로 망가져 와도 본문 몫이 남아야 한다(건설업 관리규정 제22조 실측)."""
    objects = _articles(_admrul_payload("본문 " * 400, chapter_path="제1장 목적" + "이 규정은 " * 3000))
    assert len(objects) == 1
    assert len(objects[0].search_text) <= MAX_EMBED_CHARS
    prefix_len = len(objects[0].search_text) - len(objects[0].content)
    assert prefix_len <= MAX_EMBED_CHARS - MIN_CONTENT_CHARS
    assert objects[0].content == ("본문 " * 400).strip() or objects[0].content == "본문 " * 400


def test_chapter_context_clamp_does_not_touch_normal_chapter_paths():
    """정상 장 경로(실측 99.999%가 200자 이하)는 그대로 search_text 에 들어간다."""
    chapter = "제1장 총칙 > 제2절 해고"
    objects = _articles(_admrul_payload("짧은 조문", chapter_path=chapter))
    assert chapter in objects[0].search_text
    assert objects[0].chapter == chapter          # 표시용 필드는 손대지 않는다


def test_display_chapter_field_keeps_the_raw_value_even_when_clamped():
    """접두만 줄이고 `chapter` 표시 필드는 원본을 유지한다(값이 조용히 바뀌면 안 된다)."""
    bloated = "제1장 목적" + "이 규정은 " * 3000
    objects = _articles(_admrul_payload("짧은 조문", chapter_path=bloated))
    assert objects[0].chapter == bloated.strip()           # _text() 의 trim 외엔 그대로
    assert len(objects[0].chapter) > MAX_CHAPTER_CTX_CHARS
    assert len(objects[0].search_text) <= MAX_EMBED_CHARS


def test_clamp_and_budget_helpers_are_consistent():
    assert len(clamp_search_prefix("가" * 100_000)) <= MAX_EMBED_CHARS - MIN_CONTENT_CHARS
    assert clamp_search_prefix("짧은 접두") == "짧은 접두"
    assert max_content_chars(0) == MAX_EMBED_CHARS
    assert max_content_chars(100) == MAX_EMBED_CHARS - 101
    assert max_content_chars(MAX_EMBED_CHARS * 2) >= 1          # 음수 예산이 나오면 안 된다
    assert MAX_CHAPTER_CTX_CHARS < MIN_CONTENT_CHARS * 2


# ---------------------------------------------------------------- 실제 결함 문서


# ---------------------------------------------------------------- 전체 경로(적재 파이프라인)


def _index(repo_root, store, doc_parser):
    from law_indexer.config import Settings
    from law_indexer.pipeline import index_documents
    from test_pipeline import _FakeEmbedder
    return index_documents(store, _FakeEmbedder(), Settings.from_env(), repo_root, "admrul",
                           repo_root, "abc123", "https://example.test/admrul.git",
                           recursive=True, doc_parser=doc_parser)


def _write_boundary_free_doc(repo_root: Path, content: str) -> Path:
    doc_dir = repo_root / "행정규칙" / "표본 한줄 고시"
    doc_dir.mkdir(parents=True)
    data = json.loads(ADMRUL_FIXTURE.read_text(encoding="utf-8"))
    data["law_name"] = "표본 한줄 고시"
    data["body"] = {"format": "articles", "article_count": 1, "articles": [{
        "provision_id": "admrul:표본한줄고시#JO0001", "article_no": "전문",
        "article_title": "전문", "content": content, "relations": [],
    }]}
    data["addenda"], data["appendices"], data["attachments"] = [], [], []
    path = doc_dir / "표본 한줄 고시.json"
    path.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
    return path


def test_orphan_cleanup_does_not_delete_the_newly_split_chunks(tmp_path):
    """분할로 **늘어난** 청크가 같은 실행의 고아 정리에 지워지면 안 된다.

    이 레포에서 실제로 났던 사고와 같은 모양이다 — 한쪽에서 보정한 산출물을 같은 실행의
    orphan 정리가 지워 버려 실패 목록에도 안 잡히고 done 으로 굳었다. 여기서는
    pipeline 이 upsert 에 쓴 **그 objects 목록**에서 keep_chunk_ids 를 뽑는지를 본다."""
    from test_pipeline import _FakeDocParser, _FakeStore

    repo_root = tmp_path / "admrul_data"
    content = "가나다라마바사아자차" * 60_000            # 600,000자 · 줄바꿈 0개
    _write_boundary_free_doc(repo_root, content)
    store, doc_parser = _FakeStore(), _FakeDocParser()

    result = _index(repo_root, store, doc_parser)
    assert result["files_failed"] == 0 and result["objects_failed"] == 0

    from law_indexer.config import Settings
    stored = [o for o in store.objects_in(Settings.from_env().admrul_collection)
              if o.source_type == "JSON"]
    assert len(stored) > 1, "한 줄짜리 본문이 분할되지 않았다"
    stored.sort(key=lambda o: o.chunk_index)
    assert [o.chunk_index for o in stored] == list(range(len(stored)))
    assert {o.chunk_count for o in stored} == {len(stored)}
    assert {o.provision_id for o in stored} == {"admrul:표본한줄고시#JO0001"}
    assert "".join(o.content for o in stored) == content, "고아 정리 후 본문이 빠졌다"
    assert max(len(o.search_text) for o in stored) <= MAX_EMBED_CHARS


def test_reindexing_the_same_document_is_idempotent(tmp_path):
    """같은 문서를 다시 색인해도 청크 집합이 그대로여야 한다(중복·유실 없음)."""
    from test_pipeline import _FakeDocParser, _FakeStore
    from law_indexer.config import Settings

    repo_root = tmp_path / "admrul_data"
    _write_boundary_free_doc(repo_root, "가나다라마바사아자차" * 60_000)
    store, collection = _FakeStore(), Settings.from_env().admrul_collection

    _index(repo_root, store, _FakeDocParser())
    first = {o.chunk_id for o in store.objects_in(collection)}
    _index(repo_root, store, _FakeDocParser())
    second = {o.chunk_id for o in store.objects_in(collection)}
    assert first == second and len(second) == len(first)


@pytest.mark.parametrize("path,unit_no", REPO_CASES)
def test_real_repo_documents_that_used_to_overflow(path, unit_no):
    """검증에서 실제로 걸렸던 문서로 확인한다(데이터 레포가 있을 때만)."""
    source = Path(path)
    if not source.exists():
        pytest.skip(f"데이터 레포 없음: {source}")
    objects = [o for o in map_admrul_data(json.loads(source.read_text(encoding="utf-8")), source)
               if o.unit_no == unit_no and o.unit_type == "ARTICLE"]
    assert objects
    assert max(len(o.search_text) for o in objects) <= MAX_EMBED_CHARS
    assert len({o.provision_id for o in objects}) == 1
    assert [o.chunk_index for o in objects] == list(range(len(objects)))

    tokenizer = _tokenizer()
    if tokenizer is None:
        return
    for obj in objects:
        tokens = len(tokenizer(obj.search_text, add_special_tokens=True, truncation=False)["input_ids"])
        assert tokens <= EMBED_MAX_TOKENS, f"{source.name} {unit_no} 조각 {obj.chunk_index}: {tokens}토큰"
