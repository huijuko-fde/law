"""조문/부칙/별표 API content 청킹 정책(§5).

단순 글자 수로 아무데서나 자르지 않는다 — **법 구조 경계**를 우선한다:
조문이 최대 크기를 넘으면 항(①②) → 호(1.) → 문단(빈 줄) → 줄 순서로 내려가며 나눈다.
문장 중간은 자르지 않는다(단 하나의 예외는 아래 '모델 상한'이다 — 구조 경계가 전혀 없는
한 줄은 잘라야 임베딩이 뒤를 통째로 버리지 않는다).
표처럼 보이는 별표는 줄(행) 경계로만 나누고 헤더를 반복한다.
최대 크기 이하면 항상 1청크 그대로 둔다(대다수 조문·별표가 그렇다).

기본 최대 크기는 2,500자다. 예전엔 arctic-embed-l-v2.0-ko 의 학습 길이(1,300토큰)가 상한이라
어쩔 수 없는 값이었지만, bge-m3 는 8,192토큰까지 받으므로 지금은 **모델 제약이 아니라 검색
단위 선택**이다 — 한 청크가 커질수록 조문 하나를 정확히 집어내는 힘이 떨어진다. 늘리려면
CHUNK_MAX_CHARS 로 조절하되, 바꾸면 청크 경계가 달라져 chunk_id 가 전부 바뀐다(전량 재색인).

**두 개의 상한이 있다 — 헷갈리면 안 된다.**
  · `MAX_CHUNK_CHARS`(검색 단위) — 구조 경계로만 지킨다. 경계가 없으면 **못 지킬 수 있다**
    (문장 중간을 자르지 않기 위한 의도적 방침). 지금까지 상한은 이것 하나뿐이었다.
  · `MAX_EMBED_CHARS`(모델 상한) — **무조건 지킨다**. 넘으면 임베딩 모델이 앞부분만 보고
    말없이 잘라내 본문 대부분이 벡터에 안 들어간다(무음 소실). 구조 경계가 하나도 없으면
    마지막엔 글자 수로라도 자른다.

왜 문자 수로 토큰 상한을 보장할 수 있나: bge-m3 토크나이저는 XLM-R SentencePiece **Unigram·
byte_fallback=False** 라, 토큰 하나가 원문 1자 이상을 반드시 먹는다(사전에 없는 글자도 <unk>
토큰 1개로 접힌다 — 바이트 단위로 불어나지 않는다). 원문 글자를 안 먹는 토큰은 특수토큰
<s>·</s> 2개와 선두 '▁' 1개뿐이라 `tokens <= chars + 3` 이 항상 성립한다. 실측 최악
(한글 자모 4,000자 → 4,003토큰, 랜덤 BMP·이모지 교대도 동일)이 정확히 이 상한이다.
토크나이저를 **런타임에 불러 재지 않는 이유**는 결정성이다 — 모델 파일을 못 받는 실행이
섞이면 같은 문서가 실행마다 다르게 쪼개져 chunk_id 가 흔들리고 중복 청크가 남는다.
byte_fallback 이 있는 토크나이저(Llama 계열 등)로 갈아끼운다면 EMBED_MAX_CHARS 를 직접 낮춰라."""
import os
import re
from typing import List

from dotenv import load_dotenv

load_dotenv()

# 검색 단위 기준(모델 상한 아님 — bge-m3 는 8,192토큰). CHUNK_MAX_CHARS 로 조절.
MAX_CHUNK_CHARS = int(os.getenv("CHUNK_MAX_CHARS", "2500"))
_TABLE_HEADER_LINES = 2  # 표 제목·헤더로 간주해 분할된 조각마다 반복할 앞줄 수

# 임베딩 모델 입력 상한(토큰). bge-m3 = 8,192.
EMBED_MAX_TOKENS = int(os.getenv("EMBED_MAX_TOKENS", "8192"))
# 그 토큰 상한을 토크나이저 없이 보장하는 문자 상한(위 모듈 설명 참고).
# 상한식은 `tokens <= chars + 3` 이다 — 특수토큰 <s>·</s> 2개와, 원문 글자를 먹지 않는
# 선두 '▁'(SentencePiece 어두 표시) 1개. 실측 최악(한글 자모 4,000자 → 4,003토큰)과 정확히 같다.
# 여유 8자를 빼 8,184자로 둔다 → 최악이어도 8,187토큰으로 8,192 안쪽.
MAX_EMBED_CHARS = max(1, int(os.getenv("EMBED_MAX_CHARS", str(EMBED_MAX_TOKENS - 8))))
# 접두(문서명·장 경로·조번호…)가 아무리 길어도 본문 몫으로 남겨 두는 최소 예산.
# 접두가 예산을 통째로 먹으면 본문이 한 글자도 벡터에 안 들어간다(실측: 장 경로 14,243자 문서).
MIN_CONTENT_CHARS = int(os.getenv("EMBED_MIN_CONTENT_CHARS", "512"))
# search_text 접두에 넣는 장 경로의 상한. 정상 장 경로는 실측 최대 400자(전 코퍼스 조문
# 344,000개 중 400자 초과는 5개뿐이고 전부 아웃라인 파싱이 깨져 장 '본문'이 들어간 것)라
# 512자면 멀쩡한 문서는 하나도 건드리지 않는다.
MAX_CHAPTER_CTX_CHARS = int(os.getenv("CHAPTER_CONTEXT_MAX_CHARS", "512"))

# 분할 경계 우선순위(위에서부터): 항(①②…) > 호(1. 2. …) > 문단(빈 줄) > 줄.
# 항/호 는 마커 '앞'에서 나눠(lookahead, 구분자 소비 안 함 → 조각을 그대로 이어붙이면 원문 복원).
# 문단·줄 은 구분자(\n\n·\n)를 소비하므로 그 구분자로 다시 이어붙인다.
_HANG_RE = re.compile(r"(?m)(?=^[ \t]*[①②③④⑤⑥⑦⑧⑨⑩⑪⑫⑬⑭⑮⑯⑰⑱⑲⑳㉑㉒㉓㉔㉕㉖㉗㉘㉙㉚])")
_HO_RE = re.compile(r"(?m)(?=^[ \t]*\d{1,3}\.[ \t])")
_PARA_RE = re.compile(r"\n[ \t]*\n")
_LINE_RE = re.compile(r"\n")
_BOUNDARIES = [(_HANG_RE, ""), (_HO_RE, ""), (_PARA_RE, "\n\n"), (_LINE_RE, "\n")]

# 구조 경계가 **하나도 없는** 한 줄(표를 줄바꿈 없이 흘려 쓴 고시 등)을 모델 상한에 맞춰
# 마지막으로 자를 때 쓰는 자리. 선호 순서: 문장 끝 > 열거 기호 앞 > 공백 > (없으면) 글자 수.
# 여기까지 와도 글자는 절대 버리지 않는다 — 조각을 그대로 이어붙이면 원문이 복원된다.
_SENTENCE_END_RE = re.compile(r"[.。!?！？;][\s\"'”’)\]】]*")
_ENUM_MARKER_RE = re.compile(r"[①-⑳㉑-㉚]|(?<!\d)\d{1,3}\.|[가-힣]\.")
_WHITESPACE_RE = re.compile(r"\s+")
# (정규식, 자르는 위치가 매치 끝인가) — 열거 기호는 '앞'에서 잘라 다음 조각의 머리로 보낸다.
_HARD_CUTS = ((_SENTENCE_END_RE, True), (_ENUM_MARKER_RE, False), (_WHITESPACE_RE, True))


def _hard_cut_point(window: str, min_keep: int) -> int:
    """max_chars 짜리 창 안에서 자를 위치를 고른다. 못 찾으면 0(= 글자 수로 자르라)."""
    for pattern, at_end in _HARD_CUTS:
        cut = 0
        for match in pattern.finditer(window):
            position = match.end() if at_end else match.start()
            if min_keep <= position <= len(window):
                cut = position          # 창 안에서 가능한 한 뒤쪽 자리를 쓴다(조각을 꽉 채운다)
        if cut:
            return cut
    return 0


def _split_hard(text: str, max_chars: int) -> List[str]:
    """구조 경계가 없는 텍스트를 max_chars 이하 조각으로 **반드시** 자른다(최후 수단).

    되도록 문장 끝·열거 기호·공백에서 자르되, 그런 자리가 하나도 없으면 글자 수로 자른다.
    "문장 중간은 자르지 않는다"는 원칙보다 "모델이 말없이 버리게 두지 않는다"가 우선이다 —
    잘린 뒤쪽은 벡터에 아예 안 들어가 검색으로 되찾을 방법이 없기 때문이다.
    슬라이스만 하므로 반환 조각을 이어붙이면 원문과 정확히 같다(글자 손실 없음)."""
    max_chars = max(1, int(max_chars))
    chunks: List[str] = []
    start, length = 0, len(text)
    while length - start > max_chars:
        cut = _hard_cut_point(text[start:start + max_chars], max(1, max_chars // 2))
        if not 0 < cut <= max_chars:
            cut = max_chars
        chunks.append(text[start:start + cut])
        start += cut
    if start < length:
        chunks.append(text[start:])
    return chunks or [text]


def enforce_char_limit(parts: List[str], max_chars: int = MAX_EMBED_CHARS) -> List[str]:
    """조각 목록에서 상한을 넘는 것만 더 잘라 **모든 조각이 max_chars 이하**임을 보장한다.

    구조 분할(split_prose·split_table_like)이 끝난 뒤 마지막에 한 번 돌린다. 상한 이하인
    조각은 손대지 않으므로 기존 청크 경계(=chunk_id)는 그대로다 — 상한을 넘겨 임베딩에서
    잘려 나가던 조각만 바뀐다."""
    limit = max(1, int(max_chars))
    out: List[str] = []
    for part in parts:
        out.extend(_split_hard(part, limit) if len(part) > limit else [part])
    return out


def max_content_chars(prefix_chars: int, max_chars: int = MAX_EMBED_CHARS) -> int:
    """search_text 접두 길이를 뺀, content 한 조각에 허용되는 문자 예산.

    search_text = 접두 + "\\n" + content 이므로 접두와 줄바꿈 1자를 예산에서 뺀다."""
    spent = prefix_chars + 1 if prefix_chars else 0
    return max(1, int(max_chars) - spent)


def clamp_search_prefix(prefix: str, max_chars: int = MAX_EMBED_CHARS,
                        min_content: int = MIN_CONTENT_CHARS) -> str:
    """접두가 임베딩 예산을 통째로 먹지 않도록 길이를 제한한다.

    접두는 문서명·종류·장 경로·조번호·제목인데 **장 경로만 길이 보장이 없다**(수집기 아웃라인
    파서가 장 제목 대신 장 본문을 통째로 넣은 문서가 실제로 있다 — 실측 최대 14,243자).
    이런 문서는 본문을 아무리 잘게 나눠도 접두만으로 상한을 넘어 본문이 벡터에 못 들어간다."""
    limit = max(1, int(max_chars) - max(0, int(min_content)))
    return prefix if len(prefix) <= limit else prefix[:limit].rstrip()


def _split_hierarchical(text: str, max_chars: int, level: int = 0) -> List[str]:
    """경계 우선순위(항>호>문단>줄)로 재귀 분할한다.

    각 레벨에서 그 경계로 조각내 max_chars 이하로 그리디하게 채운다. 조각 하나가 여전히 넘으면
    다음(더 촘촘한) 경계로 내려간다. 마지막(줄)까지 갔는데도 한 줄이 넘으면 그 줄은 그대로 둔다
    (문장 중간을 글자 수로 자르지 않기 위한 최후 방침)."""
    if len(text) <= max_chars:
        return [text]
    if level >= len(_BOUNDARIES):
        return [text]  # 더 쪼갤 경계 없음(한 줄이 너무 긺)
    pattern, joiner = _BOUNDARIES[level]
    pieces = [p for p in pattern.split(text) if p and p.strip()]
    if len(pieces) <= 1:
        return _split_hierarchical(text, max_chars, level + 1)  # 이 경계로 안 나뉘면 다음 경계

    chunks: List[str] = []
    current = ""
    for piece in pieces:
        candidate = f"{current}{joiner}{piece}" if current else piece
        if len(candidate) <= max_chars:
            current = candidate
            continue
        if current:
            chunks.append(current)
        if len(piece) <= max_chars:
            current = piece
        else:
            chunks.extend(_split_hierarchical(piece, max_chars, level + 1))
            current = ""
    if current:
        chunks.append(current)
    return chunks


def split_prose(content: str, max_chars: int = MAX_CHUNK_CHARS, *,
                hard_max_chars: int = MAX_EMBED_CHARS) -> List[str]:
    """조문·부칙처럼 문단으로 구성된 내용을 법 구조 경계(항>호>문단>줄)로 나눈다.

    max_chars 이하면 그대로 1개. 초과하면 항 단위로 먼저 묶고, 항이 너무 길면 호, 그래도 길면
    문단·줄 순으로 내려간다 — 문장·항·호 중간은 자르지 않는다.

    hard_max_chars 는 **양보 없는** 모델 상한이다. 구조 경계가 없어 max_chars 를 못 지킨
    조각이라도 이 상한은 넘기지 않는다(넘으면 임베딩이 뒤를 말없이 버린다)."""
    parts = [content] if len(content) <= max_chars else (_split_hierarchical(content, max_chars) or [content])
    return enforce_char_limit(parts, hard_max_chars)


def _split_lines(text: str, max_chars: int) -> List[str]:
    """줄 경계로만 나눈다(문장 내부는 자르지 않음). 한 줄 자체가 max_chars 를 넘으면 그 줄만
    별도 청크로 낸다(더 잘게 쪼개면 의미가 없어지는 표/서식류 원문일 가능성이 커서)."""
    lines = text.splitlines()
    chunks: List[str] = []
    current = ""
    for line in lines:
        candidate = f"{current}\n{line}" if current else line
        if len(candidate) <= max_chars or not current:
            current = candidate
        else:
            chunks.append(current)
            current = line
    if current:
        chunks.append(current)
    return chunks


def split_table_like(content: str, max_chars: int = MAX_CHUNK_CHARS, *,
                     hard_max_chars: int = MAX_EMBED_CHARS) -> List[str]:
    """별표·별지 API content(표·문자로 그린 표 포함)를 행 단위로 나눈다.

    max_chars 이하면 통째로 1개(작은 표는 안 쪼갠다). 초과하면 앞 _TABLE_HEADER_LINES 줄을
    표 제목/헤더로 보고 모든 조각에 반복하며, 나머지는 줄(행) 단위로 채워 나간다 — 행·셀
    중간을 글자 수로 끊지 않는다.

    hard_max_chars 는 split_prose 와 같은 뜻의 모델 상한이다(한 행이 상한을 넘으면 그 행만
    글자 수로 나뉜다)."""
    return enforce_char_limit(_table_parts(content, max_chars, hard_max_chars), hard_max_chars)


def _table_parts(content: str, max_chars: int, hard_max_chars: int) -> List[str]:
    """split_table_like 의 구조 분할 본체(모델 상한 강제는 호출자가 마지막에 한 번 한다)."""
    if len(content) <= max_chars:
        return [content]

    lines = content.splitlines()
    if len(lines) <= _TABLE_HEADER_LINES:
        return _split_lines(content, max_chars)

    header = lines[:_TABLE_HEADER_LINES]
    # 헤더 자체가 **모델 상한**의 절반을 넘으면 반복하지 않는다 — 반복했다가 상한에 걸려
    # 잘리면 조각이 통째로 '헤더만' 남고 정작 행 내용이 벡터에서 사라진다.
    # (검색 단위 max_chars 가 아니라 hard_max_chars 기준이다 — max_chars 기준으로 재면
    #  상한을 넘지도 않는 평범한 표의 헤더 반복까지 꺼져 기존 청크 경계가 흔들린다.)
    if len("\n".join(header)) + 1 >= max(2, hard_max_chars) // 2:
        return _split_lines(content, max_chars)
    body_lines = lines[_TABLE_HEADER_LINES:]
    header_text = "\n".join(header)

    chunks: List[str] = []
    current_lines = list(header)
    current_len = len(header_text)
    for line in body_lines:
        added_len = current_len + 1 + len(line)
        if added_len > max_chars and len(current_lines) > len(header):
            chunks.append("\n".join(current_lines))
            current_lines = list(header) + [line]
            current_len = len(header_text) + 1 + len(line)
        else:
            current_lines.append(line)
            current_len = added_len
    if current_lines:
        chunks.append("\n".join(current_lines))
    return chunks or [content]
