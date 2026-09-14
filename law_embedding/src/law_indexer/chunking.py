"""조문/부칙/별표 API content 청킹 정책(§5).

단순 글자 수로 아무데서나 자르지 않는다 — **법 구조 경계**를 우선한다:
조문이 최대 크기를 넘으면 항(①②) → 호(1.) → 문단(빈 줄) → 줄 순서로 내려가며 나눈다.
문장 중간은 절대 자르지 않는다. 표처럼 보이는 별표는 줄(행) 경계로만 나누고 헤더를 반복한다.
최대 크기 이하면 항상 1청크 그대로 둔다(대다수 조문·별표가 그렇다).

기본 최대 크기는 2,500자다. 예전엔 arctic-embed-l-v2.0-ko 의 학습 길이(1,300토큰)가 상한이라
어쩔 수 없는 값이었지만, bge-m3 는 8,192토큰까지 받으므로 지금은 **모델 제약이 아니라 검색
단위 선택**이다 — 한 청크가 커질수록 조문 하나를 정확히 집어내는 힘이 떨어진다. 늘리려면
CHUNK_MAX_CHARS 로 조절하되, 바꾸면 청크 경계가 달라져 chunk_id 가 전부 바뀐다(전량 재색인)."""
import os
import re
from typing import List

from dotenv import load_dotenv

load_dotenv()

# 검색 단위 기준(모델 상한 아님 — bge-m3 는 8,192토큰). CHUNK_MAX_CHARS 로 조절.
MAX_CHUNK_CHARS = int(os.getenv("CHUNK_MAX_CHARS", "2500"))
_TABLE_HEADER_LINES = 2  # 표 제목·헤더로 간주해 분할된 조각마다 반복할 앞줄 수

# 분할 경계 우선순위(위에서부터): 항(①②…) > 호(1. 2. …) > 문단(빈 줄) > 줄.
# 항/호 는 마커 '앞'에서 나눠(lookahead, 구분자 소비 안 함 → 조각을 그대로 이어붙이면 원문 복원).
# 문단·줄 은 구분자(\n\n·\n)를 소비하므로 그 구분자로 다시 이어붙인다.
_HANG_RE = re.compile(r"(?m)(?=^[ \t]*[①②③④⑤⑥⑦⑧⑨⑩⑪⑫⑬⑭⑮⑯⑰⑱⑲⑳㉑㉒㉓㉔㉕㉖㉗㉘㉙㉚])")
_HO_RE = re.compile(r"(?m)(?=^[ \t]*\d{1,3}\.[ \t])")
_PARA_RE = re.compile(r"\n[ \t]*\n")
_LINE_RE = re.compile(r"\n")
_BOUNDARIES = [(_HANG_RE, ""), (_HO_RE, ""), (_PARA_RE, "\n\n"), (_LINE_RE, "\n")]


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


def split_prose(content: str, max_chars: int = MAX_CHUNK_CHARS) -> List[str]:
    """조문·부칙처럼 문단으로 구성된 내용을 법 구조 경계(항>호>문단>줄)로 나눈다.

    max_chars 이하면 그대로 1개. 초과하면 항 단위로 먼저 묶고, 항이 너무 길면 호, 그래도 길면
    문단·줄 순으로 내려간다 — 문장·항·호 중간은 자르지 않는다."""
    if len(content) <= max_chars:
        return [content]
    return _split_hierarchical(content, max_chars) or [content]


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


def split_table_like(content: str, max_chars: int = MAX_CHUNK_CHARS) -> List[str]:
    """별표·별지 API content(표·문자로 그린 표 포함)를 행 단위로 나눈다.

    max_chars 이하면 통째로 1개(작은 표는 안 쪼갠다). 초과하면 앞 _TABLE_HEADER_LINES 줄을
    표 제목/헤더로 보고 모든 조각에 반복하며, 나머지는 줄(행) 단위로 채워 나간다 — 행·셀
    중간을 글자 수로 끊지 않는다."""
    if len(content) <= max_chars:
        return [content]

    lines = content.splitlines()
    if len(lines) <= _TABLE_HEADER_LINES:
        return _split_lines(content, max_chars)

    header = lines[:_TABLE_HEADER_LINES]
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
