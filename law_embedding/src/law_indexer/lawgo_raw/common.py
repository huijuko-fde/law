"""law.go.kr 본문 API 원본 응답 → 조문·부칙·별표 구조 (수집기에서 가져온 순수 변환 함수).

출처: temporal_law/collector/common.py (HEAD 8ef506d + 2026-10-22 작업 트리). **네트워크·환경 의존 없음.**

왜 복사했나: 외부 수집기(투라인)가 law.go.kr 응답을 **그대로** 배달하고, 우리 수집기(temporal_law)는
운영에 올라가지 않는다. 적재기가 raw 형식을 직접 이해해야 하고, 수집기는 더 발전하지 않으므로
복사본이 어긋날 일이 없다. 적재기가 파일명 규칙(_appendix_label·_safe_name)을 같은 이유로
재구현해 둔 것의 연장이다. 수집기 원본과의 동일성은 tests/test_lawgo_raw_equivalence.py 가
실제 배달 ZIP 2건으로 고정한다.
"""

import re
from html import unescape
from datetime import datetime, timedelta, timezone



LAWGO = "https://www.law.go.kr"


KST = timezone(timedelta(hours=9))


def now_kst() -> str:
    return datetime.now(KST).isoformat(timespec="seconds")


def normalize_text(text: str) -> str:
    if not text:
        return ""

    return (
        unescape(str(text))
        .replace("　", " ")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&amp;", "&")
        .strip()
    )


_FULLWIDTH_TO_ASCII = {c: c - 0xFEE0 for c in range(0xFF01, 0xFF5F)}


def compact(text: str) -> str:
    """
    비교용 정규화.
    공백, 특수 공백 제거.
    """
    if not text:
        return ""

    return (
        unescape(str(text))
        .replace(" ", "")
        .replace("　", "")
        .replace("\n", "")
        .replace("\t", "")
        .replace("ㆍ", "").replace("·", "").replace("․", "").replace("‧", "").replace("・", "")  # 가운뎃점 변형 통일
        .translate(_FULLWIDTH_TO_ASCII)               # 전각 영숫자·괄호 → 반각(매칭키 통일)
        .strip()
    )


def first_value_recursive(obj, candidate_keys):
    """
    중첩 JSON에서 특정 key 후보 중 첫 값을 찾음.
    법제처 API 응답 필드명이 조금씩 달라질 수 있어서 방어적으로 처리.
    """
    if isinstance(obj, dict):
        for key in candidate_keys:
            if key in obj and obj[key] not in (None, ""):
                return obj[key]

        for value in obj.values():
            found = first_value_recursive(value, candidate_keys)
            if found not in (None, ""):
                return found

    elif isinstance(obj, list):
        for item in obj:
            found = first_value_recursive(item, candidate_keys)
            if found not in (None, ""):
                return found

    return ""


def listify(value):
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


def flatten_text(value) -> str:
    """문자열/중첩 리스트를 공백으로 이어붙인 한 줄 텍스트로.
    (목내용 등이 법령에 따라 문자열·리스트·리스트의 리스트로 와서 방어 처리)"""
    if value is None:
        return ""
    if isinstance(value, list):
        return " ".join(flatten_text(v) for v in value)
    return str(value)


def lines_text(value) -> str:
    out: list[str] = []

    def _c(v):
        if isinstance(v, list):
            for x in v:
                _c(x)
        elif v not in (None, ""):
            out.append(normalize_text(str(v)))

    _c(value)
    return "\n".join(out)


REF_CATEGORIES = {"법령", "행정규칙", "자치법규", "학칙공단"}


def build_ref_url(category: str, title: str, article_no: str | None = None) -> str:
    """
    카테고리 + 제목(+조문)으로 캐노니컬 한글 URL 생성.
    예) build_ref_url("법령", "자동차관리법", "제3조")
        -> https://www.law.go.kr/법령/자동차관리법/제3조
    """
    category = category if category in REF_CATEGORIES else "법령"
    title = normalize_text(title)

    url = f"{LAWGO}/{category}/{title}"

    if article_no:
        url = f"{url}/{article_no}"

    return url


def article_no_from_fields(jo_no, jo_branch) -> str:
    """
    조번호 + 조가지번호를 제N조 또는 제N조의M으로 변환.
    예:
    조번호 1, 조가지번호 2 -> 제1조의2
    조번호 132, 조가지번호 0 -> 제132조
    """
    jo_no = str(jo_no or "").strip()
    jo_branch = str(jo_branch or "").strip()

    if not jo_no:
        return ""

    try:
        base = int(jo_no)
    except ValueError:
        return jo_no

    if base == 0:                                # 조문번호 0 = '모든조문'(법령 전체) → 조 없음(법령단위 링크)
        return ""

    if not jo_branch or jo_branch in ("0", "00"):
        return f"제{base}조"

    try:
        branch = int(jo_branch)
        return f"제{base}조의{branch}"
    except ValueError:
        return f"제{base}조"


def article_sort_key(article_no: str) -> tuple[int, int]:
    """
    '제N조(의M)' / '제N-M조(의K)' 문자열을 정렬용 (N, M) 튜플로.
    제1조 < 제2조 < … < 제15조 < 제15조의2 < 제15조의3 순서.
    복합조(제N-M조: 감독규정·외국환거래규정류)는 (N, M*10000+K) 로 같은 N 안에서 순서 보존
    (제1-1조 < 제1-2조 < 제1-2조의2 < 제2-1조). 일반 조문은 (N, K) 로 기존과 동일.
    조문번호를 못 읽으면 맨 뒤로 보낸다.
    """
    match = re.match(r"제(\d+)(?:-(\d+))?조(?:의(\d+))?", article_no or "")
    if not match:
        return (10 ** 9, 0)
    n = int(match.group(1))
    mm = int(match.group(2)) if match.group(2) else 0
    k = int(match.group(3)) if match.group(3) else 0
    return (n, mm * 10000 + k)


def extract_article_text(jo: dict) -> str:
    """
    조문단위 1개에서 조문내용 -> 항내용 -> 호내용 -> 목내용 순서로 텍스트를 모은다.

    (이전 PoC는 '조내용' 키를 찾았지만 본문 JSON은 '조문내용/항내용/호내용/목내용'
     체계라서 본문이 비어있었음. 여기서 정상화.)
    """
    parts = []                                   # 조문 텍스트 조각 누적

    if not isinstance(jo, dict):
        return ""

    # 조문/항/호/목 '내용' 은 법령에 따라 문자열·리스트·리스트의 리스트로 와서
    # flatten_text 로 평탄화한 뒤 정규화한다(안 하면 "[['...']]" 가 그대로 박힘).
    head = normalize_text(flatten_text(jo.get("조문내용", "")))  # 조 제목/머리글
    if head:
        parts.append(head)

    for hang in listify(jo.get("항")):            # 항(①②…) 순회
        if not isinstance(hang, dict):
            continue

        hang_text = normalize_text(flatten_text(hang.get("항내용", "")))
        if hang_text:
            parts.append(hang_text)

        for ho in listify(hang.get("호")):        # 호(1.2.…) 순회
            if not isinstance(ho, dict):
                continue

            ho_text = normalize_text(flatten_text(ho.get("호내용", "")))
            if ho_text:
                parts.append(ho_text)

            for mok in listify(ho.get("목")):     # 목(가.나.…) 순회 — 호 밑에 달린 경우
                if not isinstance(mok, dict):
                    continue
                mok_text = normalize_text(flatten_text(mok.get("목내용", "")))
                if mok_text:
                    parts.append(mok_text)

        # 목이 '호' 가 아니라 '항' 바로 밑에 오는 응답도 있다(실측: 표본 12개 법에서
        # 항→목 231건 · 호→목 0건). 이걸 안 보면 목에만 들어있는 내용(주로 박스드로잉 표)이
        # 통째로 누락된다 — 소득세법·지방세법·상속세법 등에서 조문 본문이 크게 줄었다.
        # 어느 호에 속한 목인지는 응답에 없어(목 키 = 목번호/목내용뿐) 원문 위치를 복원할 수
        # 없으므로 그 항의 호들 뒤에 붙인다. 저장·임베딩이 '조 단위'라 조문 안에서의 순서보다
        # 내용 누락 여부가 중요하다는 판단.
        for mok in listify(hang.get("목")):
            if not isinstance(mok, dict):
                continue
            mok_text = normalize_text(flatten_text(mok.get("목내용", "")))
            if mok_text:
                parts.append(mok_text)

    return "\n".join(parts)


_CHAPTER_RE = re.compile(r"^제\d+\s*[편장절관]")


_CHAPTER_LEVEL = {"편": 0, "장": 1, "절": 2, "관": 3}


_CHAPTER_LEVEL_RE = re.compile(r"^제\d+\s*([편장절관])(?:\s*의\s*\d+)?(?:\s|$)")


class ChapterPath:
    """편 > 장 > 절 > 관 계층을 유지한다.

    예전엔 표제를 만날 때마다 단일 변수를 덮어써서 **가장 마지막 표제만** 남았다. 편/장/절이
    계층인 문서(범죄수사규칙·통합공고·은행업감독규정)는 조문이 어느 편·장에 속하는지 통째로
    잃는다(실측: 통합공고 조문의 chapter 가 전부 `제N절 …`, 장은 하나도 안 남음).
    `chapter` 는 종전대로 **가장 가까운 표제**를 주고, `chapter_path` 로 전체 경로를 함께 남긴다.
    """

    __slots__ = ("_levels",)

    def __init__(self):
        self._levels = {}

    def push(self, heading: str) -> None:
        # flat 본문(감독규정류)은 `제2편 …\n제1장 …\n제1절 …` 처럼 표제 여러 줄이 한 덩어리로 온다.
        #   통째로 넣으면 계층이 안 잡히므로 줄 단위로 처리한다.
        for line in str(heading or "").splitlines():
            # 표제에 본문 이미지가 붙어 오는 문서가 있다(`제2장  면    허<img id="…">`).
            #   태그를 그대로 두면 chapter_path 에 마크업이 실린다.
            line = extract_images(line)[0].strip()
            m = _CHAPTER_LEVEL_RE.match(line)
            if not m:
                continue
            lv = _CHAPTER_LEVEL[m.group(1)]
            self._levels = {k: v for k, v in self._levels.items() if k < lv}
            self._levels[lv] = line

    def _pushed(self, heading: str) -> bool:
        """테스트용 — 이 문자열이 표제로 인정되는가."""
        self.push(heading)
        return bool(self._levels)

    @property
    def nearest(self) -> str:
        return self._levels[max(self._levels)] if self._levels else ""

    @property
    def path(self) -> str:
        return " > ".join(self._levels[k] for k in sorted(self._levels))


def build_articles(body_json: dict) -> list[dict]:
    """
    본문 JSON 을 '조 단위' 리스트로 만든다. (search 팀이 본문을 재파싱하지 않도록)
    각 원소 = { article_no, article_title, chapter, content, relations(빈 배열) }.

    - 실제 조문(조문여부=='조문')만 단위로 만든다.
    - 편/장/절/관 표제(조문여부=='전문' 등)는 단위로 만들지 않고, 직전 '장'을
      기억해 뒤따르는 조문의 chapter 로 붙인다(원문 구조 보존).
    - relations 는 호출부에서 source_article_no 기준으로 채워 넣는다.
    """
    units = listify(first_value_recursive(body_json, ["조문단위"]))

    articles = []
    seen = set()                                  # 중복 조문번호 방지(첫 항목 유지)
    chapters = ChapterPath()
    preamble = []                                 # 조문 아닌 '전문' 단문(章/節 표제 제외) — 조문 없는 특수법 대비

    for jo in units:
        if not isinstance(jo, dict):
            continue

        # 조문 여부 판정: '조문' 이거나(표시 없을 때) 조문제목/항이 있으면 조문으로 본다
        is_article = (jo.get("조문여부") == "조문") or (
            not jo.get("조문여부") and (jo.get("조문제목") or jo.get("항"))
        )

        head = normalize_text(flatten_text(jo.get("조문내용", "")))

        if not is_article:                        # 편/장/절/관 표제면 현재 장 갱신만
            if _CHAPTER_RE.match(head):
                chapters.push(head)
            elif head:                            # 표제도 조문도 아닌 전문(관할구역변경·폐지법 등 단문 본문)
                preamble.append(extract_article_text(jo) or head)
            continue

        article_no = article_no_from_fields(jo.get("조문번호", ""), jo.get("조문가지번호", ""))
        if not article_no or article_no in seen:
            if not article_no and str(jo.get("조문번호", "")).strip() in ("0", "00", "000000"):
                content = extract_article_text(jo)
                if content:
                    preamble.append(content)
            continue
        content = extract_article_text(jo)
        if not content:
            continue

        seen.add(article_no)
        # `조문참고자료` = 이동/종전 안내 등 조문 상태 설명. 예전엔 이 필드를 아예 안 읽어서, API 가
        #   `조문내용` 을 헤더만("제5조") 주고 설명을 참고자료에만 담은 조문이 **본문 없는 조문**이 됐다
        #   (실측 1,323건. 예: 보호소년 등의 처우에 관한 법률 시행령 제5조 →
        #    조문참고자료 "[종전 제5조는 제2조로 이동 <2021.4.20>]").
        note = normalize_text(flatten_text(jo.get("조문참고자료", "")))
        body_only = content.replace(article_no, "", 1).strip()
        title_txt = normalize_text(jo.get("조문제목", ""))
        if title_txt:
            body_only = body_only.replace(f"({title_txt})", "", 1).strip()
        if note and not body_only:                # 헤더만 남은 조문 → 참고자료를 본문으로 보존
            content = f"{content} {note}".strip()
        art = {
            "article_no": article_no,
            "article_title": title_txt,
            "chapter": chapters.nearest,
            "chapter_path": chapters.path,
            "content": content,
            "enforcement_date": normalize_text(jo.get("조문시행일자", "")),  # 조별 시행일(참고)
            "relations": [],                      # 호출부에서 채움
        }
        if note:
            art["note"] = note                    # 원본 그대로도 보존(이동/종전 추적용)
        # 조문 단위 개정 메타 — API 가 주는데 예전엔 안 읽었다. 조문별 "언제·어떻게 바뀌었나" 를
        #   RAG/이력 조회에서 쓸 수 있게 보존한다(문서 단위 revision_type 과 별개).
        for _src, _dst in (("조문제개정유형", "revision_type"),
                           ("조문변경여부", "changed"),
                           ("조문키", "article_key")):
            _v = normalize_text(str(jo.get(_src, "") or ""))
            if _v:
                art[_dst] = _v
        moved = normalize_text(jo.get("조문이동이전", "")) or normalize_text(jo.get("조문이동이후", ""))
        if moved:
            art["moved"] = moved
        articles.append(art)

    # 번호 있는 조문이 하나도 없고 '전문' 단문만 있는 특수법(관할구역 변경·폐지법 등):
    # 그 전문을 조문 1건('전문')으로 담아 빈 payload 실패를 막는다. (일반 법령은 여기 안 걸림)
    if not articles and preamble:
        articles.append({
            "article_no": "전문",
            "article_title": "",
            "chapter": "",
            "chapter_path": "",
            "content": "\n".join(t for t in preamble if t),
            "enforcement_date": "",
            "relations": [],
        })

    # 본문 속 <img>(사진·도표)를 인라인 이미지 URL 로 뽑고 content 는 '[그림]' 으로 치환
    for a in articles:
        a["content"], a["images"] = extract_images(a["content"])

    return articles


_LAW_NAME_SUFFIX = re.compile(
    r"(법|법률|령|규칙|조례|규정|준칙|협정|특례|헌장|예규|훈령|고시|지침|약관|조약"
    r"|학칙|정관|내규|세칙|요령|기준)$"
)


_CIRCLED_NUM = {chr(0x2460 + i): i + 1 for i in range(20)}


_APPENDIX_KIND_RE = r"별표|별지|서식|부록|별첨|별도|양식|붙임|기타|부속서|부표|도식|부도"


_APPENDIX_REF_RE = re.compile(rf"(?<![가-힣])({_APPENDIX_KIND_RE})\s*(?:제\s*)?(\d+)\s*(?:호)?(?:\s*의\s*(\d+))?")


_APPENDIX_SYMBOL_REF_RE = re.compile(
    rf"(?<![가-힣])({_APPENDIX_KIND_RE})\s*(?:제\s*)?"
    rf"(?:[\[(（]?\s*)?([A-Za-z]{{1,3}}|[가-힣]|[ⅠⅡⅢⅣⅤⅥⅦⅧⅨⅩ]+|[①-⑳])(?:\s*[\])）])?\s*(?:호)?"
    rf"(?:\s*(?:의|-)\s*([A-Za-z0-9가-힣ⅠⅡⅢⅣⅤⅥⅦⅧⅨⅩ①-⑳]+))?"
    r"(?=\s|$|[),.\]]|에|을|를|의|와|과|에서|부터|까지)"
)


_APPENDIX_TITLE_KIND_RE = re.compile(
    rf"^\s*(?:\[|\()?\s*({_APPENDIX_KIND_RE})"
    rf"(?=\s*(?:제\s*)?(?:\d|[A-Za-z]|[가-힣]|[ⅠⅡⅢⅣⅤⅥⅦⅧⅨⅩ]))"
)


_ROMAN_NUMERALS = {
    "Ⅰ": "I", "Ⅱ": "II", "Ⅲ": "III", "Ⅳ": "IV", "Ⅴ": "V",
    "Ⅵ": "VI", "Ⅶ": "VII", "Ⅷ": "VIII", "Ⅸ": "IX", "Ⅹ": "X",
}


def appendix_kind_group(kind: str) -> str:
    """참조 매칭용 부속문서 그룹.

    `서식`은 독립 단위로도 오고 `[별지 제N호 서식]`처럼 별지의 표시형으로도 온다.
    kind 자체는 접지 않고, `_appendix_unit_from_text`에서 제목이 명시하는 별지 서식만 BJ 계열로 보정한다.
    """
    return compact(kind) or "별표"


_CIRCLED_TRANS = {ord(k): str(v) for k, v in _CIRCLED_NUM.items()}


def _circled_to_num(s: str) -> str:
    """라벨 속 원문자 번호(①〜⑳)를 아라비아 숫자로. `붙임①` → `붙임1`."""
    return s.translate(_CIRCLED_TRANS) if s else s


def _appendix_no_norm(value) -> str:
    # ⚠ isdigit() 은 원문자(①)에도 True 인데 int("①") 은 ValueError 다. 참조 파서가 ①〜⑳ 를
    #   허용하므로 `붙임 ①` 같은 라벨이 오면 여기서 터져 그 문서 수집이 **결정적으로** 실패했다
    #   (재시도해도 같은 곳에서 죽는다). 원문자는 숫자로 접고, 판정은 isdecimal() 로 한다.
    s = _circled_to_num(compact(value))
    return str(int(s)) if s.isdecimal() else s


def _appendix_branch_norm(value) -> str:
    s = _circled_to_num(compact(value))
    return "" if s in ("", "0", "00") else (str(int(s)) if s.isdecimal() else s)


def _appendix_token(value: str) -> str:
    """문자형 부속문서 키를 ID에 넣기 좋게 정규화한다. 숫자형 기존 ID는 건드리지 않는다."""
    s = compact(value).upper()
    for src, dst in _ROMAN_NUMERALS.items():
        s = s.replace(src, dst)
    return re.sub(r"[^0-9A-Z가-힣]+", "", s)


_APPENDIX_NO_STOPWORDS = {"및", "중", "또", "등", "내", "외", "그", "각", "이하", "상",
                          "과", "와", "의", "에", "을", "를", "은", "는", "이", "로"}


def parse_appendix_label(label: str) -> dict | None:
    """본문/앵커의 `별표1`, `별첨 A`, `붙임 가` 같은 표현을 공통 구조로 파싱한다."""
    text = normalize_text(label or "")
    m = _APPENDIX_SUBLABEL_RE.search(text)              # `별지 제3-1-가호` — 노드 쪽과 같은 우선순위
    if m:
        return {"kind": m.group(1), "no": m.group(2), "branch": m.group(3) or "",
                "sub": m.group(4), "link_text": normalize_text(m.group(0))}
    for regex in (_APPENDIX_REF_RE, _APPENDIX_SYMBOL_REF_RE):
        m = regex.search(text)
        if m:
            no = normalize_text(m.group(2))
            if no in _APPENDIX_NO_STOPWORDS:            # `별표 및 …` 의 '및' 은 번호가 아니다
                continue
            return {
                "kind": m.group(1),
                "no": no,
                "branch": m.group(3) or "",
                "link_text": normalize_text(m.group(0)),
            }
    return None


def _appendix_kind_from_title(title: str) -> str:
    """별표제목 앞머리에서 실제 부속문서 kind 를 추정한다.

    일부 응답은 `별표구분`을 넓게 `별표`로 주면서 제목에는 `부속서 1`, `별도 1`처럼 더 정확한
    단위명을 담는다. 제목이 명확할 때만 이 값을 써야 `별표1`과 `부속서1`이 같은 BYL0001 로
    충돌하지 않는다.
    """
    m = _APPENDIX_TITLE_KIND_RE.search(normalize_text(title or ""))
    return m.group(1) if m else ""


def build_addenda(body_json: dict) -> list[dict]:
    """
    본문 JSON 의 부칙단위 → [{promulgation_date, promulgation_no, addendum_key, content}].
    부칙내용은 문자열/중첩 리스트로 와서 줄 단위로 평탄화. 공포일자순 정렬.
    """
    out = []
    for bu in listify(first_value_recursive(body_json, ["부칙단위"])):
        if not isinstance(bu, dict):
            continue
        lines: list[str] = []

        def _collect(v):
            if isinstance(v, list):
                for x in v:
                    _collect(x)
            elif v not in (None, ""):
                lines.append(normalize_text(str(v)))

        _collect(bu.get("부칙내용", ""))
        out.append({
            "promulgation_date": str(bu.get("부칙공포일자", "") or ""),
            "promulgation_no": str(bu.get("부칙공포번호", "") or ""),
            "addendum_key": str(bu.get("부칙키", "") or ""),
            "content": extract_images("\n".join(lines))[0],   # 부칙 속 <img> 태그 정제
        })
    out.sort(key=lambda a: a["promulgation_date"])
    return out


_APPENDIX_PLACEHOLDER = re.compile(r"자세한\s*내용은.*(다운로드|주소를\s*복사)")


_APPENDIX_LABEL_ONLY = re.compile(r"^[\[〈<]\s*(별표|별지|서식|별도|부록|별첨|양식|붙임)[^\]〉>]{0,15}[\]〉>]$")


def to_abs_url(url) -> str:
    url = str(url or "").strip() if not isinstance(url, str) else url.strip()
    if not url:
        return ""
    if url.startswith("//"):
        return "https:" + url
    if url.startswith("/"):
        return LAWGO + url
    return url


_IMG_TAG_RE = re.compile(r"<img\b[^>]*?>", re.I)


_IMG_CLOSE_RE = re.compile(r"</\s*img\s*>", re.I)


_IMG_ID_RE = re.compile(r'\bid\s*=\s*["\']?(\d+)', re.I)


_IMG_SRC_RE = re.compile(r'\bsrc\s*=\s*["\']([^"\']+)', re.I)


def extract_images(text: str) -> tuple[str, list[dict]]:
    """텍스트 속 <img> 를 인라인 표시 가능한 이미지 URL 로 뽑고, 태그는 '[그림]' 으로 치환.
    반환: (치환된 텍스트, [{type:'image', url, seq}]). 다운로드가 아니라 그대로 열람 가능한 URL."""
    if not text or "<img" not in text.lower():
        return text, []
    images = []

    def _repl(m):
        tag = m.group(0)
        msrc = _IMG_SRC_RE.search(tag)
        mid = _IMG_ID_RE.search(tag)
        if msrc:
            url, seq = to_abs_url(msrc.group(1)), ""
        elif mid:
            seq = mid.group(1)
            url = f"{LAWGO}/LSW/flDownload.do?flSeq={seq}"
        else:
            return "[그림]"
        images.append({"type": "image", "url": url, "seq": seq})
        return "[그림]"

    text = _IMG_TAG_RE.sub(_repl, text)
    text = _IMG_CLOSE_RE.sub("", text)                 # 남은 닫는 태그 </img> 제거(내용 없는 void)
    return text, images


def _appendix_files(u: dict) -> list[dict]:
    files = []
    for link_key, ftype, name_key in (
        ("별표서식파일링크", "hwp", "별표HWP파일명"),
        ("별표서식PDF파일링크", "pdf", "별표PDF파일명"),
        ("별표서식이미지파일링크", "image", "별표이미지파일명"),
    ):
        urls = listify(u.get(link_key))                        # 문자열이면 [문자열], 리스트면 그대로
        names = listify(u.get(name_key))
        for i, raw in enumerate(urls):
            url = to_abs_url(raw)
            if url:
                files.append({
                    "type": ftype,
                    "url": url,                                # pdf/이미지는 브라우저에서 바로 열림
                    "name": normalize_text(str(names[i]) if i < len(names)
                                           else (str(names[0]) if names else "")),
                })
    return files


def build_appendices(body_json: dict, viewer: tuple | None = None) -> list[dict]:
    out = []
    for u in listify(first_value_recursive(body_json, ["별표단위"])):
        if not isinstance(u, dict):
            continue
        content = lines_text(u.get("별표내용", ""))
        content, byl_images = extract_images(content)     # 별표 본문 속 <img> → 인라인 이미지 URL
        files = _appendix_files(u)
        # 텍스트 내용 없이 파일에만 실체가 있는 경우(안내문구만 오거나 아예 빈 경우) 표시
        is_file_only = bool(files) and (
            not content.strip() or bool(_APPENDIX_PLACEHOLDER.search(content))
            or bool(_APPENDIX_LABEL_ONLY.match(content.strip()))
        )
        # 대표 파일링크(file_url): 브라우저에서 바로 열리는 pdf > image 우선, hwp(다운로드)는 마지막.
        #   각 별표/별지/서식은 자기 flSeq 파일이라 종류 혼동 없이 정확히 열림 → 가장 신뢰 가능한 링크.
        primary = ""
        for pref in ("pdf", "image", "hwp"):
            primary = next((f["url"] for f in files if f["type"] == pref), "")
            if primary:
                break
        no = normalize_text(flatten_text(u.get("별표번호", "")))       # 문자열/리스트 방어
        branch = normalize_text(flatten_text(u.get("별표가지번호", "")))
        title = normalize_text(u.get("별표제목", ""))
        raw_kind = normalize_text(u.get("별표구분", ""))
        # 제목으로 kind 를 덮어쓰는 건 **제목 라벨의 번호가 API 번호와 같을 때만** 허용한다.
        #   예전엔 번호 확인 없이 덮어써서 `양식산업발전법 시행규칙` 별표 1("**양식**장기본도의 작성방법…")
        #   처럼 제목 첫 단어가 kind 와 겹치면 kind=양식(FRM) 이 되고 본문의 `별표 1`(BYL) 과 영구 미물림
        #   이 됐다(실측 law 16 · admrul 156건).
        title_kind = _appendix_kind_from_title(title)
        if title_kind and compact(raw_kind) in ("", "별표"):
            _lab = parse_appendix_label(title)
            _api_no = _appendix_no_norm(u.get("별표번호", ""))
            _lab_no = _appendix_no_norm(_lab["no"]) if _lab else ""
            kind = title_kind if (_lab and _lab_no and _lab_no == _api_no) else raw_kind
        else:
            kind = raw_kind
        # 사이트 뷰어 링크: bylNo 만으론 '별표 vs 서식'(둘 다 같은 번호, 별표키 E/F 로만 구분)이나
        #   무번호(0000)를 특정 못 해 엉뚱하게 열림 → **번호 있는 '별표'에만** 뷰어 링크를 건다.
        #   별지·서식·부록·무번호는 위 file_url(정확) 로 연다.
        view_url = ""
        if viewer and viewer[2] and kind == "별표" and (no and no not in ("0000",)):
            endpoint, id_param, id_value = viewer
            view_url = (f"{LAWGO}/LSW/{endpoint}?{id_param}={id_value}"
                        f"&bylNo={no}&bylBrNo={branch or '00'}")
        out.append({
            "no": no,
            "branch": branch,                                  # 별표 가지번호(제1호의2 등)
            "kind": kind,                                      # 별표 / 별지 / 서식 / 부록
            "title": title,
            "enforcement_date": normalize_text(u.get("별표시행일자", "")),  # 별표 자체 시행일
            "content": content,
            "is_file_only": is_file_only,                       # True = 실체는 파일에만(content는 안내문/빈값)
            "view_url": view_url,                               # 사이트 뷰어(별표 페이지)
            "file_url": primary,                                # 대표 파일(절대 URL) — 하위호환
            "files": files,                                     # 전체 파일링크(hwp/pdf/이미지, 절대 URL)
            "images": byl_images,                               # 별표 본문 속 인라인 이미지 URL
        })
    return out


_UNIT_ART_RE = re.compile(r"제?(\d+)(?:-(\d+))?조(?:의(\d+))?$")


_UNIT_APX_RE = re.compile(rf"({_APPENDIX_KIND_RE})\s*제?\s*(\d+)\s*(?:호)?(?:\s*의\s*(\d+))?")


_APX_PREFIX = {
    "별표": "BYL", "별지": "BJ", "서식": "FORM", "부록": "BR",
    "별첨": "BC", "별도": "BD", "양식": "FRM", "붙임": "ATT",
    "기타": "ETC", "부속서": "ANN", "부표": "BT", "도식": "DIA", "부도": "MAP",
}


_UNIT_CODE_RE = re.compile(r"^([A-Z]+)(\d{4})(?:_(\w+))?$")


_APX_SAME_SPACE = {"BJ": ("FORM",), "FORM": ("BJ",)}


_SCHLPUB_TARGETS = {"school", "pi", "public", "SchlPubRul"}


_ORDIN_TARGETS = {"ordin", "ordinance", "자치법규", "조례"}


def _looks_ordinance_name(name: str | None) -> bool:
    """law.go.kr 카테고리가 흔들려도 이름 자체가 조례면 자치법규로 본다."""
    return compact(name or "").endswith("조례")


def ref_unit(label: str) -> str:
    """조문/부속문서 라벨 → unit 코드.

    예: '제90조'→JO0090, '제5조의2'→JO0005_02, '제4-36조'→JO0004-36,
    '제9-4조의2'→JO0009-04_02(복합조=감독규정류), '별표1'→BYL0001,
    '별지3'→BJ0003, '서식3'→FORM0003, '별첨1'→BC0001, '별도1'→BD0001,
    '부속서1'→ANN0001. 미상→DOC.
    """
    s = _circled_to_num(compact(label or ""))      # `붙임①` 처럼 원문자 번호로 오는 라벨 정규화
    m = _UNIT_ART_RE.fullmatch(s)
    if m:
        unit = "JO" + m.group(1).zfill(4)
        if m.group(2):                             # 복합조 제N-M조(감독규정류) — '_'(조의)와 구분해 '-' 사용
            unit += "-" + m.group(2).zfill(2)
        if m.group(3):                             # 조의K 가지조문
            unit += f"_{m.group(3).zfill(2)}"
        return unit
    m = _APPENDIX_SUBLABEL_RE.match(s)                 # `별지 제3-1-가호` — 노드 unit 과 같은 형태로
    if m:
        base = _APX_PREFIX[m.group(1)] + m.group(2).zfill(4)
        if m.group(3):
            base += "_" + m.group(3).zfill(2)
        return base + "-" + m.group(4)
    m = _UNIT_APX_RE.match(s)
    if m:
        return _APX_PREFIX[m.group(1)] + m.group(2).zfill(4) + (f"_{m.group(3).zfill(2)}" if m.group(3) else "")
    parsed = parse_appendix_label(label)
    if parsed and not _appendix_no_norm(parsed["no"]).isdecimal():
        return _apx_unit(parsed["kind"], parsed["no"], parsed["branch"])
    return "DOC"


def _appendix_kind_compatible(raw_kind: str, parsed_kind: str) -> bool:
    raw = appendix_kind_group(raw_kind)
    parsed = appendix_kind_group(parsed_kind)
    if raw == parsed:
        return True
    # API kind=서식 이지만 표시 제목이 `[별지 제N호 서식]`이면 사이트 링크도 별지로 건다.
    return raw == "서식" and parsed == "별지"


_APPENDIX_SUBLABEL_RE = re.compile(
    rf"({_APPENDIX_KIND_RE})\s*제\s*(\d+)(?:\s*-\s*(\d+))?\s*-\s*([가-하])\s*호")


def _leading_appendix_label(text: str) -> dict | None:
    """제목 **맨 앞**의 부속문서 라벨만 인정한다: `[별지 제1호 서식] …`, `별지 제1호서식 …`.

    제목 중간에 나오는 건 라벨이 아니라 **상호참조**다(예 "…봉급표(제5조 및 별표 1 관련)").
    그걸 라벨로 오인해 unit 을 만들면 서로 다른 별표가 한 ID 를 공유한다(실측 629건 충돌).
    """
    t = normalize_text(text or "").lstrip("[［ \t")
    if not t:
        return None
    m = _APPENDIX_SUBLABEL_RE.match(t)                  # 제3-1-가호 형태 먼저(더 구체적)
    if m:
        return {"kind": m.group(1), "no": m.group(2), "branch": m.group(3) or "",
                "sub": m.group(4), "link_text": normalize_text(m.group(0))}
    for regex in (_APPENDIX_REF_RE, _APPENDIX_SYMBOL_REF_RE):
        m = regex.match(t)                              # search 아니라 match — 선두 고정
        if m:
            return {"kind": m.group(1), "no": m.group(2), "branch": m.group(3) or "",
                    "sub": "", "link_text": normalize_text(m.group(0))}
    return None


def _appendix_unit_from_text(kind: str, *texts: str) -> str:
    for text in texts:
        parsed = parse_appendix_label(text or "")
        if parsed and _appendix_kind_compatible(kind, parsed["kind"]):
            return _apx_unit(parsed["kind"], parsed["no"], parsed["branch"])
    return ""


def _apx_unit(kind: str, no: str, branch: str, title: str = "", file_name: str = "",
              fallback_index: int | None = None) -> str:
    pre = _APX_PREFIX.get(compact(kind), "APX")
    n_raw = _appendix_no_norm(no)
    b_raw = _appendix_branch_norm(branch)
    # ⚠ **API 가 준 번호가 최우선**이다. 예전엔 제목/파일명 파싱 결과를 먼저 채택해서, 제목에 들어있는
    #   상호참조·이동안내를 번호로 오인했다(실측 1,145건, 그중 629건이 문서 안에서 provision_id 충돌):
    #     · `공무원보수규정` 별표 3·4·5·6·8·10… 제목이 "…봉급표(제5조 및 **별표 1** 관련)" → 전부 BYL0001
    #     · "[별지 제2호서식]으로 이동 <2015.10.20.>" → 이동 목적지 ID(BJ0002)를 훔침
    #     · "그 밖에 **별표에** 규정된 …"(무번호) → `BYL-에` 같은 쓰레기 unit
    #   제목 파싱은 **API 번호가 없을 때만** 보조로 쓴다.
    if n_raw and n_raw.isdecimal():
        # 제목 **선두** 라벨이 API 번호와 같은 번호를 말하면 그 라벨의 kind 를 쓴다
        #   (예 API kind=서식 + 제목 "[별지 제1호 서식]" → 사이트 표시대로 별지=BJ).
        #   선두가 아니거나 번호가 다르면(상호참조·이동안내) **API 값이 이긴다**.
        lab = _leading_appendix_label(title)
        if lab and _appendix_no_norm(lab["no"]) == n_raw and _appendix_kind_compatible(kind, lab["kind"]):
            pre = _APX_PREFIX.get(compact(lab["kind"]), pre)
            sub = (lab.get("sub") or "").strip()
            if sub:                                     # 제3-1-가호 → …-가 (같은 번호의 가/나 구분 보존)
                lab_br = _appendix_branch_norm(lab.get("branch") or "")
                if not lab_br or lab_br == b_raw:        # 라벨의 -M 이 API branch 와 일치할 때만
                    base = pre + n_raw.zfill(4)
                    if b_raw:
                        base += "_" + (b_raw.zfill(2) if b_raw.isdecimal() else _appendix_token(b_raw))
                    return base + "-" + sub
    else:
        text_unit = _appendix_unit_from_text(kind, title, file_name)
        if text_unit:
            return text_unit
    if n_raw.isdecimal() and n_raw:
        if not b_raw:
            return pre + n_raw.zfill(4)
        branch_unit = b_raw.zfill(2) if b_raw.isdecimal() else _appendix_token(b_raw)
        return pre + n_raw.zfill(4) + f"_{branch_unit}"
    if n_raw:
        n = _appendix_token(n_raw)
        b = _appendix_token(b_raw)
        return pre + f"-{n}" + (f"_{b}" if b else "")
    if fallback_index is not None:
        return pre + f"-IDX{fallback_index:04d}"
    return pre + "0000"


def _appendix_file_name(ap: dict) -> str:
    for f in ap.get("files") or []:
        if isinstance(f, dict) and f.get("name"):
            return normalize_text(f.get("name", ""))
    return ""


def _appendix_unit(ap: dict, fallback_index: int | None = None) -> str:
    return _apx_unit(
        ap.get("kind", ""),
        ap.get("no", ""),
        ap.get("branch", ""),
        ap.get("title", ""),
        _appendix_file_name(ap),
        fallback_index,
    )


def _file_seq_from_url(url: str) -> str:
    m = re.search(r"(?:[?&]flSeq=|/)(\d{5,})(?:$|[&#?/])", str(url or ""))
    return m.group(1) if m else ""


def id_prefix(doc_target: str | None, fallback: str = "law") -> str:
    """payload doc_target/API target → relation ID prefix.

    저장·조회용 doc_target 은 원본 그대로 보존하지만, relation 이 서로 물리는 ID prefix 는 큰 축으로
    접는다. 학칙/공단정관/공공기관은 law.go.kr 링크에서 세부 target 이 모호하게 들어와 `schlpub`으로
    통일하고, 조례/자치법규는 `ordin`으로 분리한다.
    """
    raw = compact(doc_target or fallback)
    if raw in ("", "eflaw", "law", "법령"):
        return "law"
    if raw in _SCHLPUB_TARGETS:
        return "SchlPubRul"
    if raw in _ORDIN_TARGETS:
        return "ordin"
    if raw == "admrul" or raw == "행정규칙":
        return "admrul"
    return raw or fallback


def _category_prefix(category: str | None, fallback: str = "law") -> str:
    """relation target_category → reference_id prefix."""
    cat = compact(category or "")
    if cat in ("행정규칙", "admrul"):
        return "admrul"
    if cat in ("학칙공단", "학칙", "공단정관", "공공기관", "school", "pi", "public", "SchlPubRul"):
        return "SchlPubRul"
    if cat in ("자치법규", "조례", "ordin", "ordinance"):
        return "ordin"
    if cat in ("법령", "법률", "시행령", "시행규칙", "law", "eflaw"):
        return "law"
    return fallback


_KNOWN_CATEGORIES = {"법령", "행정규칙", "자치법규"}


_PREFIX_REPO = {"law": "LAW", "admrul": "ADMRUL", "SchlPubRul": "SCHLPUBRUL", "ordin": ""}


def predicted_git_path(prefix: str, target_name: str) -> tuple[str, str]:
    """관계 대상의 **예상** git 경로를 (레포, 상대경로)로. 코퍼스 인덱스 없이 계산한다.

    RAG 가 "벡터검색 → 애매하면 relation 따라가 원문 읽기" 를 하려면 relation 하나만으로 파일에
    닿아야 한다. reference_id 의 이름부는 compact(공백·가운뎃점 제거)라 폴더명으로 되돌릴 수 없지만,
    relation 은 `target_law_name`(원본 표기)을 갖고 있으므로 저장 규칙을 그대로 적용하면 된다.

    ⚠ **예상**이다. 확정이 아니다.
      · 대상 문서가 아직 수집 안 됐으면 그 경로에 파일이 없다(dangling — 소비자가 없으면 폴백).
      · 이름이 겹쳐 split 된 문서(실측 172그룹 453문서)는 한 단계 더 깊다
        (`{문서명}/{target}_{ID}/`). 그 경우 예상 경로에는 파일이 없어 조용히 빗나갈 뿐,
        **다른 문서를 가리키지는 않는다**(base 폴더엔 JSON 이 없다).
      정확한 경로는 전체 인덱스를 가진 enrich 단계가 `target_git_path` 로 덮어쓴다.
    """
    from . import mdexport                      # lazy — mdexport 는 common 을 import 하지 않음
    name = normalize_text(target_name or "")
    repo = _PREFIX_REPO.get(prefix, "")
    if not name or not repo:
        return "", ""
    safe = mdexport._safe(name)
    if prefix == "law":
        fam = mdexport._safe(mdexport.doc_family(name))
        return repo, f"{fam}/{mdexport.doc_kind_of(name)}/{safe}.json"
    return repo, f"{safe}/{safe}.json"


def _target_prefix(rel: dict, self_prefix: str, self_name_c: str) -> str:
    """관계 대상의 type 접두 결정."""
    rt = rel.get("relation_type"); dt = rel.get("delegation_type")
    target_name = compact(rel.get("target_law_name", ""))
    cat = compact(rel.get("target_category") or "")
    known = cat in _KNOWN_CATEGORIES
    if rt == "internal_ref":
        return self_prefix
    if rt == "appendix_ref":
        if target_name == self_name_c:
            return self_prefix
        if known:
            return _category_prefix(cat, "law")          # 확정 문서종 우선
        if _looks_ordinance_name(target_name):
            return "ordin"
        return _category_prefix(rel.get("target_category"), "law")
    if rt == "delegation":
        if known and cat != "자치법규" and not dt:
            return _category_prefix(cat, "law")
        if _looks_ordinance_name(target_name) and not known:
            return "ordin"
        if dt == "위임행정규칙":
            return "admrul"
        if dt == "위임자치법규":
            return "ordin"
        if dt == "위임학칙공단":
            return "SchlPubRul"
        return "law"                                    # 시행령/시행규칙
    if rt == "citation":
        if known:
            return _category_prefix(cat, "law")          # 확정 문서종 우선(이름 추측으로 덮지 않음)
        if _looks_ordinance_name(target_name):
            return "ordin"
        return _category_prefix(rel.get("target_category"), "law")
    return "law"


def enrich_file_meta(payload: dict) -> dict:
    """별표·첨부·본문이미지에 **git 저장 파일명과 레포 상대경로**를 부착한다(법령·행정규칙류 공용).

    payload(.json) · DB file_asset · git 다운로드가 같은 파일명을 공유하게 만드는 곳이다.
    이게 없으면 소비자(전처리·임베딩)가 url/kind/제목으로 로컬 경로를 **역산**해야 하고, 그 역산은
    실제로 상당 비율이 실패한다. 예전엔 이 함수가 law.py 에만 있어 **행정규칙·학칙공단 payload 에는
    filename 이 전혀 없었다**(샘플 실측: 별표 파일 2,079건 무기록 → 디스크 1,258개가 payload 에서 추적 불가).

    부착 필드
      · `filename`   : 디스크에 저장되는 실제 파일명(gitexport 와 동일 규칙)
      · `file_dir`   : 저장 폴더(별표/별지/서식/… · 첨부파일 · 본문이미지)
      · `local_path` : `{file_dir}/{filename}` = 데이터레포 문서 폴더 기준 상대경로
    (별표는 gitexport 가 `pick_file` 로 **대표 파일 1개**만 저장하므로 filename 도 대표 기준이다.)
    """
    from . import mdexport                      # lazy — mdexport 는 common 을 import 하지 않음
    for ap in payload.get("appendices") or []:
        if not isinstance(ap, dict):
            continue
        f = mdexport.pick_file(ap.get("files") or [])
        if f:
            ap["filename"] = mdexport.appendix_fname(ap, f)
            ap["file_dir"] = mdexport.appendix_dir(ap)
            ap["local_path"] = f'{ap["file_dir"]}/{ap["filename"]}'
        ap.setdefault("file_store_url", "")             # MINIO 붙으면 채움
        for one in ap.get("files") or []:
            if isinstance(one, dict):
                one.setdefault("file_store_url", "")
    for at in payload.get("attachments") or []:         # 행정규칙류 최상위 첨부(원문 등)
        if not isinstance(at, dict):
            continue
        if at.get("url"):
            # ⚠ 디스크에는 `mdexport._safe()` 로 살균된 이름으로 저장된다. 원본 이름을 그대로 적으면
            #   `개정안(2020/03).hwp` 처럼 금지문자가 든 첨부에서 payload 경로와 실제 파일이 어긋난다
            #   (`/` 는 경로 깊이까지 바꾼다). 저장 규칙과 같은 함수를 쓴다.
            at["filename"] = mdexport._safe(at.get("name") or "첨부파일.bin")
            at["file_dir"] = "첨부파일"
            at["local_path"] = f'{at["file_dir"]}/{at["filename"]}'
        at.setdefault("file_store_url", "")
    for a in (payload.get("body") or {}).get("articles") or []:
        if not isinstance(a, dict):
            continue
        for i, im in enumerate(a.get("images") or [], 1):
            if not isinstance(im, dict):
                continue
            im["filename"] = mdexport.image_fname(a.get("article_no"), i, im)
            im["file_dir"] = "본문이미지"
            im["local_path"] = f'{im["file_dir"]}/{im["filename"]}'
            im.setdefault("file_store_url", "")
    return payload


def attach_ref_ids(payload: dict) -> dict:
    """각 조/별표에 **provision_id**(본인 노드 ID), 각 관계에 **reference_id**(참조 대상 ID)와
    **source_provision_id**(어디서 = 출처 조/별표의 provision_id)를 부여. 같은 공식이라 서로 물림."""
    self_name_c = compact(payload.get("law_name", ""))
    prefix = id_prefix(payload.get("doc_target"), "law")
    if prefix == "SchlPubRul" and _looks_ordinance_name(self_name_c):
        prefix = "ordin"                               # 학칙공단 목록에 섞여온 조례(…조례) → 자치법규
    suffix = compact(payload.get("_schlpub_id_suffix") or "")
    doc = f"{prefix}:{self_name_c}"
    if suffix and prefix in ("SchlPubRul", "admrul"):
        # 이름이 겹치는 문서만 self ID 를 분리한다. 접미는 git 저장 폴더명과 같은 값이며
        #   다른 target 충돌이면 `public`, **같은 target 안 충돌**이면 `admrul_71712` 처럼
        #   문서 ID 가 들어간다(고시끼리는 target 이 같아 그것만으론 못 가른다).
        doc = f"{doc}:{suffix}"
    payload["provision_id"] = f"{doc}#DOC"             # 문서 자체(전체) — top-level
    payload["addendum_provision_id"] = f"{doc}#ADDENDUM"
    payload["amendment_provision_id"] = f"{doc}#AMENDMENT"
    article_provision_ids: dict[str, str] = {}
    # 문서가 실제로 보유한 부속문서 unit — self 참조의 별지/서식 별칭 해석에 쓴다.
    own_apx_units = {_appendix_unit(ap) for ap in payload.get("appendices", [])
                     if isinstance(ap, dict)}
    own_apx_by_unit: dict = {}                         # unit → 그 부속문서 dict(첫 항목)
    for _ap in payload.get("appendices", []) or []:
        if isinstance(_ap, dict):
            own_apx_by_unit.setdefault(_appendix_unit(_ap), _ap)
    own_apx_num: dict[tuple, set] = {}                 # (번호4, 가지) → {보유 prefix}
    for _u in own_apx_units:
        _m = _UNIT_CODE_RE.match(_u or "")
        if _m:
            own_apx_num.setdefault((_m.group(2), _m.group(3) or ""), set()).add(_m.group(1))

    def _alias_self_apx_unit(unit: str) -> str:
        """self 부속문서 참조 unit 을 **문서가 실제 보유한 unit** 으로 교정한다.

        정확 일치가 있으면 그대로. 없으면 같은 번호·가지를 가진 보유 항목이 **유일할 때만** 그것으로
        바꾼다(후보 2개 이상이면 모호 → 그대로 둔다). 사이트가 별지/서식·별표/부속서를 참조와 문서에서
        다르게 라벨링하는 것을 흡수하되, 추측은 하지 않는다.
        """
        if not unit or unit in own_apx_units:
            return unit
        m = _UNIT_CODE_RE.match(unit)
        if not m:
            return unit
        pre, num, br = m.group(1), m.group(2), m.group(3) or ""
        tail = num + (f"_{br}" if br else "")
        # ① 같은 갈래(별지↔서식): 사이트가 BF 하나로 묶는 확정 관계 → 있으면 바로 채택
        for alt in _APX_SAME_SPACE.get(pre, ()):
            if alt + tail in own_apx_units:
                return alt + tail
        # ② 유일후보: 본문이 부르는 kind 와 API `별표구분`이 다른 경우(본문 `부속서18` ↔ 문서 `별표18`,
        #   본문 `별표11` ↔ 문서 `별지11`). self 는 대상이 자기 자신이라 **보유 unit 전체를 이미 알고**
        #   있으므로, 같은 (번호,가지)를 가진 보유 부속문서가 **정확히 1개**일 때만 그것으로 교정한다.
        #   후보가 2개 이상이면(주세사무처리규정 별표3 → 별지3·부표3) 모호 → 손대지 않는다.
        #   ⚠ 과거 회귀 교훈: 이 규칙을 ①(확정 갈래) **대신** 쓰면 FORM0001·BYL0001 공존 문서에서
        #     확정 관계까지 모호 처리돼 물림이 떨어졌다(100%→98%). ① 뒤에 계층으로 둬야 안전하다.
        #     (enrich 의 _resolve_appendix_unit 와 동일한 계층 — self 는 인덱스 없이 수집 시점에 가능.)
        #   (-가/-나 하위라벨 unit 은 _UNIT_CODE_RE 에 매치되지 않아 위에서 이미 걸러진다.)
        cands = {p for p in own_apx_num.get((num, br), ()) if p != pre}
        if len(cands) == 1:
            cand_unit = next(iter(cands)) + tail
            if cand_unit in own_apx_units:
                return cand_unit
        return unit

    def assign_relation_id(r: dict, source_provision_id: str = "") -> None:
        """article relations/unmatched_relations 에 같은 공식으로 reference_id 를 부여한다."""
        if source_provision_id:
            r["source_provision_id"] = source_provision_id
        tname = compact(r.get("target_law_name", ""))
        # 이름이 '조례'로 끝나도, 문서종이 **데이터로 확정**돼 있으면 덮어쓰지 않는다(위 회귀 참고).
        if _looks_ordinance_name(tname) and compact(r.get("target_category") or "") not in _KNOWN_CATEGORIES:
            r["target_category"] = "자치법규"
            r["target_url"] = build_ref_url("자치법규", r.get("target_law_name", ""),
                                            r.get("target_article_no") or None)
        if r.get("appendix_ambiguous"):
            r["reference_id"] = ""
            return
        target_unit = r.get("target_unit")
        if r.get("relation_type") == "appendix_ref":
            if not target_unit:
                target_unit = _apx_unit(
                    r.get("appendix_kind", "") or r.get("target_category", ""),
                    r.get("appendix_no", ""),
                    r.get("appendix_branch", ""),
                    r.get("target_article_no", ""),
                    r.get("link_text", ""),
                )
            # 자기 문서 별표/별지는 **보유 unit 기준**으로 교정한다. 이미 target_unit 이 실려 있어도
            #   다시 확인한다 — gitexport·enrich 가 저장된 payload 로 attach_ref_ids 를 재실행하므로
            #   여기서 건너뛰면 옛 값(BJ0001)이 그대로 남아 FORM0001 과 안 물린다.
            if tname == self_name_c:
                target_unit = _alias_self_apx_unit(target_unit)
                # ⚠ appendix_found·title·url 은 도메인별 enrich 가 **attach_ref_ids 보다 먼저**
                #   정확일치로 판정한다 → 여기서 별칭 교정이 일어나면 `found=False` 인데 reference_id 는
                #   물리는 불일치가 남는다(실측 67건). 최종 unit 기준으로 한 곳에서 맞춘다.
                _ap = own_apx_by_unit.get(target_unit)
                if _ap is not None:
                    r["appendix_found"] = True
                    if not r.get("target_article_title"):
                        r["target_article_title"] = normalize_text(_ap.get("title", ""))
                    if not r.get("target_url"):
                        r["target_url"] = _ap.get("view_url") or _ap.get("file_url") or ""
            r["target_unit"] = target_unit
        else:
            target_unit = target_unit or ref_unit(r.get("target_article_no", ""))
        # 대상이 자기 문서면 self 문서의 머리(`doc`)를 그대로 쓴다.
        #   학칙공단 동명충돌로 `_schlpub_id_suffix` 가 붙은 문서는 조문 provision_id 가
        #   `SchlPubRul:이름:public#JO0005` 인데, 이름만으로 머리를 다시 조립하면 접미가 빠져
        #   **자기 자신과 영구 미물림**이 된다(접미 장치가 보호하려던 바로 그 문서들).
        if not tname:
            # target_law_name 없는 관계(이미 reference_id 만 들고 온 경우 포함) — 기존 id 는
            # 보존하고 추정 경로만 생략한다. 종전엔 id 를 "" 로 지우고 t_prefix 미정의로
            # 아래 predicted_git_path 에서 UnboundLocalError 가 났다.
            r.setdefault("reference_id", "")
            t_prefix = ""
        elif tname == self_name_c and _target_prefix(r, prefix, self_name_c) == prefix:
            r["reference_id"] = f"{doc}#{target_unit}"
            t_prefix = prefix
        else:
            t_prefix = _target_prefix(r, prefix, self_name_c)
            r["reference_id"] = f"{t_prefix}:{tname}#{target_unit}"
        # 관계 하나만으로 대상 원문 파일에 닿게 한다(전체 인덱스 없이 계산되는 예상 경로).
        if t_prefix:
            t_repo, t_path = predicted_git_path(t_prefix, r.get("target_law_name"))
            if t_path:
                r["target_repo"] = t_repo
                r["target_git_path_guess"] = t_path

    for a in payload.get("body", {}).get("articles", []):
        unit = ref_unit(a.get("article_no", ""))
        # '전문'처럼 조문 아닌 문서단위 조문 = 문서 그 자체 → 문서 #DOC 와 같은 provision_id.
        a["provision_id"] = payload["provision_id"] if unit == "DOC" else f"{doc}#{unit}"
        article_provision_ids[a.get("article_no", "")] = a["provision_id"]
        for r in a.get("relations", []):
            assign_relation_id(r, a["provision_id"])
    for r in payload.get("body", {}).get("unmatched_relations", []) or []:
        source_pid = article_provision_ids.get(r.get("source_article_no", "")) or payload["provision_id"]
        assign_relation_id(r, source_pid)
    units: list[str] = []
    for ap in payload.get("appendices", []):
        units.append(_appendix_unit(ap))
    unit_counts = {unit: units.count(unit) for unit in set(units)}
    for i, ap in enumerate(payload.get("appendices", []), 1):
        unit = units[i - 1]
        ap["provision_id"] = f"{doc}#{unit}"
        count = unit_counts.get(unit, 0)
        if count > 1:
            file_seq = ""
            for f in ap.get("files") or []:
                if isinstance(f, dict):
                    file_seq = _file_seq_from_url(f.get("url") or "")
                    if file_seq:
                        break
            ap["appendix_duplicate_count"] = count
            ap["appendix_instance_id"] = f"{doc}#{unit}:file:{file_seq}" if file_seq else f"{doc}#{unit}:idx:{i:04d}"
        else:
            ap.pop("appendix_duplicate_count", None)
            ap.pop("appendix_instance_id", None)
    for ad in payload.get("addenda", []):
        if isinstance(ad, dict):
            ad["provision_id"] = payload["addendum_provision_id"]
    return payload

