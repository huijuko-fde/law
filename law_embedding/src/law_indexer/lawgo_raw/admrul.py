"""law.go.kr 본문 API 원본 응답 → 조문·부칙·별표 구조 (수집기에서 가져온 순수 변환 함수).

출처: temporal_law/collector/admrul.py (HEAD 8ef506d + 2026-10-22 작업 트리). **네트워크·환경 의존 없음.**

왜 복사했나: 외부 수집기(투라인)가 law.go.kr 응답을 **그대로** 배달하고, 우리 수집기(temporal_law)는
운영에 올라가지 않는다. 적재기가 raw 형식을 직접 이해해야 하고, 수집기는 더 발전하지 않으므로
복사본이 어긋날 일이 없다. 적재기가 파일명 규칙(_appendix_label·_safe_name)을 같은 이유로
재구현해 둔 것의 연장이다. 수집기 원본과의 동일성은 tests/test_lawgo_raw_equivalence.py 가
실제 배달 ZIP 2건으로 고정한다.
"""

import re
from . import common



_CHAPTER_TAIL_RE = re.compile(r"제\d+\s*[편장절관]")


_CITE_JOSA_RE = re.compile(
    r"(?:에|의|를|을|은|는|이|가|와|과|로|으로|부터|까지|및|또는|에서|에게)(?=[\s가-힣,.])"
    #   접속사·구두점은 **뒤에 조문 참조가 이어질 때만** 인용으로 본다.
    #   `제10조(보고의무) 및 제11조` = 인용 / `제50조의2(융합학과(부) 및 융합전공)` = 제목 일부.
    #   `제33조 <삭제, 2023.…>` 의 날짜 쉼표도 이 조건으로 걸러진다.
    r"|\s*(?:및|또는|그리고|내지)\s*제\s*\d+\s*조"
    r"|\s*[,ㆍ·]\s*제\s*\d+\s*조")


_OUTLINE_MARK_RE = re.compile(r"\d+-\d+-\d+\.")


_OUTLINE_MIN_MARKS = 10


_OUTLINE_MAX_ARTICLES = 5


_OUTLINE_MIN_LEN = 2000


DOC_KIND = {"admrul": "행정규칙", "school": "학칙", "pi": "공단정관", "public": "공공기관"}


def _seq_id(meta: dict) -> tuple[str, str]:
    seq = str(meta.get("행정규칙일련번호") or meta.get("mst") or "")
    aid = str(meta.get("행정규칙ID") or meta.get("ID") or "")
    return seq, aid


_ADMRUL_HDR = re.compile(r"(제\d+(?:-\d+)?조(?:의\d+)?)\s*(?:\(([^)]*)\)|<?\s*삭\s*제)")


def _is_citation_header(before: str) -> bool:
    """헤더 '제N조(제목)' 바로 앞이 법령명이면 조문 헤더가 아니라 인용이다.
    예: '「건설기술진흥법 시행령」 제59조(건설사업관리 등)' 의 제59조 → 새 조로 분할 금지.

    단 편/장/절/관 표제가 사이에 끼면 인용이 아니다 — 표제 끝 단어가 법령 접미와 겹치기 때문
    (예 '제3절 통신판매전문보험회사의 모집 및 모집비율기준제2-10조(…)' 의 '기준',
     '제3관 위험관리기준제7-5조(…)' 의 '기준'). 실제 인용은 법령명과 조문이 붙어 있어
    사이에 표제가 없다."""
    b = before.rstrip()
    tail = _CHAPTER_TAIL_RE.split(b)[-1] if _CHAPTER_TAIL_RE.search(b) else b
    if tail is not b and "」" not in tail:                 # 표제 뒤 = 조문 헤더(인용 아님)
        return False
    if b.endswith("」"):                                   # 「외부법」 제N조(…)
        return True
    m = re.search(r"([가-힣A-Za-z0-9()·ㆍ]+)$", b)          # 끝 어절이 법령 접미면 인용
    return bool(m and common._LAW_NAME_SUFFIX.search(m.group(1)))


_ADMRUL_BARE_HDR = re.compile(r"(제\d+(?:-\d+)?조(?:의\d+)?)(?!\s*\()(?=\s)")


def _split_bare_article(chunk: str):
    """표제 + 제목없는 조문이 붙어 온 덩어리를 (표제, 조번호, 조문텍스트) 로 분리.

    예 '제1장 총칙제1절 통칙제1-1조 이 규정은 …'
       → ('제1장 총칙제1절 통칙', '제1-1조', '제1-1조 이 규정은 …')
    표제로 시작하지 않으면 None(=인용이나 직전 조문 꼬리일 수 있어 손대지 않는다).
    이 분리가 없으면 제목없는 조문이 preamble 로 흘러 **내용까지 버려진다**(실측 확인).
    """
    if not common._CHAPTER_RE.match(chunk):
        return None
    for m in _ADMRUL_BARE_HDR.finditer(chunk):
        head = chunk[:m.start()].rstrip()
        if not head or not common._CHAPTER_RE.match(head):
            continue
        # 표제 마커와 조문 사이에 '표제 이름' 만 있어야 한다. 산문(목차형 지침의 '1-1-1. 이 지침은
        # … 제30조 및' 같은 인용문)이 끼면 헤더가 아니다 — 마지막 표제 마커 이후 간격으로 판정.
        last = None
        for cm in _CHAPTER_TAIL_RE.finditer(head):
            last = cm
        gap = head[last.end():] if last else head
        if len(gap) > 25 or any(ch in gap for ch in ".。「(①"):
            continue
        return head, m.group(1), chunk[m.start():].strip()
    return None


_CHAPTER_ONLY_MAX = 400


_CHAPTER_BODY_ONSET_RE = re.compile(
    r"\d+\s*[.)]"                     # 1. / 1)
    r"|\d+\s*[-.]\s*\d+"              # 1-1 / 1.1
    r"|제\s*\d+\s*(?:조|항|호|-)"      # 제1항 / 제1-1-1조
    r"|[①-⑳]"
    r"|[ㅇ○□▪▶◦]\s"
    r"|[ⅠⅡⅢⅣⅤⅥⅦⅧⅨⅩ]\s*[.)]"
    # '제1장 목적이 규정은 …' — 번호·기호 없이 곧바로 본문이 시작하는 유일한 관용구(자기지칭).
    #   '목적이'의 '이'가 조사라 표제 이름과 붙어 오지만, 표제 **이름**에 이 어구가 들어갈 일은 없다.
    r"|이\s*(?:규정|지침|고시|기준|예규|훈령|요령|규칙|세칙|내규|표준|계획|요강|안내서)(?:은|는|이)\s")


_CHAPTER_NAME_MAX = 60


def _is_chapter_only(text: str) -> bool:
    """청크 **전체**가 편/장/절/관 표제인가(= 본문이 이어 붙지 않았는가).

    표제에 `<img>` 가 여러 개 붙어 오는 문서가 있어(징수사무처리규정류) 길이는 이미지 태그를
    걷어낸 뒤 잰다.
    """
    if not common._CHAPTER_RE.match(text):
        return False
    if len(text) <= _CHAPTER_ONLY_MAX:
        return True
    return len(common.extract_images(text)[0].strip()) <= _CHAPTER_ONLY_MAX


def _chapter_prefix(text: str) -> str:
    """표제 + 본문이 한 덩어리로 온 청크에서 **앞쪽 표제 부분만** 잘라 낸다(없으면 '').

    '제1장 도로분야 교통시설안전진단제1절 일반사항1.1 목적이 장은 …'
      → '제1장 도로분야 교통시설안전진단제1절 일반사항'
    본문은 호출부가 통째로 보존한다. 이 함수는 계층(chapter) 메타데이터만 만든다.
    """
    if not common._CHAPTER_RE.match(text):
        return ""
    pos, end = 0, 0
    while True:
        m = _CHAPTER_TAIL_RE.match(text, pos)
        if not m:
            break
        nxt = _CHAPTER_TAIL_RE.search(text, m.end())
        name_end = min(nxt.start() if nxt else len(text), m.end() + _CHAPTER_NAME_MAX)
        onset = _CHAPTER_BODY_ONSET_RE.search(text, m.end(), name_end)
        if onset:                                     # 표제 이름 뒤에서 본문이 시작
            end = onset.start()
            break
        end = name_end
        if nxt and nxt.start() == name_end:           # 표제가 연달아 이어진다
            pos = nxt.start()
            continue
        break
    return text[:end].strip()


def _push_chapter_keep_body(text, chapters, append) -> None:
    """표제로 **시작**하지만 본문이 이어 붙은 청크: 표제는 계층에 반영하고 본문은 살린다.

    예전엔 이 청크를 통째로 `chapters.push` 하고 버려서 본문이 사라졌다(이 파일 상단 주석 참고).
    표제 부분을 본문에서 떼어내지 않고 그대로 두는 것은 의도적이다 — flat 본문에서는 표제가
    본문의 일부로 읽히고(법령 파서도 `chapters.push(seg)` 로 같은 처리를 한다), 떼어내면
    조문이 하나도 없는 문서에서 표제 텍스트가 어디에도 남지 않는다.
    """
    head = _chapter_prefix(text)
    if head:
        chapters.push(head)
    append(text)


def _take_chapter_head(head, chapters, append) -> None:
    """`_split_bare_article` 이 떼어낸 '표제 덩어리'를 계층에 반영한다.

    그 덩어리도 표제만이라는 보장이 없다 — 목차형 지침은
    '제1장 총칙제1절 지침의 목적1-1-1 이 지침은 …(본문 1,264자)… 제38조 …' 처럼 표제와 본문이
    섞여 온다(실측: 공동구 점용예정면적 산정기준에 관한 지침). 예전엔 이 덩어리를 통째로
    `chapters.push` 하고 버려서 본문이 chapter 문자열로 사라졌다.
    """
    if _is_chapter_only(head):
        chapters.push(head)
    else:
        _push_chapter_keep_body(head, chapters, append)


def build_admrul_articles(body_json: dict) -> list[dict]:
    items = common.listify(common.first_value_recursive(body_json, ["조문내용"]))
    articles, seen = [], set()
    cur = None
    chapters = common.ChapterPath()
    preamble = []

    def _new_article(no, title, content):
        nonlocal cur
        if no in seen:                                # 중복 조번호는 직전 조에 이어붙임
            if cur:
                cur["content"] += "\n" + content.strip()
            return
        seen.add(no)
        cur = {"article_no": no, "article_title": common.normalize_text(title or ""),
               "chapter": chapters.nearest, "chapter_path": chapters.path,
               "content": content.strip(),
               "enforcement_date": "", "relations": [], "images": []}
        articles.append(cur)

    def _append(txt):
        if cur:
            cur["content"] += "\n" + txt.strip()
        else:
            preamble.append(txt.strip())

    all_chunks = []
    for raw in items:
        text = common.normalize_text(common.flatten_text(raw))
        if not text:
            continue
        all_chunks.append(text)
        hdrs = list(_ADMRUL_HDR.finditer(text))
        if not hdrs:                                  # 제목괄호 헤더 없음
            bare = _split_bare_article(text)
            if bare:                                  # 표제 + 제목없는 조문
                bare_no, bare_seg = bare[1], bare[2]
                cur = None                            # 표제 앞부분은 직전 조문 꼬리가 아니다
                _take_chapter_head(bare[0], chapters, _append)
                _new_article(bare_no, "", bare_seg)
            elif _is_chapter_only(text):              # 편/장/절/관 표제(청크 전체가 표제)
                chapters.push(text); cur = None
            else:                                     # 항·호 등 이어지는 줄 / 표제+본문 flat 청크
                _push_chapter_keep_body(text, chapters, _append)
            continue
        # 첫 헤더 앞부분: 장 표제거나 직전 조문 꼬리
        pre = text[:hdrs[0].start()].strip()
        if pre:
            bare = _split_bare_article(pre)
            if bare:                                  # 표제 + 제목없는 조문(예 제1-1조 목적)
                bare_no, bare_seg = bare[1], bare[2]
                cur = None
                _take_chapter_head(bare[0], chapters, _append)
                _new_article(bare_no, "", bare_seg)
            elif _is_chapter_only(pre):
                chapters.push(pre); cur = None
            else:
                _push_chapter_keep_body(pre, chapters, _append)
        for i, h in enumerate(hdrs):
            no = h.group(1)
            title = h.group(2) if h.group(2) is not None else "삭제"   # 삭제 헤더는 제목을 '삭제'로
            seg = text[h.start(): (hdrs[i + 1].start() if i + 1 < len(hdrs) else len(text))]
            at_start = (h.start() == 0 and not pre)
            # 헤더 앞이 법령명이면(「외부법」 제N조(…)) 인용이지 조문헤더 아님 → 분할 금지.
            is_cite = _is_citation_header(text[:h.start()])
            # 헤더 뒤가 곧바로 조사면 인용이다 — '제48조의2(…)에 따라', '제25조부터'.
            #   조문 헤더는 제목괄호 뒤에 공백/항번호가 오고 조사가 바로 붙지 않는다.
            if not is_cite and _CITE_JOSA_RE.match(text[h.end():]):
                is_cite = True
            # 항목 시작 헤더이거나, 번호가 직전 조보다 크면(가지조문/순차) 새 조. 아니면 인용의 제목괄호 → 본문으로.
            if not is_cite and (at_start or cur is None
                                or common.article_sort_key(no) > common.article_sort_key(cur["article_no"])):
                _new_article(no, title, seg)
            else:
                _append(seg)
            # flat 본문은 조문 사이에 장 표제가 끼어 온다(`…목적.\n제2장 다음\n제9조(…)`).
            #   이 구간의 표제는 **다음 조문**에 적용되므로 여기서 계층에 반영한다.
            chapters.push(seg)

    all_text = "\n".join(all_chunks)
    if (len(articles) <= _OUTLINE_MAX_ARTICLES and len(all_text) > _OUTLINE_MIN_LEN
            and len(_OUTLINE_MARK_RE.findall(all_text)) >= _OUTLINE_MIN_MARKS):
        # 목차형 지침 → 조문 오검출 폐기, 본문 전량을 전문 1건으로 (내용 보존 + provision_id=#DOC)
        articles = [{"article_no": "전문", "article_title": "", "chapter": "",
                     "content": all_text.strip(), "enforcement_date": "",
                     "relations": [], "images": []}]
        preamble = []
    # [본문 유실 방지 — README §9 "조 번호 없는 본문도 텍스트가 있으면 전문 조문 1건으로 저장"]
    #   ⚠ 예전 조건은 `if not articles and preamble` 이었다. 그래서 조문이 **하나라도** 잡히면
    #     남은 preamble(조 번호 없는 본문)이 조용히 사라졌다 — 실측 표본 1,294문서 중 17문서
    #     14,363자(지방세관계법 운영 예규 10,539자, 헌법재판소 관사관리 내규 468자 등).
    #     본문 인용이 조문 헤더로 오검출되는 flat 문서에서는 이 구멍으로 본문 **전량**이 샌다.
    #   → 조문 유무와 무관하게, 남은 본문은 문서 앞머리 '전문' 조문 1건으로 보존한다.
    rescue = "\n".join(preamble).strip()
    if not articles and not rescue:
        rescue = all_text.strip()                     # 최후 방어선: preamble 조차 비어도 원문은 살린다
    if rescue:
        articles.insert(0, {"article_no": "전문", "article_title": "", "chapter": "",
                            "content": rescue, "enforcement_date": "", "relations": []})
    # 본문 속 <img>(사진·도표) → 인라인 이미지 URL 로 뽑고 content 는 '[그림]' 치환
    for a in articles:
        a["content"], a["images"] = common.extract_images(a["content"])
    return articles


def build_admrul_addenda(body_json: dict) -> list[dict]:
    bu = common.first_value_recursive(body_json, ["부칙"])
    if not isinstance(bu, dict):
        return []
    dates = common.listify(bu.get("부칙공포일자"))
    texts = common.listify(bu.get("부칙내용"))
    nos = common.listify(bu.get("부칙공포번호"))
    out = []
    for i in range(max(len(dates), len(texts), len(nos))):
        out.append({
            "promulgation_date": str(dates[i]) if i < len(dates) else "",
            "promulgation_no": str(nos[i]) if i < len(nos) else "",
            "content": common.extract_images(common.lines_text(texts[i]))[0] if i < len(texts) else "",
        })
    out.sort(key=lambda a: a["promulgation_date"])
    return out


def build_admrul_attachments(body_json: dict) -> list[dict]:
    att = common.first_value_recursive(body_json, ["첨부파일"])
    if not isinstance(att, dict):
        return []
    urls = common.listify(att.get("첨부파일링크"))
    names = common.listify(att.get("첨부파일명"))
    out = []
    for i in range(max(len(urls), len(names))):
        out.append({
            "name": str(names[i]) if i < len(names) else "",
            "url": str(urls[i]) if i < len(urls) else "",
        })
    return out

