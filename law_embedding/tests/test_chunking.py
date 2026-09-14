from law_indexer.chunking import split_prose, split_table_like


def test_split_prose_returns_single_chunk_when_under_limit():
    assert split_prose("짧은 조문 내용", max_chars=100) == ["짧은 조문 내용"]


def test_split_prose_splits_on_paragraph_boundaries():
    para1 = "첫 번째 문단." * 5
    para2 = "두 번째 문단." * 5
    para3 = "세 번째 문단." * 5
    content = f"{para1}\n\n{para2}\n\n{para3}"
    max_chars = len(para1) + len(para2) + 5  # 두 문단까지만 한 조각에 들어가게
    chunks = split_prose(content, max_chars=max_chars)
    assert len(chunks) >= 2
    assert "".join(chunks).replace("\n\n", "") == content.replace("\n\n", "")  # 내용 손실 없음
    for c in chunks:
        assert len(c) <= max_chars or "\n\n" not in c  # 문단 하나가 넘는 경우 예외 허용


def test_split_prose_never_splits_a_paragraph_that_fits():
    """문단이 max_chars 이하면 그 문단 중간을 자르지 않는다."""
    para = "제1조(목적) 이 법은 표본을 정한다. " * 3
    content = f"{para}\n\n짧은 두 번째 문단"
    chunks = split_prose(content, max_chars=len(para) + 5)
    assert any(c.strip() == para.strip() or para.strip() in c for c in chunks)


def test_split_table_like_returns_single_chunk_when_small():
    table = "[표 제목]\n| 등급 | 조건 |\n| 1급 | ... |"
    assert split_table_like(table, max_chars=1000) == [table]


def test_split_table_like_repeats_header_across_chunks():
    header = "[표 제목]\n| 등급 | 조건 | 조치 |"
    rows = [f"| {i}급 | 조건{i} | 조치{i} |" for i in range(20)]
    content = header + "\n" + "\n".join(rows)
    max_chars = len(header) + 3 * 25  # 대략 3행씩만 들어가게
    chunks = split_table_like(content, max_chars=max_chars)
    assert len(chunks) > 1
    for chunk in chunks:
        assert chunk.startswith(header)  # 모든 조각에 헤더가 반복됨
    # 모든 행이 어딘가에는 살아있어야 한다(행 중간에 자르지 않았는지 확인)
    joined = "\n".join(chunks)
    for row in rows:
        assert row in joined


def test_split_table_like_never_cuts_a_row_in_half():
    header = "제목\n헤더"
    rows = ["가나다라마바사아자차카타파하" * 2 for _ in range(10)]
    content = header + "\n" + "\n".join(rows)
    chunks = split_table_like(content, max_chars=60)
    for chunk in chunks:
        for line in chunk.splitlines():
            assert line in (header.splitlines() + rows)  # 줄 자체가 항상 온전함
