"""별표 본문이 라벨뿐("[별도1]")이면 is_file_only — 실체가 파일에만 있는 별표를 놓치지 않는다.

2026-10-22 운영 실측: admrul 631건 중 8건(교정기 규정 별도1~8)이 content="[별도N]" 뿐인데
수집기 원본의 _APPENDIX_PLACEHOLDER("자세한 내용은 … 다운로드")에 안 걸려 is_file_only=False 였다.
초기적재는 index-files 크롤링으로 파일이 들어갔지만 증분(package file record)에서는 통째로 빠진다.
수집기가 운영에 없으므로 이 판정은 lawgo_raw(복사본)에서 산다. 삭제 표기("[별표] 삭제 <…>")는
라벨 뒤에 글자가 있어 걸리지 않아야 한다 — 폐지된 별표는 파일로 메울 것이 없다.
"""
from law_indexer.lawgo_raw.common import _APPENDIX_LABEL_ONLY, build_appendices


def _unit(content, with_file=True):
    u = {"별표번호": "0001", "별표가지번호": "00", "별표구분": "별도", "별표제목": "지방교정청 旗", "별표내용": content}
    if with_file:
        u["별표서식파일링크"] = "/flDownload.do?flSeq=1"
        u["별표HWP파일명"] = "별도1_지방교정청 旗.hwp"
    return {"별표": {"별표단위": [u]}}


def test_label_only_content_is_file_only():
    for label in ("[별도1]", "[별표 1]", "[별표 2의2]", "〈별지 제1호 서식〉", "[별지 제3호서식]"):
        (ap,) = build_appendices(_unit(label))
        assert ap["is_file_only"] is True, label


def test_deleted_marker_is_not_file_only():
    for text in ("[별표] 삭제 <1995.7.15>", "[별지 1] 삭제 <1983.12.8>", "〈별지 제1호 서식〉삭제"):
        (ap,) = build_appendices(_unit(text))
        assert ap["is_file_only"] is False, text


def test_real_content_is_not_file_only():
    (ap,) = build_appendices(_unit("[별표 1] 경비등급별 수형자 처우 기준\n1. 개방처우급: …"))
    assert ap["is_file_only"] is False


def test_label_without_file_is_not_file_only():
    (ap,) = build_appendices(_unit("[별도1]", with_file=False))
    assert ap["is_file_only"] is False


def test_regex_exact_shapes():
    assert _APPENDIX_LABEL_ONLY.match("[별도8]")
    assert not _APPENDIX_LABEL_ONLY.match("[별도8] 교정 旗 배열 요령")
    assert not _APPENDIX_LABEL_ONLY.match("별도8")
