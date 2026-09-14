"""`index` 계열이 도는 **데이터 레포**를 고르는 규칙(_index_targets).

레포 3개(LAW·ADMRUL·SCHLPUBRUL) = 컬렉션 3개(law/admrul/schlpub), source 1:1.
(2컬렉션 시절엔 `--source admrul` 이 ADMRUL+SCHLPUBRUL 둘을 걸었지만, 이제 학칙공단은
`--source schlpub` 로 자기 컬렉션에 들어간다. 다른 source 레포 밑 경로를 콕 집으면
엉뚱한 컬렉션에 조용히 넣지 않고 즉시 에러를 낸다.)
"""
from pathlib import Path

import pytest

from law_indexer.cli import _index_targets
from law_indexer.config import Settings


@pytest.fixture
def settings(tmp_path, monkeypatch):
    for name, sub in (("LAW", "LAW"), ("ADMRUL", "ADMRUL"), ("SCHLPUB", "SCHLPUBRUL")):
        path = tmp_path / sub
        path.mkdir()
        monkeypatch.setenv(f"{name}_REPO_PATH", str(path))
    return Settings.from_env()


def test_admrul_source_walks_only_admrul_repo(settings, tmp_path):
    """--source admrul --input 생략 = ADMRUL 만(학칙공단은 schlpub source 가 따로 돈다)."""
    roots = [root for _label, _url, root, _inp, _paths in _index_targets(settings, "admrul", None, None)]
    assert roots == [tmp_path / "ADMRUL"]


def test_schlpub_source_walks_schlpub_repo(settings, tmp_path):
    roots = [root for _label, _url, root, _inp, _paths in _index_targets(settings, "schlpub", None, None)]
    assert roots == [tmp_path / "SCHLPUBRUL"]


def test_law_source_walks_only_law_repo(settings, tmp_path):
    roots = [root for _label, _url, root, _inp, _paths in _index_targets(settings, "law", None, None)]
    assert roots == [tmp_path / "LAW"]


def test_input_under_schlpub_uses_schlpub_as_repo_root(settings, tmp_path):
    """--input 을 콕 집으면 그 경로를 품은 레포가 repo_root — 틀리면 git_path 가 어긋난다."""
    target = tmp_path / "SCHLPUBRUL" / "전남대학교 학칙"
    target.mkdir()
    targets = _index_targets(settings, "schlpub", target, None)
    assert len(targets) == 1
    _label, _url, repo_root, input_path, _paths = targets[0]
    assert repo_root == tmp_path / "SCHLPUBRUL"
    assert input_path == target


def test_input_under_other_sources_repo_raises(settings, tmp_path):
    """--source admrul 인데 SCHLPUBRUL 밑 경로 = 지정 실수 → 조용히 admrul 컬렉션에 넣지 않고 에러."""
    target = tmp_path / "SCHLPUBRUL" / "전남대학교 학칙"
    target.mkdir()
    with pytest.raises(ValueError, match="schlpub"):
        _index_targets(settings, "admrul", target, None)


def test_paths_file_groups_within_own_repo(settings, tmp_path):
    """--paths-file 은 자기 source 레포 안 경로들만 — 레포별 grouped 로 돈다."""
    a = tmp_path / "ADMRUL" / "가.json"
    a.write_text("{}", encoding="utf-8")
    grouped = {root: paths for _l, _u, root, _i, paths in _index_targets(settings, "admrul", None, [a])}
    assert grouped == {tmp_path / "ADMRUL": [a]}


def test_paths_file_crossing_repos_raises(settings, tmp_path):
    """--paths-file 에 다른 source 레포 경로가 섞이면 에러(컬렉션 오적재 방지)."""
    a = tmp_path / "ADMRUL" / "가.json"
    b = tmp_path / "SCHLPUBRUL" / "나.json"
    for one in (a, b):
        one.write_text("{}", encoding="utf-8")
    with pytest.raises(ValueError, match="schlpub"):
        _index_targets(settings, "admrul", None, [a, b])


def test_no_input_returns_none_input_for_recursive_walk(settings, tmp_path):
    """--input 생략 시 input 자리는 None — 호출부의 recursive 휴리스틱(target_input is None)이
    켜져야 레포 전체를 재귀 순회한다. 레포 경로를 채우면 최상위 glob 만 돌아 0건이 된다(회귀)."""
    targets = _index_targets(settings, "admrul", None, None)
    assert targets[0][3] is None
