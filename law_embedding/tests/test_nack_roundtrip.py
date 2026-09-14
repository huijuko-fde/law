"""소비자 격리 → nack → 생산자 되돌림 왕복 테스트.

생산자는 sink 전달 성공 시점에 '발송 완료' 로 마킹하므로, 소비자가 package 를 조용히 격리하면
그 문서들이 다음 개정까지 색인되지 않는다. 이 파일은 그 구멍이 실제로 메워졌는지 본다:
소비자가 남긴 nack 을 생산자가 읽어 kind 에 맞게 되돌리는가.
"""
import json
from pathlib import Path

import pytest

from law_indexer import pipeline as P


def _write_package(path: Path, source="law", law_ids=("001",)):
    lines = [{"record_type": "package_header", "package_id": path.stem, "source": source}]
    lines += [{"record_type": "document", "op": "upsert", "law_id": i} for i in law_ids]
    lines.append({"record_type": "package_footer", "record_count": len(law_ids)})
    path.write_text("\n".join(json.dumps(r, ensure_ascii=False) for r in lines) + "\n",
                    encoding="utf-8")


def _read_nack(nack_dir: Path) -> dict:
    files = list(nack_dir.glob("*.nack.json"))
    assert len(files) == 1, f"nack 파일이 정확히 1개여야 한다: {files}"
    return json.loads(files[0].read_text(encoding="utf-8"))


def test_quarantine_writes_nack_with_law_ids(tmp_path):
    """격리하면 그 package 안의 law_id 목록이 nack 으로 남는다(생산자가 되돌릴 대상)."""
    pkg = tmp_path / "law-1.jsonl"
    _write_package(pkg, law_ids=("001627", "001628"))
    nack_dir = tmp_path / "nack"

    P._quarantine(pkg, tmp_path / "failed", pkg.name, [{"error": "파싱 실패"}],
                  permanent=True, nack_dir=nack_dir)

    record = _read_nack(nack_dir)
    assert record["kind"] == "permanent"
    assert record["source"] == "law"
    assert record["law_ids"] == ["001627", "001628"]
    assert not pkg.exists() and (tmp_path / "failed" / pkg.name).exists()


def test_retry_exhausted_is_marked_transient(tmp_path):
    """재시도 소진 격리는 permanent 와 구분된다 — 생산자가 재수집이 아니라 재발송으로 처리한다."""
    pkg = tmp_path / "law-2.jsonl"
    _write_package(pkg, law_ids=("002",))
    nack_dir = tmp_path / "nack"

    P._quarantine(pkg, tmp_path / "failed", pkg.name, [{"error": "Weaviate 연결 실패"}],
                  permanent=False, nack_dir=nack_dir)

    assert _read_nack(nack_dir)["kind"] == "transient_exhausted"


def test_nack_dir_absent_does_not_block_quarantine(tmp_path):
    """nack 을 못 남겨도 격리 자체는 진행한다(되돌림은 부가기능, 소비 루프를 막으면 안 된다)."""
    pkg = tmp_path / "law-3.jsonl"
    _write_package(pkg)
    P._quarantine(pkg, tmp_path / "failed", pkg.name, [{"error": "x"}], nack_dir=None)
    assert (tmp_path / "failed" / pkg.name).exists()


def test_consume_folder_nacks_when_weaviate_down(tmp_path, monkeypatch):
    """실제 소비 경로: Weaviate 가 계속 죽어 있으면 재시도를 소진한 뒤 nack 을 남긴다."""
    folder = tmp_path / "in"
    folder.mkdir()
    _write_package(folder / "law-4.jsonl", law_ids=("004",))

    class _Boom:
        def __init__(self, *a, **k):
            raise RuntimeError("connection refused")

    monkeypatch.setattr(P, "WeaviateStore", _Boom)
    settings = type("S", (), {"package_nack_dir": None})()

    for _ in range(P._MAX_CONSUME_ATTEMPTS):          # 임계까지 반복 sweep
        P.consume_folder(None, settings, folder, failed_dir=tmp_path / "failed")

    record = _read_nack(folder / "nack")
    assert record["kind"] == "transient_exhausted"
    assert record["law_ids"] == ["004"]
