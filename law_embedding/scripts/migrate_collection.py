"""컬렉션 이관 — 기존 벡터를 재사용해 새 스키마 컬렉션으로 옮긴다.

`indexFilterable` 은 이미 있는 속성에 대해 바꿀 수 없다(Weaviate 제약). 스키마를 고치려면
새 컬렉션을 만들어 옮기는 수밖에 없는데, 임베딩을 다시 돌리면 비싸다. 여기서는 기존 객체를
**벡터까지 통째로** 파일로 빼둔 뒤 새 컬렉션에 그대로 넣는다(임베딩 호출 0회).

파일을 거치는 이유가 하나 더 있다 — 옛/새 컬렉션이 동시에 존재하면 메모리가 2배로 뛴다.
파일로 빼두면 **옛 컬렉션을 먼저 지우고** 넣을 수 있어 피크가 늘지 않는다(파일이 백업 역할).

    export  : 기존 컬렉션 → parquet (uuid + properties + vector)
    import  : parquet → 새 컬렉션 (SCHEMA 기준 생성 후 batch import)
    verify  : 건수 · 벡터 일치 · 필터 동작 확인

사용:
    python -m scripts.migrate_collection export --source law --out /tmp/law.parquet
    python -m scripts.migrate_collection import --source law --in /tmp/law.parquet --collection NEW
    python -m scripts.migrate_collection verify --source law --in /tmp/law.parquet --collection NEW
"""
import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from law_indexer.config import Settings                      # noqa: E402
from law_indexer.weaviate_store import SCHEMA, WeaviateStore  # noqa: E402

# parquet 컬럼: uuid(str) · vector(list[float]) · props(JSON 문자열)
#   properties 를 컬럼마다 펼치지 않고 JSON 하나로 두는 이유 — 스키마가 바뀌어도 파일이 안 깨진다.
_COLS = ("uuid", "vector", "props")


def _vec(obj):
    v = obj.vector
    return v.get("default") if isinstance(v, dict) else v


def _jsonable(value):
    """Weaviate 가 돌려준 값을 다시 넣을 수 있는 형태로 바꾼다.

    ⚠ date 속성은 읽을 때 datetime 으로 오는데, str() 하면 '2026-01-02 00:00:00+00:00' 이 되어
    Weaviate 가 거부한다(RFC3339 필요 — 가운데가 공백이 아니라 'T', 끝은 'Z'). 실제 운영
    컬렉션으로 이관을 시험했을 때 이것 때문에 배치가 전건 실패했다.
    """
    import datetime as _dt
    if isinstance(value, _dt.datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=_dt.timezone.utc)
        return value.astimezone(_dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    if isinstance(value, _dt.date):
        return value.strftime("%Y-%m-%dT00:00:00Z")
    if isinstance(value, dict):
        return {k: _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(v) for v in value]
    return value


def do_export(settings, source: str, out: Path, limit=None, batch_rows: int = 20000) -> dict:
    """스트리밍 export — 배치(기본 2만 행)마다 parquet row group 으로 흘려 써서 메모리를
    평탄하게 유지한다. ⚠ 종전엔 전량을 파이썬 리스트로 모았다가 한 방에 썼는데, 벡터
    26만 개 시점에 컨테이너 한도(16GiB)를 넘겨 OOM kill 로 조용히 죽었다(실측 —
    트레이스백조차 없음). 대형 컬렉션(수십만~수백만 청크)은 반드시 스트리밍이어야 한다."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    name = settings.collection_for(source)
    schema = pa.schema([("uuid", pa.string()),
                        ("vector", pa.list_(pa.float32())),
                        ("props", pa.string())])
    out.parent.mkdir(parents=True, exist_ok=True)
    tmp = out.with_name(out.name + ".tmp")
    uuids, vectors, props = [], [], []
    n = 0

    def _flush(writer):
        nonlocal uuids, vectors, props
        if not uuids:
            return
        writer.write_table(pa.table({"uuid": pa.array(uuids, pa.string()),
                                     "vector": pa.array(vectors, pa.list_(pa.float32())),
                                     "props": pa.array(props, pa.string())}, schema=schema))
        uuids, vectors, props = [], [], []

    with WeaviateStore(settings, source) as store:
        coll = store.client.collections.get(name)
        total = coll.aggregate.over_all(total_count=True).total_count
        with pq.ParquetWriter(str(tmp), schema, compression="zstd") as writer:
            for obj in coll.iterator(include_vector=True):
                v = _vec(obj)
                if not v:
                    continue                              # 벡터 없는 객체는 재임베딩 대상
                uuids.append(str(obj.uuid))
                vectors.append(list(v))
                props.append(json.dumps(_jsonable(obj.properties), ensure_ascii=False, default=str))
                n += 1
                if n % batch_rows == 0:
                    _flush(writer)
                    print(f"  … {n:,}/{total:,}", file=sys.stderr)
                if limit and n >= limit:
                    break
            _flush(writer)
    tmp.replace(out)
    return {"exported": n, "collection_total": total, "file": str(out),
            "size_mb": round(out.stat().st_size / 1024 ** 2, 1)}


def do_import(settings, source: str, src_file: Path, collection: str, batch_size=100) -> dict:
    import pyarrow.parquet as pq

    tbl = pq.read_table(src_file)
    rows = tbl.num_rows
    with WeaviateStore(settings, source) as store:
        created = store.create_collection(collection, recreate=False)
        coll = store.client.collections.get(collection)
        failed = 0
        with coll.batch.fixed_size(batch_size=batch_size) as batch:
            for i in range(rows):
                batch.add_object(
                    properties=json.loads(tbl["props"][i].as_py()),
                    vector=tbl["vector"][i].as_py(),
                    uuid=tbl["uuid"][i].as_py())
        failed = len(coll.batch.failed_objects)
        return {"rows": rows, "collection": collection, "created": created,
                "failed": failed, "count": coll.aggregate.over_all(total_count=True).total_count}


def do_verify(settings, source: str, src_file: Path, collection: str, sample=200) -> dict:
    import pyarrow.parquet as pq
    from weaviate.classes.query import Filter

    tbl = pq.read_table(src_file)
    with WeaviateStore(settings, source) as store:
        coll = store.client.collections.get(collection)
        cnt = coll.aggregate.over_all(total_count=True).total_count
        step = max(1, tbl.num_rows // sample)
        same = diff = missing = 0
        for i in range(0, tbl.num_rows, step):
            uid = tbl["uuid"][i].as_py()
            got = coll.query.fetch_object_by_id(uid, include_vector=True)
            if got is None:
                missing += 1
                continue
            a = [round(x, 6) for x in tbl["vector"][i].as_py()]
            b = [round(x, 6) for x in (_vec(got) or [])]
            same, diff = (same + 1, diff) if a == b else (same, diff + 1)
        # 새 스키마에서만 되는 필터가 실제로 동작하는지
        filt_ok = None
        gp = None
        for i in range(min(tbl.num_rows, 50)):
            p = json.loads(tbl["props"][i].as_py())
            if p.get("git_path"):
                gp = p["git_path"]
                break
        if gp:
            try:
                r = coll.query.fetch_objects(filters=Filter.by_property("git_path").equal(gp), limit=3)
                filt_ok = len(r.objects) > 0
            except Exception as e:
                filt_ok = f"실패: {type(e).__name__}"
        return {"file_rows": tbl.num_rows, "collection_count": cnt,
                "vector_same": same, "vector_diff": diff, "missing": missing,
                "git_path_filter": filt_ok, "sample_git_path": gp}


def main():
    ap = argparse.ArgumentParser(prog="migrate_collection")
    sub = ap.add_subparsers(dest="cmd", required=True)
    for c in ("export", "import", "verify"):
        p = sub.add_parser(c)
        p.add_argument("--source", required=True, choices=("law", "admrul", "schlpub"))
        p.add_argument("--limit", type=int)
        if c == "export":
            p.add_argument("--out", type=Path, required=True)
        else:
            p.add_argument("--in", dest="src", type=Path, required=True)
            p.add_argument("--collection", required=True)
    a = ap.parse_args()
    st = Settings.from_env()
    if a.cmd == "export":
        r = do_export(st, a.source, a.out, a.limit)
    elif a.cmd == "import":
        r = do_import(st, a.source, a.src, a.collection)
    else:
        r = do_verify(st, a.source, a.src, a.collection)
    print(json.dumps(r, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
