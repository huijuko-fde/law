from law_indexer.mapper import object_uuid


def test_uuid5_changes_only_with_chunk_id():
    assert object_uuid("same") == object_uuid("same")
    assert object_uuid("same") != object_uuid("different")

