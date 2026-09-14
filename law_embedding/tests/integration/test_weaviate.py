import os
import pytest

pytestmark = pytest.mark.skipif(os.getenv("RUN_WEAVIATE_INTEGRATION") != "1", reason="set RUN_WEAVIATE_INTEGRATION=1")


def test_weaviate_ready():
    from law_indexer.config import Settings
    from law_indexer.weaviate_store import WeaviateStore
    with WeaviateStore(Settings.from_env()) as store:
        assert store.health()
