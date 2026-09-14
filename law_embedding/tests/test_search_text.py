from law_indexer.mapper import build_search_text


def test_search_text_combines_values_and_omits_empty_values():
    text = build_search_text("표본법", "법률", "", None, "제1조", "목적", "원문")
    assert text == "표본법\n법률\n제1조\n목적\n원문"
    assert "None" not in text


def test_search_text_is_separate_from_content():
    from pathlib import Path
    from law_indexer.mapper import load_law_json
    obj = load_law_json(Path(__file__).parent / "fixtures" / "law.json")[0]
    assert obj.content != obj.search_text
    assert obj.search_text.endswith(obj.content)

