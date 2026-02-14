from __future__ import annotations

from notion_sync_tool.postgres_store import (
    extract_relation_rows,
    extract_title_from_page,
    property_plain_text,
)


def test_extract_title_from_page() -> None:
    page = {
        "properties": {
            "名称": {"type": "title", "title": [{"plain_text": "测试标题"}]},
        }
    }
    assert extract_title_from_page(page, "名称") == "测试标题"


def test_property_plain_text_relation() -> None:
    prop = {"type": "relation", "relation": [{"id": "a"}, {"id": "b"}]}
    assert property_plain_text(prop) == "a b"


def test_extract_relation_rows() -> None:
    page = {
        "id": "p1",
        "properties": {
            "技能": {"type": "relation", "relation": [{"id": "s1"}, {"id": "s2"}]},
            "名称": {"type": "title", "title": [{"plain_text": "x"}]},
        },
    }
    assert list(extract_relation_rows(page)) == [("p1", "技能", "s1"), ("p1", "技能", "s2")]

