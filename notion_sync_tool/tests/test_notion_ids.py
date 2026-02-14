from __future__ import annotations

from notion_sync_tool.notion_ids import normalize_notion_id


def test_normalize_notion_id_hyphenless_32_chars() -> None:
    raw = "eb1861e3dd9f4d058f66eed20405c5bb"
    assert normalize_notion_id(raw) == "eb1861e3-dd9f-4d05-8f66-eed20405c5bb"


def test_normalize_notion_id_preserves_non_32() -> None:
    assert normalize_notion_id(" abc ") == "abc"

