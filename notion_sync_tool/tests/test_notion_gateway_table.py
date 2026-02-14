from __future__ import annotations

from notion_sync_tool.notion_gateway import NotionGateway, _split_table_cells, markdown_to_notion_blocks


def test_split_table_cells_with_escaped_pipe() -> None:
    cells = _split_table_cells(r"| a\|b | c |")
    assert cells == ["a|b", "c"]


def test_markdown_table_children_are_nested_under_table_field() -> None:
    md = "| 列1 | 列2 |\n| --- | --- |\n| a | b |\n"
    blocks = markdown_to_notion_blocks(md)
    assert blocks[0]["type"] == "table"
    assert "children" not in blocks[0]
    assert len(blocks[0]["table"]["children"]) == 2


def test_build_last_edited_filter() -> None:
    assert NotionGateway._build_last_edited_filter(None) is None
    assert NotionGateway._build_last_edited_filter("   ") is None
    assert NotionGateway._build_last_edited_filter("2026-01-01T00:00:00.000Z") == {
        "timestamp": "last_edited_time",
        "last_edited_time": {"on_or_after": "2026-01-01T00:00:00.000Z"},
    }
