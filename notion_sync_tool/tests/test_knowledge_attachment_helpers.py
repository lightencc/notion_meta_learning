from __future__ import annotations

from pathlib import Path

from notion_sync_tool.knowledge_agent import _normalize_attachment_paths
from notion_sync_tool.knowledge_workflow import _resolve_source_path


def test_normalize_attachment_paths_deduplicates_and_skips_empty() -> None:
    values = ["", "  ", "/a/b.pdf", "/a/b.pdf", " /a/c.docx "]
    assert _normalize_attachment_paths(values) == ["/a/b.pdf", "/a/c.docx"]


def test_resolve_source_path_prefers_base_dir_existing(tmp_path: Path) -> None:
    nested = tmp_path / "docs" / "lesson.pdf"
    nested.parent.mkdir(parents=True, exist_ok=True)
    nested.write_text("x", encoding="utf-8")

    resolved = _resolve_source_path("docs/lesson.pdf", base_dir=tmp_path)
    assert resolved == nested.resolve()
