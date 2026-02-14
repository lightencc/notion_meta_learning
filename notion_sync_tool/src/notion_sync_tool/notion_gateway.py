from __future__ import annotations

import re
import time
from typing import Any

from notion_client import Client
from notion_client.errors import APIResponseError

from .logging_utils import get_logger


class NotionGateway:
    def __init__(self, token: str, retry_max: int = 5) -> None:
        self.client = Client(auth=token)
        self.retry_max = retry_max
        self._data_source_id_cache: dict[str, str] = {}
        self.logger = get_logger(self.__class__.__name__)

    def _call_with_retry(self, fn, *args, **kwargs) -> Any:
        delay = 0.5
        for attempt in range(self.retry_max):
            try:
                return fn(*args, **kwargs)
            except APIResponseError as exc:
                code = str(getattr(exc, "code", ""))
                retryable = code in {"rate_limited", "internal_server_error", "service_unavailable"}
                if not retryable or attempt == self.retry_max - 1:
                    self.logger.error(
                        "Notion API request failed: function=%s code=%s attempt=%s/%s retryable=%s",
                        getattr(fn, "__name__", "unknown"),
                        code,
                        attempt + 1,
                        self.retry_max,
                        retryable,
                    )
                    raise
                self.logger.warning(
                    "Notion API request retrying: function=%s code=%s attempt=%s/%s delay=%.2fs",
                    getattr(fn, "__name__", "unknown"),
                    code,
                    attempt + 1,
                    self.retry_max,
                    delay,
                )
                time.sleep(delay)
                delay *= 2

    def get_database(self, database_id: str) -> dict[str, Any]:
        return self._call_with_retry(self.client.databases.retrieve, database_id=database_id)

    def get_data_source(self, data_source_id: str) -> dict[str, Any]:
        return self._call_with_retry(self.client.data_sources.retrieve, data_source_id=data_source_id)

    def get_default_data_source_id(
        self, database_id: str, database_obj: dict[str, Any] | None = None
    ) -> str | None:
        cached = self._data_source_id_cache.get(database_id)
        if cached:
            return cached

        data = database_obj if database_obj is not None else self.get_database(database_id)
        data_sources = data.get("data_sources", [])
        if isinstance(data_sources, list):
            for item in data_sources:
                data_source_id = str(item.get("id", "")).strip()
                if data_source_id:
                    self._data_source_id_cache[database_id] = data_source_id
                    return data_source_id
        return None

    def get_database_properties(
        self, database_id: str, database_obj: dict[str, Any] | None = None
    ) -> tuple[dict[str, Any], str | None]:
        data = database_obj if database_obj is not None else self.get_database(database_id)
        props = data.get("properties")
        if isinstance(props, dict) and props:
            return props, None

        data_source_id = self.get_default_data_source_id(database_id, database_obj=data)
        if not data_source_id:
            return {}, None

        data_source = self.get_data_source(data_source_id)
        ds_props = data_source.get("properties")
        if isinstance(ds_props, dict):
            return ds_props, data_source_id
        return {}, data_source_id

    def query_database_all(
        self,
        database_id: str,
        page_size: int = 100,
        edited_after: str | None = None,
    ) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        data_source_id = self.get_default_data_source_id(database_id)
        query_filter = self._build_last_edited_filter(edited_after)
        if data_source_id:
            cursor: str | None = None
            while True:
                payload: dict[str, Any] = {"data_source_id": data_source_id, "page_size": page_size}
                if cursor:
                    payload["start_cursor"] = cursor
                if query_filter is not None:
                    payload["filter"] = query_filter
                resp = self._call_with_retry(self.client.data_sources.query, **payload)
                results.extend(resp.get("results", []))
                if not resp.get("has_more"):
                    break
                cursor = resp.get("next_cursor")
            return results

        # Backward compatibility for legacy SDKs that still expose databases.query.
        if hasattr(self.client.databases, "query"):
            cursor = None
            while True:
                payload = {"database_id": database_id, "page_size": page_size}
                if cursor:
                    payload["start_cursor"] = cursor
                if query_filter is not None:
                    payload["filter"] = query_filter
                resp = self._call_with_retry(self.client.databases.query, **payload)
                results.extend(resp.get("results", []))
                if not resp.get("has_more"):
                    break
                cursor = resp.get("next_cursor")
            return results

        raise RuntimeError(
            "Unable to query database rows: no data source found and SDK has no databases.query."
        )

    @staticmethod
    def _build_last_edited_filter(edited_after: str | None) -> dict[str, Any] | None:
        ts = (edited_after or "").strip()
        if not ts:
            return None
        return {
            "timestamp": "last_edited_time",
            "last_edited_time": {"on_or_after": ts},
        }

    def update_page_properties(self, page_id: str, properties: dict[str, Any]) -> dict[str, Any]:
        return self._call_with_retry(self.client.pages.update, page_id=page_id, properties=properties)

    def get_page(self, page_id: str) -> dict[str, Any]:
        return self._call_with_retry(self.client.pages.retrieve, page_id=page_id)

    def get_page_plain_text(self, page_id: str, max_chars: int = 1500, max_depth: int = 2) -> str:
        chunks: list[str] = []

        def visit_block_children(block_id: str, depth: int) -> None:
            if depth > max_depth or len("\n".join(chunks)) >= max_chars:
                return
            cursor: str | None = None
            while True:
                payload: dict[str, Any] = {"block_id": block_id, "page_size": 100}
                if cursor:
                    payload["start_cursor"] = cursor
                resp = self._call_with_retry(self.client.blocks.children.list, **payload)
                for block in resp.get("results", []):
                    text = _extract_block_text(block)
                    if text:
                        chunks.append(text)
                    if block.get("has_children"):
                        visit_block_children(block["id"], depth + 1)
                if not resp.get("has_more"):
                    break
                cursor = resp.get("next_cursor")

        visit_block_children(page_id, depth=0)
        out = "\n".join(chunks)
        return out[:max_chars]

    def list_block_children_all(self, block_id: str) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        cursor: str | None = None
        while True:
            payload: dict[str, Any] = {"block_id": block_id, "page_size": 100}
            if cursor:
                payload["start_cursor"] = cursor
            resp = self._call_with_retry(self.client.blocks.children.list, **payload)
            out.extend(resp.get("results", []))
            if not resp.get("has_more"):
                break
            cursor = resp.get("next_cursor")
        return out

    def clear_page_content(self, page_id: str) -> int:
        deleted = 0
        children = self.list_block_children_all(page_id)
        for block in children:
            block_id = str(block.get("id", "")).strip()
            if not block_id:
                continue
            self._call_with_retry(self.client.blocks.delete, block_id=block_id)
            deleted += 1
        return deleted

    def append_page_blocks(self, page_id: str, blocks: list[dict[str, Any]]) -> int:
        if not blocks:
            return 0
        appended = 0
        chunk_size = 50
        for i in range(0, len(blocks), chunk_size):
            chunk = blocks[i : i + chunk_size]
            self._call_with_retry(self.client.blocks.children.append, block_id=page_id, children=chunk)
            appended += len(chunk)
        return appended

    def replace_page_with_markdown(self, page_id: str, markdown: str) -> dict[str, int]:
        deleted = self.clear_page_content(page_id)
        blocks = markdown_to_notion_blocks(markdown)
        appended = self.append_page_blocks(page_id, blocks)
        return {"deleted_blocks": deleted, "appended_blocks": appended}


def _extract_block_text(block: dict[str, Any]) -> str:
    block_type = block.get("type")
    if not block_type:
        return ""
    data = block.get(block_type, {})
    rich = data.get("rich_text")
    if isinstance(rich, list):
        txt = "".join(item.get("plain_text", "") for item in rich).strip()
        if txt:
            return txt

    if block_type == "equation":
        expression = data.get("expression", "").strip()
        if expression:
            return expression

    if block_type == "child_page":
        title = data.get("title", "").strip()
        if title:
            return title

    return ""


def markdown_to_notion_blocks(markdown: str) -> list[dict[str, Any]]:
    text = (markdown or "").replace("\r", "\n").strip()
    if not text:
        return []

    blocks: list[dict[str, Any]] = []
    pending_para: list[str] = []
    pending_bullets: list[str] = []
    pending_numbers: list[str] = []

    def flush_paragraph() -> None:
        if not pending_para:
            return
        joined = " ".join(part.strip() for part in pending_para if part.strip()).strip()
        pending_para.clear()
        if not joined:
            return
        blocks.append(_text_block("paragraph", joined))

    def flush_bullets() -> None:
        if not pending_bullets:
            return
        items = pending_bullets[:]
        pending_bullets.clear()
        for item in items:
            blocks.append(_text_block("bulleted_list_item", item))

    def flush_numbers() -> None:
        if not pending_numbers:
            return
        items = pending_numbers[:]
        pending_numbers.clear()
        for item in items:
            blocks.append(_text_block("numbered_list_item", item))

    def flush_all() -> None:
        flush_paragraph()
        flush_bullets()
        flush_numbers()

    lines = text.split("\n")
    i = 0
    while i < len(lines):
        line = lines[i].strip()
        if not line:
            flush_all()
            i += 1
            continue

        if _is_markdown_table_line(line):
            j = i
            table_lines: list[str] = []
            while j < len(lines):
                candidate = lines[j].strip()
                if not _is_markdown_table_line(candidate):
                    break
                table_lines.append(candidate)
                j += 1
            parsed = _parse_markdown_table(table_lines)
            if parsed is not None:
                flush_all()
                blocks.append(parsed)
                i = j
                continue

        heading_match = re.match(r"^(#{1,3})\s+(.*)$", line)
        if heading_match:
            flush_all()
            level = len(heading_match.group(1))
            content = heading_match.group(2).strip()
            if content:
                block_type = f"heading_{level}"
                blocks.append(_text_block(block_type, content))
            i += 1
            continue

        bullet_match = re.match(r"^[-*]\s+(.*)$", line)
        if bullet_match:
            flush_paragraph()
            flush_numbers()
            pending_bullets.append(bullet_match.group(1).strip())
            i += 1
            continue

        number_match = re.match(r"^\d+\.\s+(.*)$", line)
        if number_match:
            flush_paragraph()
            flush_bullets()
            pending_numbers.append(number_match.group(1).strip())
            i += 1
            continue

        if line.startswith(">"):
            flush_all()
            quote_text = line.lstrip(">").strip()
            if quote_text:
                blocks.append(_text_block("quote", quote_text))
            i += 1
            continue

        pending_para.append(line)
        i += 1

    flush_all()
    return blocks[:100]


def _text_block(block_type: str, text: str) -> dict[str, Any]:
    rich_text = _parse_inline_rich_text(text)
    if not rich_text:
        rich_text = [_plain_rich_text("")]
    return {
        "object": "block",
        "type": block_type,
        block_type: {
            "rich_text": rich_text
        },
    }


def _parse_inline_rich_text(text: str) -> list[dict[str, Any]]:
    raw = (text or "").strip()
    if not raw:
        return []

    pattern = re.compile(
        r"(\[([^\]]+)\]\(([^)]+)\)|\*\*([^*]+)\*\*|__([^_]+)__|`([^`]+)`|~~([^~]+)~~|\*([^*\n]+)\*|_([^_\n]+)_)"
    )
    out: list[dict[str, Any]] = []
    cursor = 0
    for match in pattern.finditer(raw):
        start, end = match.span()
        if start > cursor:
            out.extend(_plain_rich_text_chunks(raw[cursor:start]))

        if match.group(2) is not None and match.group(3) is not None:
            out.extend(_plain_rich_text_chunks(match.group(2), link=match.group(3)))
        elif match.group(4) is not None:
            out.extend(_plain_rich_text_chunks(match.group(4), bold=True))
        elif match.group(5) is not None:
            out.extend(_plain_rich_text_chunks(match.group(5), bold=True))
        elif match.group(6) is not None:
            out.extend(_plain_rich_text_chunks(match.group(6), code=True))
        elif match.group(7) is not None:
            out.extend(_plain_rich_text_chunks(match.group(7), strikethrough=True))
        elif match.group(8) is not None:
            out.extend(_plain_rich_text_chunks(match.group(8), italic=True))
        elif match.group(9) is not None:
            out.extend(_plain_rich_text_chunks(match.group(9), italic=True))
        cursor = end

    if cursor < len(raw):
        out.extend(_plain_rich_text_chunks(raw[cursor:]))

    return out


def _plain_rich_text_chunks(
    text: str,
    *,
    bold: bool = False,
    italic: bool = False,
    strikethrough: bool = False,
    code: bool = False,
    link: str | None = None,
) -> list[dict[str, Any]]:
    raw = text or ""
    if not raw:
        return []
    out: list[dict[str, Any]] = []
    chunk_size = 1800
    start = 0
    while start < len(raw):
        chunk = raw[start : start + chunk_size]
        out.append(
            _plain_rich_text(
                chunk,
                bold=bold,
                italic=italic,
                strikethrough=strikethrough,
                code=code,
                link=link,
            )
        )
        start += chunk_size
    return out


def _plain_rich_text(
    text: str,
    *,
    bold: bool = False,
    italic: bool = False,
    strikethrough: bool = False,
    code: bool = False,
    link: str | None = None,
) -> dict[str, Any]:
    text_obj: dict[str, Any] = {"content": text}
    if link:
        text_obj["link"] = {"url": link}
    return {
        "type": "text",
        "text": text_obj,
        "annotations": {
            "bold": bold,
            "italic": italic,
            "strikethrough": strikethrough,
            "underline": False,
            "code": code,
            "color": "default",
        },
    }


def _is_markdown_table_line(line: str) -> bool:
    raw = (line or "").strip()
    if not raw:
        return False
    if "|" not in raw:
        return False
    return raw.startswith("|") or raw.endswith("|")


def _parse_markdown_table(lines: list[str]) -> dict[str, Any] | None:
    if len(lines) < 2:
        return None

    rows = [_split_table_cells(line) for line in lines]
    if len(rows) < 2:
        return None
    if not _is_markdown_table_separator(rows[1]):
        return None

    body_rows = [rows[0]] + rows[2:]
    if not body_rows:
        return None

    table_width = max(len(row) for row in body_rows)
    if table_width <= 0:
        return None
    if table_width > 20:
        table_width = 20

    children: list[dict[str, Any]] = []
    for row in body_rows:
        cells: list[list[dict[str, Any]]] = []
        normalized = row[:table_width] + ([""] * max(0, table_width - len(row)))
        for cell in normalized:
            rich = _parse_inline_rich_text(cell)
            cells.append(rich if rich else [_plain_rich_text("")])
        children.append(
            {
                "object": "block",
                "type": "table_row",
                "table_row": {"cells": cells},
            }
        )

    return {
        "object": "block",
        "type": "table",
        "table": {
            "table_width": table_width,
            "has_column_header": True,
            "has_row_header": False,
            "children": children,
        },
    }


def _split_table_cells(line: str) -> list[str]:
    raw = (line or "").strip()
    if raw.startswith("|"):
        raw = raw[1:]
    if raw.endswith("|"):
        raw = raw[:-1]

    out: list[str] = []
    buf: list[str] = []
    escaped = False
    for ch in raw:
        if escaped:
            buf.append(ch)
            escaped = False
            continue
        if ch == "\\":
            escaped = True
            continue
        if ch == "|":
            out.append("".join(buf).strip())
            buf.clear()
            continue
        buf.append(ch)
    out.append("".join(buf).strip())
    while len(out) > 1 and not out[0]:
        out.pop(0)
    while len(out) > 1 and not out[-1]:
        out.pop()
    return out


def _is_markdown_table_separator(cells: list[str]) -> bool:
    if not cells:
        return False
    for cell in cells:
        token = (cell or "").strip().replace(" ", "")
        if not re.fullmatch(r":?-{3,}:?", token):
            return False
    return True
