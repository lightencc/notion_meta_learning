from __future__ import annotations


def normalize_notion_id(value: str) -> str:
    raw = (value or "").strip().replace("-", "")
    if len(raw) != 32:
        return (value or "").strip()
    return f"{raw[:8]}-{raw[8:12]}-{raw[12:16]}-{raw[16:20]}-{raw[20:]}"

