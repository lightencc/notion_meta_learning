from __future__ import annotations

from typing import Any, Iterable


def first_title_property_name(schema_properties: dict[str, Any]) -> str:
    for prop_name, prop_schema in schema_properties.items():
        if prop_schema.get("type") == "title":
            return prop_name
    raise RuntimeError("No title property found in database schema")


def extract_title_from_page(page: dict[str, Any], title_property: str) -> str:
    prop = page.get("properties", {}).get(title_property, {})
    if prop.get("type") != "title":
        return ""
    return "".join(chunk.get("plain_text", "") for chunk in prop.get("title", [])).strip()


def property_plain_text(prop: dict[str, Any]) -> str:
    ptype = prop.get("type")
    if ptype == "title":
        return " ".join(chunk.get("plain_text", "") for chunk in prop.get("title", [])).strip()
    if ptype == "rich_text":
        return " ".join(chunk.get("plain_text", "") for chunk in prop.get("rich_text", [])).strip()
    if ptype == "number":
        value = prop.get("number")
        return "" if value is None else str(value)
    if ptype == "url":
        return (prop.get("url") or "").strip()
    if ptype == "select":
        sel = prop.get("select") or {}
        return (sel.get("name") or "").strip()
    if ptype == "multi_select":
        items = prop.get("multi_select") or []
        return " ".join((item.get("name") or "").strip() for item in items if item.get("name"))
    if ptype == "status":
        value = prop.get("status") or {}
        return (value.get("name") or "").strip()
    if ptype == "date":
        value = prop.get("date") or {}
        start = (value.get("start") or "").strip()
        end = (value.get("end") or "").strip()
        if start and end:
            return f"{start} {end}"
        return start
    if ptype == "email":
        return (prop.get("email") or "").strip()
    if ptype == "phone_number":
        return (prop.get("phone_number") or "").strip()
    if ptype == "checkbox":
        return "true" if prop.get("checkbox") else "false"
    if ptype == "formula":
        formula = prop.get("formula") or {}
        ftype = formula.get("type")
        if ftype == "string":
            return (formula.get("string") or "").strip()
        if ftype == "number":
            value = formula.get("number")
            return "" if value is None else str(value)
        if ftype == "boolean":
            return "true" if formula.get("boolean") else "false"
        if ftype == "date":
            value = formula.get("date") or {}
            return (value.get("start") or "").strip()
    if ptype == "relation":
        rel_items = prop.get("relation") or []
        return " ".join(item.get("id", "") for item in rel_items if item.get("id"))
    return ""


def extract_property_text(properties: dict[str, Any], skip_relation: bool = True) -> str:
    parts: list[str] = []
    for name, prop in properties.items():
        if skip_relation and prop.get("type") == "relation":
            continue
        text = property_plain_text(prop)
        if text:
            parts.append(f"{name}:{text}")
    return "\n".join(parts)


def extract_relation_rows(page: dict[str, Any]) -> Iterable[tuple[str, str, str]]:
    page_id = page["id"]
    for prop_name, prop in page.get("properties", {}).items():
        if prop.get("type") != "relation":
            continue
        for rel in prop.get("relation", []):
            target_id = rel.get("id")
            if target_id:
                yield (page_id, prop_name, target_id)

