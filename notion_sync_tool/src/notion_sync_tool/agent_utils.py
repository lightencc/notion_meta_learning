from __future__ import annotations

import json
from typing import Any


def parse_json_response(response: Any) -> dict[str, Any]:
    text = getattr(response, "text", "") or ""
    if text.strip():
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            pass

    candidates = getattr(response, "candidates", None) or []
    for cand in candidates:
        content = getattr(cand, "content", None)
        if not content:
            continue
        parts = getattr(content, "parts", None) or []
        for part in parts:
            ptext = getattr(part, "text", "") or ""
            if not ptext:
                continue
            try:
                return json.loads(ptext)
            except json.JSONDecodeError:
                continue

    raise RuntimeError("Gemini response is not valid JSON")


def coerce_confidence(value: Any, default: float) -> float:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return default
    if out < 0:
        return 0.0
    if out > 1:
        return 1.0
    return out

