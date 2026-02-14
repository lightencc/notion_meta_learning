from __future__ import annotations

from notion_sync_tool.agent_utils import coerce_confidence, parse_json_response


class _Part:
    def __init__(self, text: str) -> None:
        self.text = text


class _Content:
    def __init__(self, parts) -> None:  # type: ignore[no-untyped-def]
        self.parts = parts


class _Candidate:
    def __init__(self, content) -> None:  # type: ignore[no-untyped-def]
        self.content = content


class _Response:
    def __init__(self, text: str = "", candidates=None) -> None:  # type: ignore[no-untyped-def]
        self.text = text
        self.candidates = candidates or []


def test_parse_json_response_from_text() -> None:
    resp = _Response(text='{"a": 1}')
    assert parse_json_response(resp) == {"a": 1}


def test_parse_json_response_from_candidate_parts() -> None:
    resp = _Response(candidates=[_Candidate(_Content([_Part('{"ok": true}')]))])
    assert parse_json_response(resp) == {"ok": True}


def test_coerce_confidence_bounds() -> None:
    assert coerce_confidence(-1, default=0.5) == 0.0
    assert coerce_confidence(9, default=0.5) == 1.0
    assert coerce_confidence("x", default=0.5) == 0.5

