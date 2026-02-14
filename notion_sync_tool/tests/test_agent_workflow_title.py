from __future__ import annotations

from notion_sync_tool.agent_workflow import AgentWorkflowService


def test_normalize_question_title_removes_reason_and_limits_length() -> None:
    text = "题目：四舍五入应用题；错因：审题不清"
    out = AgentWorkflowService._normalize_question_title(text)
    assert out == "四舍五入应用题"
    assert len(out) <= 12


def test_extract_question_title_from_source() -> None:
    svc = object.__new__(AgentWorkflowService)
    title = svc._extract_question_title_from_source(  # type: ignore[misc]
        {
            "property_text": "题干: 两位数乘两位数应用\n错因: 计算粗心",
            "plain_text": "",
        }
    )
    assert title == "两位数乘两位数应用"

