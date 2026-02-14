from __future__ import annotations

import json
from pathlib import Path
from dataclasses import dataclass
from typing import Any

from google import genai
from google.genai import types

from .agent_utils import coerce_confidence, parse_json_response


SYSTEM_PROMPT = """
你是一个小学数学知识库整理助手。
你会收到目标条目、模板、来源上下文和输出约束。

规则：
- 严格按模板结构输出 markdown。
- 只输出最终 markdown 内容，不要额外解释。
- 内容要可教学、可复习、可执行，避免空话。
- 若请求携带了文档附件，请优先依据附件内容生成。
- 必须给出“出处/来源”并引用已给出的来源标题或文档路径。
- 不得虚构题目或来源；信息不足时明确写“待补充”。
- 语气简洁，面向四年级数学学习场景。
""".strip()


@dataclass(slots=True)
class KnowledgeSuggestion:
    content_markdown: str
    confidence: float
    reasoning_summary: str
    source_refs: list[str]
    raw_response: dict[str, Any]
    uploaded_attachments: list[str]
    attachment_errors: list[str]


class KnowledgeContentAgent:
    def __init__(self, *, api_key: str, model: str, temperature: float = 0.2) -> None:
        self.client = genai.Client(api_key=api_key)
        self.model = model
        self.temperature = temperature

    def suggest(
        self, payload: dict[str, Any], *, attachment_paths: list[str] | None = None
    ) -> KnowledgeSuggestion:
        config = types.GenerateContentConfig(
            system_instruction=SYSTEM_PROMPT,
            temperature=self.temperature,
            response_mime_type="application/json",
            response_schema={
                "type": "object",
                "required": [
                    "content_markdown",
                    "confidence",
                    "reasoning_summary",
                    "source_refs",
                ],
                "properties": {
                    "content_markdown": {"type": "string"},
                    "confidence": {"type": "number"},
                    "reasoning_summary": {"type": "string"},
                    "source_refs": {"type": "array", "items": {"type": "string"}},
                },
            },
        )

        uploaded_files: list[Any] = []
        uploaded_names: list[str] = []
        upload_errors: list[str] = []
        contents: list[Any] = [json.dumps(payload, ensure_ascii=False)]
        for path in _normalize_attachment_paths(attachment_paths or []):
            try:
                uploaded = self.client.files.upload(file=path)
                uploaded_files.append(uploaded)
                uploaded_names.append(Path(path).name)
                contents.append(uploaded)
            except Exception as exc:  # noqa: BLE001
                upload_errors.append(f"{path}: {exc}")

        try:
            response = self.client.models.generate_content(
                model=self.model,
                contents=contents,
                config=config,
            )
        finally:
            for uploaded in uploaded_files:
                file_name = str(getattr(uploaded, "name", "") or "").strip()
                if not file_name:
                    continue
                try:
                    self.client.files.delete(name=file_name)
                except Exception:  # noqa: BLE001
                    pass

        data = parse_json_response(response)
        return KnowledgeSuggestion(
            content_markdown=str(data.get("content_markdown", "")).strip(),
            confidence=coerce_confidence(data.get("confidence"), default=0.0),
            reasoning_summary=str(data.get("reasoning_summary", "")).strip(),
            source_refs=[str(item).strip() for item in data.get("source_refs", []) if str(item).strip()],
            raw_response=data,
            uploaded_attachments=uploaded_names,
            attachment_errors=upload_errors,
        )


def _normalize_attachment_paths(paths: list[str]) -> list[str]:
    out: list[str] = []
    for item in paths:
        text = str(item or "").strip()
        if not text or text in out:
            continue
        out.append(text)
    return out
