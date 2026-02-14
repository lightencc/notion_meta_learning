from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from google import genai
from google.genai import types

from .agent_utils import coerce_confidence, parse_json_response


SYSTEM_PROMPT = """
You are an education knowledge-linking assistant.
You receive one math-error record and candidate records from resources, concepts, skills, mindsets,
and similar errors.

Rules:
- Choose exactly one most relevant item for each: resource, concept, skill, mindset.
- Choose up to 3 similar errors.
- If uncertain, still provide your best guess and lower confidence.
- If an ID is unavailable, return empty string for that field.
- Return strict JSON with only the requested keys.
- new_title must only contain question type + question stem.
- Do not include mistake reasons, analysis, reflection, answer steps, or correction notes in new_title.
- Keep title concise and specific (around 12 Chinese chars).
""".strip()


@dataclass(slots=True)
class AgentSuggestion:
    new_title: str
    resource_id: str | None
    concept_id: str | None
    skill_id: str | None
    mindset_id: str | None
    similar_error_ids: list[str]
    confidence: float
    reasoning_summary: str
    raw_response: dict[str, Any]


class GeminiMatcherAgent:
    def __init__(self, *, api_key: str, model: str, temperature: float = 0.2) -> None:
        self.client = genai.Client(api_key=api_key)
        self.model = model
        self.temperature = temperature

    def suggest(self, payload: dict[str, Any]) -> AgentSuggestion:
        config = types.GenerateContentConfig(
            system_instruction=SYSTEM_PROMPT,
            temperature=self.temperature,
            response_mime_type="application/json",
            response_schema={
                "type": "object",
                "required": [
                    "new_title",
                    "resource_id",
                    "concept_id",
                    "skill_id",
                    "mindset_id",
                    "similar_error_ids",
                    "confidence",
                    "reasoning_summary",
                ],
                "properties": {
                    "new_title": {"type": "string"},
                    "resource_id": {"type": "string"},
                    "concept_id": {"type": "string"},
                    "skill_id": {"type": "string"},
                    "mindset_id": {"type": "string"},
                    "similar_error_ids": {"type": "array", "items": {"type": "string"}},
                    "confidence": {"type": "number"},
                    "reasoning_summary": {"type": "string"},
                },
            },
        )

        response = self.client.models.generate_content(
            model=self.model,
            contents=json.dumps(payload, ensure_ascii=False),
            config=config,
        )

        data = parse_json_response(response)
        return AgentSuggestion(
            new_title=str(data.get("new_title", "")).strip(),
            resource_id=_optional_str(data.get("resource_id")),
            concept_id=_optional_str(data.get("concept_id")),
            skill_id=_optional_str(data.get("skill_id")),
            mindset_id=_optional_str(data.get("mindset_id")),
            similar_error_ids=[str(item).strip() for item in data.get("similar_error_ids", []) if str(item).strip()],
            confidence=coerce_confidence(data.get("confidence"), default=0.0),
            reasoning_summary=str(data.get("reasoning_summary", "")).strip(),
            raw_response=data,
        )


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None

