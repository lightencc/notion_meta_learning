from __future__ import annotations

import json
import re
from dataclasses import dataclass
from difflib import SequenceMatcher
from typing import Any

from .config import AppConfig
from .logging_utils import get_logger
from .notion_gateway import NotionGateway
from .notion_ids import normalize_notion_id
from .postgres_store import PostgresStore

_PLACEHOLDER_TITLE_PATTERNS = [
    re.compile(r"^\d{4}[-/]\d{1,2}[-/]\d{1,2}(?:\s*[-_#]?\s*\d+)?$"),
    re.compile(r"^\d{8}(?:[-_ ]\d+)?$"),
    re.compile(r"^\d{1,2}月\d{1,2}日(?:\s*[-_#]?\s*\d+)?$"),
    re.compile(r"^\d{4}年\d{1,2}月\d{1,2}日(?:\s*[-_#]?\s*\d+)?$"),
]


@dataclass(slots=True)
class EnrichOptions:
    dry_run: bool = True
    limit: int | None = None
    max_links_per_library: int = 3
    max_similar_links: int = 3
    similar_threshold: float = 0.35


@dataclass(slots=True)
class EnrichStats:
    scanned: int
    updated: int
    renamed: int
    relation_updates: int
    similar_updates: int


@dataclass(slots=True)
class Candidate:
    page_id: str
    title: str
    normalized: str


class ErrorEnrichmentService:
    TARGET_DBS = ("resources", "concepts", "skills", "mindsets")

    def __init__(self, config: AppConfig, gateway: NotionGateway, store: PostgresStore) -> None:
        self.config = config
        self.gateway = gateway
        self.store = store
        self.logger = get_logger(self.__class__.__name__)

    def run(self, options: EnrichOptions | None = None) -> EnrichStats:
        opts = options or EnrichOptions()
        self.logger.info(
            "Enrich started: dry_run=%s limit=%s max_links_per_library=%s max_similar_links=%s similar_threshold=%s",
            opts.dry_run,
            opts.limit,
            opts.max_links_per_library,
            opts.max_similar_links,
            opts.similar_threshold,
        )

        err_db_row = self.store.get_database_row("errors")
        if not err_db_row:
            raise RuntimeError("No 'errors' snapshot in PostgreSQL. Run sync first.")

        error_pages = [row for row in self.store.get_pages("errors") if int(row["archived"]) == 0]
        if opts.limit is not None:
            error_pages = error_pages[: opts.limit]

        error_schema = json.loads(err_db_row["schema_json"])
        title_prop_name = err_db_row["title_property"]
        relation_props = self._resolve_error_relation_properties(error_schema.get("properties", {}))
        existing_relations_map = self.store.get_relations_map("errors")

        target_candidates = {
            logical_db: self._build_candidates(logical_db) for logical_db in self.TARGET_DBS
        }
        error_features = {
            row["page_id"]: self._text_features(self._combine_page_text(row)) for row in error_pages
        }

        renamed = 0
        relation_updates = 0
        similar_updates = 0
        updated = 0

        for idx, page in enumerate(error_pages, start=1):
            page_id = page["page_id"]
            current_title = (page["title"] or "").strip()
            text = self._combine_page_text(page)
            normalized_text = self._normalize_for_match(text)

            page_relations = existing_relations_map.get(page_id, {})
            payload: dict[str, Any] = {}

            suggested_title = self._suggest_title(current_title=current_title, text=text)
            if suggested_title and suggested_title != current_title:
                payload[title_prop_name] = {
                    "title": [{"type": "text", "text": {"content": suggested_title}}]
                }
                renamed += 1

            for logical_db in self.TARGET_DBS:
                prop_name = relation_props.get(logical_db)
                if not prop_name:
                    continue

                predicted_ids = self._predict_related_ids(
                    normalized_text=normalized_text,
                    candidates=target_candidates[logical_db],
                    limit=opts.max_links_per_library,
                )
                if not predicted_ids:
                    continue

                old_ids = set(page_relations.get(prop_name, set()))
                merged_ids = sorted(old_ids | predicted_ids)
                if merged_ids != sorted(old_ids):
                    payload[prop_name] = {"relation": [{"id": pid} for pid in merged_ids]}
                    relation_updates += 1

            similar_prop = relation_props.get("errors")
            if similar_prop:
                predicted_similar = self._predict_similar_errors(
                    page_id=page_id,
                    all_error_features=error_features,
                    limit=opts.max_similar_links,
                    threshold=opts.similar_threshold,
                )
                if predicted_similar:
                    old_similar = set(page_relations.get(similar_prop, set())) - {page_id}
                    merged_similar = sorted((old_similar | predicted_similar) - {page_id})
                    if merged_similar != sorted(old_similar):
                        payload[similar_prop] = {"relation": [{"id": pid} for pid in merged_similar]}
                        similar_updates += 1

            if payload:
                updated += 1
                if opts.dry_run:
                    self.logger.info(
                        "Dry-run update candidate: page_id=%s title=%s fields=%s",
                        page_id,
                        current_title,
                        list(payload.keys()),
                    )
                else:
                    self.gateway.update_page_properties(page_id=page_id, properties=payload)
                    self.logger.info(
                        "Page updated: page_id=%s title=%s fields=%s",
                        page_id,
                        current_title,
                        list(payload.keys()),
                    )

            if idx % 50 == 0:
                self.logger.info("Enrich progress: scanned=%s total=%s", idx, len(error_pages))

        self.logger.info(
            "Enrich finished: scanned=%s updated=%s renamed=%s relation_updates=%s similar_updates=%s",
            len(error_pages),
            updated,
            renamed,
            relation_updates,
            similar_updates,
        )
        return EnrichStats(
            scanned=len(error_pages),
            updated=updated,
            renamed=renamed,
            relation_updates=relation_updates,
            similar_updates=similar_updates,
        )

    def _resolve_error_relation_properties(self, error_properties: dict[str, Any]) -> dict[str, str]:
        db_id_to_logical = {
            normalize_notion_id(db_id): logical_db
            for logical_db, db_id in self.config.databases.items()
        }
        mapping: dict[str, str] = {}

        for prop_name, prop_schema in error_properties.items():
            if prop_schema.get("type") != "relation":
                continue
            relation_cfg = prop_schema.get("relation", {})
            target_id = normalize_notion_id(relation_cfg.get("database_id", ""))
            logical_db = db_id_to_logical.get(target_id)
            if logical_db and logical_db not in mapping:
                mapping[logical_db] = prop_name

        return mapping

    def _build_candidates(self, logical_db: str) -> list[Candidate]:
        rows = [row for row in self.store.get_pages(logical_db) if int(row["archived"]) == 0]
        out: list[Candidate] = []
        for row in rows:
            title = (row["title"] or "").strip()
            if not title:
                continue
            out.append(Candidate(page_id=row["page_id"], title=title, normalized=self._normalize_for_match(title)))
        return out

    def _predict_related_ids(
        self,
        normalized_text: str,
        candidates: list[Candidate],
        limit: int,
    ) -> set[str]:
        if not normalized_text:
            return set()

        scored: list[tuple[float, str]] = []
        segments = self._segments_for_similarity(normalized_text)

        for cand in candidates:
            if len(cand.normalized) < 2:
                continue

            score = 0.0
            if cand.normalized in normalized_text:
                score = 1.0
            else:
                best = 0.0
                for seg in segments:
                    ratio = SequenceMatcher(None, cand.normalized, seg).ratio()
                    if ratio > best:
                        best = ratio
                score = best

            if score >= 0.72:
                scored.append((score, cand.page_id))

        scored.sort(key=lambda item: (-item[0], item[1]))
        return {pid for _, pid in scored[:limit]}

    def _predict_similar_errors(
        self,
        page_id: str,
        all_error_features: dict[str, set[str]],
        limit: int,
        threshold: float,
    ) -> set[str]:
        source = all_error_features.get(page_id, set())
        if not source:
            return set()

        scored: list[tuple[float, str]] = []
        for other_id, other_features in all_error_features.items():
            if other_id == page_id or not other_features:
                continue
            inter = len(source & other_features)
            union = len(source | other_features)
            if union == 0:
                continue
            score = inter / union
            if score >= threshold:
                scored.append((score, other_id))

        scored.sort(key=lambda item: (-item[0], item[1]))
        return {pid for _, pid in scored[:limit]}

    def _suggest_title(self, current_title: str, text: str) -> str | None:
        if not self._is_placeholder_title(current_title):
            return None

        for chunk in self._content_fragments(text):
            clean = self._cleanup_candidate_title(chunk)
            if not clean:
                continue
            if self._is_placeholder_title(clean):
                continue
            if len(clean) < 4:
                continue
            return clean[:48]

        return None

    def _is_placeholder_title(self, title: str) -> bool:
        name = (title or "").strip()
        if not name:
            return True
        return any(pattern.match(name) for pattern in _PLACEHOLDER_TITLE_PATTERNS)

    def _combine_page_text(self, page_row: Any) -> str:
        title = (page_row["title"] or "").strip()
        parts = []
        if title and not self._is_placeholder_title(title):
            parts.append(title)
        parts.extend(
            [
                (page_row["property_text"] or "").strip(),
                (page_row["plain_text"] or "").strip(),
                (page_row["text_blob"] or "").strip(),
            ]
        )
        return "\n".join(part for part in parts if part)

    @staticmethod
    def _normalize_for_match(text: str) -> str:
        raw = (text or "").lower().strip()
        if not raw:
            return ""
        raw = re.sub(r"\s+", "", raw)
        raw = re.sub(r"[，。！？；：,.!?;:\\[\\]{}()<>【】（）《》\"'`~|\\/]+", "", raw)
        return raw

    @staticmethod
    def _segments_for_similarity(normalized_text: str) -> list[str]:
        if not normalized_text:
            return []
        size = 20
        stride = 8
        if len(normalized_text) <= size:
            return [normalized_text]
        segments: list[str] = []
        for i in range(0, len(normalized_text), stride):
            segment = normalized_text[i : i + size]
            if len(segment) >= 4:
                segments.append(segment)
            if len(segments) >= 80:
                break
        return segments

    @staticmethod
    def _content_fragments(text: str) -> list[str]:
        raw = (text or "").replace("\r", "\n")
        chunks = re.split(r"[\n。！？!?；;]+", raw)
        return [chunk.strip() for chunk in chunks if chunk and chunk.strip()]

    @staticmethod
    def _cleanup_candidate_title(value: str) -> str:
        out = (value or "").strip()
        out = re.sub(r"^(题目|错因|解析|思路|答案|步骤|错题)[:：]\s*", "", out)
        out = re.sub(r"^\d+[.)、]\s*", "", out)
        out = re.sub(r"^[-_#]+", "", out)
        out = re.sub(r"\s+", " ", out)
        out = out.strip(" .,:;，。；：!！?？")
        return out

    @staticmethod
    def _text_features(text: str) -> set[str]:
        normalized = re.sub(r"\s+", "", (text or "").lower())
        normalized = re.sub(r"[，。！？；：,.!?;:\\[\\]{}()<>【】（）《》\"'`~|\\/]+", "", normalized)
        features: set[str] = set()

        for token in re.findall(r"[a-z0-9]+", normalized):
            if len(token) >= 2:
                if token.isdigit():
                    continue
                features.add(token)

        for i in range(len(normalized) - 1):
            bg = normalized[i : i + 2]
            if bg.strip() and not bg.isdigit():
                features.add(bg)

        return features
