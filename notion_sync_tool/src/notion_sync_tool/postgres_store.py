from __future__ import annotations

from .notion_helpers import (
    extract_property_text,
    extract_relation_rows,
    extract_title_from_page,
    first_title_property_name,
    property_plain_text,
)
from .store.base import PostgresStoreBase, StoredPage, cn_now_iso
from .store.error_mixin import ErrorWorkflowStoreMixin
from .store.knowledge_mixin import KnowledgeWorkflowStoreMixin
from .store.sync_mixin import SyncStoreMixin


class PostgresStore(
    SyncStoreMixin,
    ErrorWorkflowStoreMixin,
    KnowledgeWorkflowStoreMixin,
    PostgresStoreBase,
):
    pass


__all__ = [
    "PostgresStore",
    "StoredPage",
    "cn_now_iso",
    "first_title_property_name",
    "extract_title_from_page",
    "property_plain_text",
    "extract_property_text",
    "extract_relation_rows",
]
