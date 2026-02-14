from __future__ import annotations

from notion_sync_tool.enrich_service import ErrorEnrichmentService


def test_cleanup_candidate_title() -> None:
    assert ErrorEnrichmentService._cleanup_candidate_title("题目： 1. 分数比较") == "分数比较"


def test_text_features_contains_bigrams() -> None:
    feats = ErrorEnrichmentService._text_features("分数比较")
    assert "分数" in feats
    assert "数比" in feats


def test_predict_similar_errors_jaccard() -> None:
    svc = object.__new__(ErrorEnrichmentService)
    result = svc._predict_similar_errors(  # type: ignore[misc]
        page_id="a",
        all_error_features={
            "a": {"分数", "比较"},
            "b": {"分数", "比较", "大小"},
            "c": {"方程", "应用"},
        },
        limit=3,
        threshold=0.3,
    )
    assert "b" in result
    assert "c" not in result

