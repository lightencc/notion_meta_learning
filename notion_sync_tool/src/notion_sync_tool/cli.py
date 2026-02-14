from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

from .agent_workflow import AgentWorkflowService
from .config import load_config
from .enrich_service import EnrichOptions, ErrorEnrichmentService
from .knowledge_workflow import KnowledgeRunOptions, KnowledgeWorkflowService
from .logging_utils import get_logger, setup_logging
from .notion_gateway import NotionGateway
from .postgres_store import PostgresStore
from .sync_service import SyncOptions, SyncService


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="notion-sync-tool",
        description="Sync Notion databases, run Gemini workflow, and review/apply updates.",
    )
    parser.add_argument(
        "--config",
        default="./config.toml",
        help="Path to config.toml (default: ./config.toml)",
    )
    parser.add_argument(
        "--log-level",
        default=None,
        help="Logging level: DEBUG/INFO/WARNING/ERROR (default from env or INFO)",
    )
    parser.add_argument(
        "--log-dir",
        default=None,
        help="Log directory (default from env or ./data/logs)",
    )

    sub = parser.add_subparsers(dest="command", required=True)

    sync = sub.add_parser("sync", help="Sync configured Notion databases into PostgreSQL")
    sync.add_argument(
        "--skip-content",
        action="store_true",
        help="Do not fetch page block content into PostgreSQL plain_text",
    )
    sync.add_argument(
        "--content-max-chars",
        type=int,
        default=1600,
        help="Max block text chars per page when syncing content",
    )
    sync.add_argument(
        "--content-max-depth",
        type=int,
        default=2,
        help="Max nested block depth when syncing page content",
    )
    sync.add_argument(
        "--page-size",
        type=int,
        default=100,
        help="Notion query page_size per request",
    )

    enrich = sub.add_parser(
        "enrich-errors",
        help="Analyze synced error pages and update title/relations back to Notion",
    )
    enrich.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview updates without writing to Notion",
    )
    enrich.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Only process first N error pages",
    )
    enrich.add_argument(
        "--max-links-per-library",
        type=int,
        default=3,
        help="Max auto-linked pages for each of resources/concepts/skills/mindsets",
    )
    enrich.add_argument(
        "--max-similar-links",
        type=int,
        default=3,
        help="Max auto-linked similar errors per page",
    )
    enrich.add_argument(
        "--similar-threshold",
        type=float,
        default=0.35,
        help="Jaccard threshold for similar-error linking (0~1)",
    )

    stats = sub.add_parser("stats", help="Show current PostgreSQL snapshot stats")
    stats.add_argument(
        "--json",
        action="store_true",
        help="Output stats as JSON",
    )

    agent_run = sub.add_parser(
        "agent-run",
        help="Run Gemini agent workflow and store review suggestions into PostgreSQL",
    )
    agent_run.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Max number of target errors in one run (0 = all)",
    )

    knowledge_run = sub.add_parser(
        "knowledge-run",
        help="Run knowledge-base organization workflow and store review suggestions into PostgreSQL",
    )
    knowledge_run.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Max number of target pages in one run (0 = all)",
    )

    agent_web = sub.add_parser(
        "agent-web",
        help="Start web UI for workflow progress and manual review",
    )
    agent_web.add_argument("--host", default="127.0.0.1", help="Bind host (default 127.0.0.1)")
    agent_web.add_argument("--port", type=int, default=8787, help="Bind port (default 8787)")
    agent_web.add_argument("--reload", action="store_true", help="Enable auto-reload")

    return parser


def main(argv: list[str] | None = None) -> int:
    load_dotenv()
    parser = build_parser()
    args = parser.parse_args(argv)

    config_path = Path(args.config).expanduser().resolve()
    cfg = load_config(config_path)
    log_dir = Path(args.log_dir).expanduser() if args.log_dir else cfg.log_dir
    setup_logging(log_dir=log_dir, level=args.log_level)
    logger = get_logger(__name__)
    logger.info("CLI command start: command=%s config=%s", args.command, str(config_path))
    store = PostgresStore(cfg.postgres_dsn, schema=cfg.postgres_schema)

    if args.command == "stats":
        data = store.stats()
        if args.json:
            print(json.dumps(data, ensure_ascii=False, indent=2))
        else:
            print("[stats] pages:")
            for key, value in data.get("pages", {}).items():
                print(f"  - {key}: {value}")
            print("[stats] relations:")
            for key, value in data.get("relations", {}).items():
                print(f"  - {key}: {value}")
        return 0

    if args.command == "agent-run":
        svc = AgentWorkflowService(config=cfg, store=store)
        limit = None if args.limit <= 0 else args.limit
        try:
            summary = svc.run(limit=limit)
        except Exception as exc:  # noqa: BLE001
            logger.exception("agent-run failed")
            print(f"[agent-run] failed: {exc}")
            return 1
        print("[agent-run] summary")
        print(
            json.dumps(
                {
                    "run_id": summary.run_id,
                    "target_count": summary.target_count,
                    "suggestion_count": summary.suggestion_count,
                    "needs_review_count": summary.needs_review_count,
                    "failure_count": summary.failure_count,
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        logger.info(
            "agent-run completed: run_id=%s target=%s suggestions=%s needs_review=%s failures=%s",
            summary.run_id,
            summary.target_count,
            summary.suggestion_count,
            summary.needs_review_count,
            summary.failure_count,
        )
        return 0

    if args.command == "knowledge-run":
        svc = KnowledgeWorkflowService(config=cfg, store=store)
        limit = None if args.limit <= 0 else args.limit
        try:
            summary = svc.run(KnowledgeRunOptions(limit=limit))
        except Exception as exc:  # noqa: BLE001
            logger.exception("knowledge-run failed")
            print(f"[knowledge-run] failed: {exc}")
            return 1
        print("[knowledge-run] summary")
        print(
            json.dumps(
                {
                    "run_id": summary.run_id,
                    "target_count": summary.target_count,
                    "suggestion_count": summary.suggestion_count,
                    "needs_review_count": summary.needs_review_count,
                    "failure_count": summary.failure_count,
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        logger.info(
            "knowledge-run completed: run_id=%s target=%s suggestions=%s needs_review=%s failures=%s",
            summary.run_id,
            summary.target_count,
            summary.suggestion_count,
            summary.needs_review_count,
            summary.failure_count,
        )
        return 0

    if args.command == "agent-web":
        import uvicorn

        os.environ["NOTION_SYNC_TOOL_CONFIG"] = str(config_path)
        logger.info("Starting web app: host=%s port=%s reload=%s", args.host, args.port, args.reload)
        uvicorn.run(
            "notion_sync_tool.web_app:create_app",
            factory=True,
            host=args.host,
            port=args.port,
            reload=args.reload,
        )
        return 0

    token = cfg.notion_token()
    gateway = NotionGateway(token)

    if args.command == "sync":
        svc = SyncService(config=cfg, gateway=gateway, store=store)
        result = svc.run(
            SyncOptions(
                include_page_content=not args.skip_content,
                content_max_chars=args.content_max_chars,
                content_max_depth=args.content_max_depth,
                page_size=args.page_size,
            )
        )
        print("[sync] summary")
        print(
            json.dumps(
                {
                    "run_id": result.run_id,
                    "databases": result.databases,
                    "relations": result.relations,
                    "changed": result.changed,
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        logger.info(
            "sync completed: run_id=%s databases=%s relations=%s changed=%s",
            result.run_id,
            result.databases,
            result.relations,
            result.changed,
        )
        return 0

    if args.command == "enrich-errors":
        svc = ErrorEnrichmentService(config=cfg, gateway=gateway, store=store)
        result = svc.run(
            EnrichOptions(
                dry_run=args.dry_run,
                limit=args.limit,
                max_links_per_library=args.max_links_per_library,
                max_similar_links=args.max_similar_links,
                similar_threshold=args.similar_threshold,
            )
        )
        print("[enrich-errors] summary")
        print(
            json.dumps(
                {
                    "scanned": result.scanned,
                    "updated": result.updated,
                    "renamed": result.renamed,
                    "relation_updates": result.relation_updates,
                    "similar_updates": result.similar_updates,
                    "dry_run": args.dry_run,
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        logger.info(
            "enrich-errors completed: scanned=%s updated=%s renamed=%s relation_updates=%s similar_updates=%s dry_run=%s",
            result.scanned,
            result.updated,
            result.renamed,
            result.relation_updates,
            result.similar_updates,
            args.dry_run,
        )
        return 0

    parser.print_help()
    return 1


if __name__ == "__main__":
    sys.exit(main())
