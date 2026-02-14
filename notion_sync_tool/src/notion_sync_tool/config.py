from __future__ import annotations

import os
import tomllib
from dataclasses import dataclass
from pathlib import Path

from .notion_ids import normalize_notion_id


@dataclass(slots=True)
class AppConfig:
    token_env: str
    google_api_env: str
    gemini_model: str
    confidence_threshold: float
    temperature: float
    postgres_dsn: str
    postgres_schema: str
    log_dir: Path
    databases: dict[str, str]

    def notion_token(self) -> str:
        token = os.getenv(self.token_env, "").strip()
        if not token:
            raise RuntimeError(
                f"Missing Notion token in env var '{self.token_env}'. "
                "Set it in your shell or .env loader."
            )
        return token

    def google_api_key(self) -> str:
        key = os.getenv(self.google_api_env, "").strip()
        if not key:
            raise RuntimeError(
                f"Missing Google API key in env var '{self.google_api_env}'. "
                "Set it in your shell or .env loader."
            )
        return key


def load_config(config_path: str | Path) -> AppConfig:
    path = Path(config_path).expanduser().resolve()
    data = tomllib.loads(path.read_text(encoding="utf-8"))

    notion_cfg = data.get("notion", {})
    agent_cfg = data.get("agent", {})
    postgres_cfg = data.get("postgres", {})
    logging_cfg = data.get("logging", {})
    db_cfg = data.get("databases", {})

    token_env = str(notion_cfg.get("token_env", "NOTION_TOKEN")).strip() or "NOTION_TOKEN"
    google_api_env = str(agent_cfg.get("google_api_env", "GOOGLE_API_KEY")).strip() or "GOOGLE_API_KEY"
    gemini_model = str(agent_cfg.get("model", "gemini-3-flash-preview")).strip() or "gemini-3-flash-preview"
    confidence_threshold = float(agent_cfg.get("confidence_threshold", 0.7))
    temperature = float(agent_cfg.get("temperature", 0.2))
    pg_dsn_env = str(postgres_cfg.get("dsn_env", "POSTGRES_DSN")).strip() or "POSTGRES_DSN"
    postgres_dsn = str(postgres_cfg.get("dsn", "")).strip() or os.getenv(pg_dsn_env, "").strip()
    if not postgres_dsn:
        raise RuntimeError(
            f"Missing PostgreSQL DSN. Set [postgres].dsn in config or env var '{pg_dsn_env}'."
        )
    postgres_schema = str(postgres_cfg.get("schema", "notion_sync")).strip() or "notion_sync"

    log_dir = Path(str(logging_cfg.get("dir", "./data/logs"))).expanduser()
    if not log_dir.is_absolute():
        log_dir = (path.parent / log_dir).resolve()

    normalized_dbs: dict[str, str] = {}
    for key, value in db_cfg.items():
        normalized_dbs[str(key)] = normalize_notion_id(str(value))

    required = {"resources", "concepts", "skills", "mindsets", "errors", "actions"}
    missing = sorted(required - set(normalized_dbs))
    if missing:
        raise RuntimeError(f"Missing database ids in config: {', '.join(missing)}")

    return AppConfig(
        token_env=token_env,
        google_api_env=google_api_env,
        gemini_model=gemini_model,
        confidence_threshold=confidence_threshold,
        temperature=temperature,
        postgres_dsn=postgres_dsn,
        postgres_schema=postgres_schema,
        log_dir=log_dir,
        databases=normalized_dbs,
    )
