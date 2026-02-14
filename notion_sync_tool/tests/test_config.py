from __future__ import annotations

from pathlib import Path

from notion_sync_tool.config import load_config


def test_load_config_uses_postgres_dsn_env(tmp_path: Path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    cfg_file = tmp_path / "config.toml"
    cfg_file.write_text(
        """
[notion]
token_env = "NOTION_TOKEN"

[agent]
google_api_env = "GOOGLE_API_KEY"
model = "gemini-3-flash-preview"
confidence_threshold = 0.7
temperature = 0.2

[postgres]
dsn_env = "POSTGRES_DSN"
schema = "notion_sync"

[logging]
dir = "./logs"

[databases]
resources = "eb1861e3dd9f4d058f66eed20405c5bb"
concepts = "6a4d5f60-96d3-4b93-b7cc-36c9236384f9"
skills = "287e1884-8fe8-443c-98ca-e28b0ecc5b8c"
mindsets = "51963be9-a4eb-4a31-8571-e1ea14137eb7"
errors = "b73618d6-76a3-497e-a405-f618be2984ee"
actions = "15664f5c-77e3-4bc8-b12d-d4a6d6f3edbb"
""".strip(),
        encoding="utf-8",
    )
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://u:p@127.0.0.1:5432/db")
    cfg = load_config(cfg_file)
    assert cfg.postgres_schema == "notion_sync"
    assert cfg.postgres_dsn == "postgresql://u:p@127.0.0.1:5432/db"
    assert cfg.databases["resources"] == "eb1861e3-dd9f-4d05-8f66-eed20405c5bb"

