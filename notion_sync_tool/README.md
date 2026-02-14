# notion-sync-tool

独立项目：把 Notion 学习库同步到 PostgreSQL，并通过 Gemini Agent 生成错题关联建议，在 Web UI 人工确认后再写回 Notion。

## 功能

- `sync`：按配置数据库全量拉取到 PostgreSQL（页面、属性、关系、可选块内容）。
- `agent-run`：执行 Agent 工作流，生成建议并写入本地 `agent_suggestions` 队列。
- `knowledge-run`：执行知识库整理工作流，生成资料/概念/技能/思想页面内容建议并写入本地 `knowledge_suggestions` 队列。
- `agent-web`：打开 Web UI，查看每一步输出、人工复核并确认更新 Notion。
  - 页面加载时自动检查 Notion 与 PostgreSQL 是否有差异。
  - 支持在页面内手动触发 Notion -> PostgreSQL 同步。
- `stats`：查看本地快照统计。

## 为什么仍需要 `NOTION_TOKEN`

- 通过 Notion MCP 读取时，MCP 服务通常在宿主环境已完成鉴权。
- 本项目是本地直接调用 Notion 官方 API（同步与写回），所以仍需 `NOTION_TOKEN`。

## 环境变量

`.env` 至少包含：

```bash
NOTION_TOKEN='secret_xxx'
GOOGLE_API_KEY='your_google_api_key'
POSTGRES_DSN='postgresql://postgres:password@127.0.0.1:5432/notion_sync'
```

## 配置

复制配置文件：

```bash
cp config.example.toml config.toml
```

关键 Agent 参数在 `[agent]`：

- `model = "gemini-3-flash-preview"`
- `confidence_threshold`：低于此阈值自动标记 `needs_review`
- `temperature`

数据库参数在 `[postgres]`：

- `dsn` 或 `dsn_env`（推荐走环境变量 `POSTGRES_DSN`）
- `schema`（默认 `notion_sync`）

## 快速开始

```bash
cd /Users/lightencc/Documents/notion_meta_learning/notion_sync_tool
python -m venv .venv
source .venv/bin/activate
pip install -e .
```

先同步 Notion 到 PostgreSQL：

```bash
notion-sync-tool --config ./config.toml sync
```

运行 Agent 生成建议：

```bash
notion-sync-tool --config ./config.toml agent-run --limit 20

notion-sync-tool --config ./config.toml knowledge-run --limit 20
```

启动复核页面：

```bash
notion-sync-tool --config ./config.toml agent-web --host 127.0.0.1 --port 8787
```

浏览器打开：`http://127.0.0.1:8787`

- 默认进入知识库整理：`/knowledge`
- 错题检查入口：`/errors`

## SQLite -> PostgreSQL 一次性迁移（历史数据）

如果你要把当前 SQLite 全量迁移到 PostgreSQL，使用脚本：

`scripts/migrate_sqlite_to_postgres.py`

1. 安装驱动（一次）：

```bash
pip install 'psycopg[binary]>=3.2.0'
```

2. 执行迁移（示例）：

```bash
python scripts/migrate_sqlite_to_postgres.py \
  --sqlite-path ./data/notion_cache.db \
  --pg-dsn 'postgresql://postgres:password@127.0.0.1:5432/notion_sync' \
  --schema notion_sync \
  --drop-existing
```

说明：
- `--drop-existing`：会先删掉目标 schema 下同名表再重建（建议首次迁移开启）。
- 默认会迁移所有业务表数据与索引（会跳过 SQLite 内部表和 FTS shadow 表）。
- 迁移过程使用单事务，失败会回滚。

## 日志

- 日志默认写入：`data/logs/`
- 文件说明：
  - `app.log`：系统运行日志（同步、工作流、批量回写、Web 请求）
  - `error.log`：`WARNING` 及以上日志（异常与失败）
- 级别控制：
  - 环境变量：`NOTION_SYNC_LOG_LEVEL=DEBUG|INFO|WARNING|ERROR`
  - 或命令行参数：`--log-level DEBUG`
- 日志目录控制：
  - 环境变量：`NOTION_SYNC_LOG_DIR=/your/path`
  - 或命令行参数：`--log-dir /your/path`

## UI 工作流（带步骤输出）

1. Dashboard：查看待处理数量、最近运行记录。
   - Sync Status：自动对比本地快照与远端更新状态，支持手动同步按钮。
2. Run Detail：查看每一步日志输出（扫描目标、候选加载、逐条推理结果）。
3. Suggestion Detail：
   - Step 1 源上下文（错题属性与页面内容）
   - Step 2 候选上下文（四库全量标题 + 相似错题候选）
   - Step 3 Agent 输出与人工编辑
   - Step 4 确认后写回 Notion

## 知识库整理工作流（新增）

1. 从 `notion_import/昂立四年级上_关联关系映射.csv` 读取课次-资料-文档-概念-技能-思想映射。
2. 读取映射中的文档路径（doc/docx/pdf/txt）作为 Agent 上下文。
3. 按模板生成建议内容：
   - `docs/template/math_lesson_note.md`
   - `docs/template/concept_note.md`
   - `docs/template/skill_note.md`
   - `docs/template/mindset_note.md`
4. 在 `/knowledge/suggestions` 人工复核后确认写回 Notion 页面正文。

## PostgreSQL 结构（新增）

- `workflow_runs`：每次 Agent 执行汇总。
- `workflow_events`：每一步过程日志。
- `agent_suggestions`：待复核建议、人工修改、状态与失败信息。
- `knowledge_runs`：知识库整理每次执行汇总。
- `knowledge_events`：知识库整理步骤日志。
- `knowledge_suggestions`：知识库整理待复核内容建议。

## 状态说明

- `pending_review`：建议可直接复核。
- `needs_review`：低置信度或校验不通过，需人工处理。
- `applied`：已确认并写回 Notion。
- `rejected`：人工驳回。
- `failed`：写回 Notion 失败。
