# Notion Meta Learning

一个面向 Notion 学习库的 AI 工作流项目，核心目标是：
- 将 Notion 多库数据同步到本地 PostgreSQL，支持高效检索与增量更新。
- 通过 Gemini Agent 生成“错题检查”和“知识库整理”建议。
- 在 Web UI 中人工复核后，再回写 Notion，保证可控与可追溯。

## 目录结构

- `notion_sync_tool/`：主项目代码（CLI + Web + Agent + 数据层）
- `notion_import/`：导入与映射数据（CSV）
- `docs/`：模板与文档素材
- `screenshot/`：界面截图素材

## 核心能力

1. Notion -> PostgreSQL 同步
- 支持全量与增量同步
- 记录同步批次和页面级变更明细（sync runs/events）

2. 错题检查工作流
- 生成标题、四库关联、相似错题建议
- 支持批量复核、批量回写、进度追踪
- 提供错因分布、薄弱概念、趋势分析面板

3. 知识库整理工作流
- 基于资料映射和模板生成资料/概念/技能/思想页面内容
- 支持文档/PDF 作为附件直接输入 Gemini
- 人工复核后回写 Notion 页面正文

## 快速开始

```bash
cd notion_sync_tool
python -m venv .venv
source .venv/bin/activate
pip install -e .
cp config.example.toml config.toml
```

配置 `.env`（最少）：

```bash
NOTION_TOKEN=secret_xxx
GOOGLE_API_KEY=your_google_api_key
POSTGRES_DSN=postgresql://postgres:password@127.0.0.1:5432/notion_sync
```

常用命令：

```bash
# 同步数据
notion-sync-tool --config ./config.toml sync

# 错题建议
notion-sync-tool --config ./config.toml agent-run --limit 20

# 知识库建议
notion-sync-tool --config ./config.toml knowledge-run --limit 20

# Web 复核台
notion-sync-tool --config ./config.toml agent-web --host 127.0.0.1 --port 8787
```

打开：`http://127.0.0.1:8787`

## 详细文档

- 运行与参数说明：`notion_sync_tool/README.md`

