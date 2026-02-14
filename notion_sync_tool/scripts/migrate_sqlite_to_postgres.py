#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import sqlite3
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

try:
    import psycopg
    from psycopg import sql
except Exception as exc:  # noqa: BLE001
    psycopg = None  # type: ignore[assignment]
    sql = None  # type: ignore[assignment]
    _PSYCOPG_IMPORT_ERROR = exc
else:
    _PSYCOPG_IMPORT_ERROR = None


SHADOW_TABLE_PREFIXES = (
    "sqlite_",
    "pages_fts_",
)
EXPLICIT_SKIP_TABLES = {
    "sqlite_sequence",
}


@dataclass(slots=True)
class ColumnMeta:
    name: str
    sqlite_type: str
    not_null: bool
    default_value: str | None
    pk_order: int


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="One-time migrate SQLite tables/data into PostgreSQL schema."
    )
    parser.add_argument(
        "--sqlite-path",
        default="./data/notion_cache.db",
        help="SQLite DB path (default: ./data/notion_cache.db)",
    )
    parser.add_argument(
        "--pg-dsn",
        default=os.getenv("POSTGRES_DSN", "").strip(),
        help=(
            "PostgreSQL DSN, example: "
            "postgresql://user:pass@127.0.0.1:5432/dbname "
            "(default from env POSTGRES_DSN)"
        ),
    )
    parser.add_argument(
        "--schema",
        default="notion_sync",
        help="Target PostgreSQL schema (default: notion_sync)",
    )
    parser.add_argument(
        "--drop-existing",
        action="store_true",
        help="Drop and recreate target tables before migration",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="Insert batch size (default: 1000)",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if psycopg is None or sql is None:
        print(
            "缺少 PostgreSQL 驱动 psycopg。请先安装：\n"
            "  pip install 'psycopg[binary]>=3.2.0'\n"
            f"原始错误: {_PSYCOPG_IMPORT_ERROR}",
            file=sys.stderr,
        )
        return 3
    sqlite_path = Path(args.sqlite_path).expanduser().resolve()
    if not sqlite_path.exists():
        print(f"SQLite 文件不存在: {sqlite_path}", file=sys.stderr)
        return 1
    if not args.pg_dsn:
        print("缺少 --pg-dsn（或环境变量 POSTGRES_DSN）", file=sys.stderr)
        return 1

    sqlite_conn = sqlite3.connect(str(sqlite_path))
    sqlite_conn.row_factory = sqlite3.Row

    try:
        with psycopg.connect(args.pg_dsn, autocommit=False) as pg_conn:
            with pg_conn.cursor() as cur:
                migrate(sqlite_conn, cur, schema=args.schema, drop_existing=args.drop_existing, batch_size=args.batch_size)
            pg_conn.commit()
    except Exception as exc:  # noqa: BLE001
        print(f"迁移失败，已回滚: {exc}", file=sys.stderr)
        return 2
    finally:
        sqlite_conn.close()

    print("迁移完成。")
    return 0


def migrate(
    sqlite_conn: sqlite3.Connection,
    pg_cur: psycopg.Cursor[Any],
    *,
    schema: str,
    drop_existing: bool,
    batch_size: int,
) -> None:
    pg_cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema)))
    tables = list_user_tables(sqlite_conn)
    if not tables:
        print("未发现可迁移的表。")
        return

    print(f"待迁移表: {', '.join(tables)}")
    for table in tables:
        print(f"\n[migrate] {table}")
        migrate_table(sqlite_conn, pg_cur, schema=schema, table=table, drop_existing=drop_existing, batch_size=batch_size)


def list_user_tables(sqlite_conn: sqlite3.Connection) -> list[str]:
    rows = sqlite_conn.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type = 'table'
        ORDER BY name
        """
    ).fetchall()
    out: list[str] = []
    for row in rows:
        name = str(row["name"])
        if name in EXPLICIT_SKIP_TABLES:
            continue
        if name.startswith(SHADOW_TABLE_PREFIXES):
            # Keep the virtual table itself; skip FTS shadow tables.
            if name == "pages_fts":
                out.append(name)
            continue
        out.append(name)
    return out


def migrate_table(
    sqlite_conn: sqlite3.Connection,
    pg_cur: psycopg.Cursor[Any],
    *,
    schema: str,
    table: str,
    drop_existing: bool,
    batch_size: int,
) -> None:
    columns = table_columns(sqlite_conn, table)
    if not columns:
        print(f"  - 跳过（无列）")
        return

    if drop_existing:
        pg_cur.execute(
            sql.SQL("DROP TABLE IF EXISTS {}.{} CASCADE").format(
                sql.Identifier(schema), sql.Identifier(table)
            )
        )

    create_table(pg_cur, schema=schema, table=table, columns=columns)
    rows_inserted = copy_rows(sqlite_conn, pg_cur, schema=schema, table=table, columns=columns, batch_size=batch_size)
    create_indexes(sqlite_conn, pg_cur, schema=schema, table=table)
    sync_identity_sequence(pg_cur, schema=schema, table=table, columns=columns)
    print(f"  - rows: {rows_inserted}")


def table_columns(sqlite_conn: sqlite3.Connection, table: str) -> list[ColumnMeta]:
    rows = sqlite_conn.execute(f'PRAGMA table_info("{table}")').fetchall()
    out: list[ColumnMeta] = []
    for row in rows:
        out.append(
            ColumnMeta(
                name=str(row["name"]),
                sqlite_type=str(row["type"] or "").strip(),
                not_null=bool(row["notnull"]),
                default_value=row["dflt_value"],
                pk_order=int(row["pk"] or 0),
            )
        )
    return out


def create_table(
    pg_cur: psycopg.Cursor[Any],
    *,
    schema: str,
    table: str,
    columns: list[ColumnMeta],
) -> None:
    pk_cols = [c for c in columns if c.pk_order > 0]
    pk_cols.sort(key=lambda c: c.pk_order)
    pk_col_names = [c.name for c in pk_cols]

    table_parts: list[sql.SQL] = []
    for col in columns:
        is_single_int_pk = (
            len(pk_col_names) == 1
            and pk_col_names[0] == col.name
            and is_integer_type(col.sqlite_type)
        )
        pg_type = sqlite_type_to_pg(col.sqlite_type)
        if is_single_int_pk:
            col_def = sql.SQL("{} BIGINT GENERATED BY DEFAULT AS IDENTITY").format(
                sql.Identifier(col.name)
            )
        else:
            col_def = sql.SQL("{} {}").format(sql.Identifier(col.name), sql.SQL(pg_type))
            if col.not_null:
                col_def += sql.SQL(" NOT NULL")
        table_parts.append(col_def)

    if pk_col_names:
        table_parts.append(
            sql.SQL("PRIMARY KEY ({})").format(
                sql.SQL(", ").join(sql.Identifier(name) for name in pk_col_names)
            )
        )

    create_sql = sql.SQL("CREATE TABLE IF NOT EXISTS {}.{} ({})").format(
        sql.Identifier(schema),
        sql.Identifier(table),
        sql.SQL(", ").join(table_parts),
    )
    pg_cur.execute(create_sql)


def copy_rows(
    sqlite_conn: sqlite3.Connection,
    pg_cur: psycopg.Cursor[Any],
    *,
    schema: str,
    table: str,
    columns: list[ColumnMeta],
    batch_size: int,
) -> int:
    col_names = [c.name for c in columns]
    quoted_cols = ", ".join(f'"{name}"' for name in col_names)
    select_sql = f'SELECT {quoted_cols} FROM "{table}"'
    rows = sqlite_conn.execute(select_sql)

    insert_sql = sql.SQL("INSERT INTO {}.{} ({}) VALUES ({})").format(
        sql.Identifier(schema),
        sql.Identifier(table),
        sql.SQL(", ").join(sql.Identifier(c) for c in col_names),
        sql.SQL(", ").join(sql.Placeholder() for _ in col_names),
    )

    total = 0
    batch: list[tuple[Any, ...]] = []
    for row in rows:
        batch.append(tuple(row[c] for c in col_names))
        if len(batch) >= batch_size:
            pg_cur.executemany(insert_sql, batch)
            total += len(batch)
            batch.clear()

    if batch:
        pg_cur.executemany(insert_sql, batch)
        total += len(batch)
    return total


def create_indexes(
    sqlite_conn: sqlite3.Connection,
    pg_cur: psycopg.Cursor[Any],
    *,
    schema: str,
    table: str,
) -> None:
    idx_rows = sqlite_conn.execute(f'PRAGMA index_list("{table}")').fetchall()
    for idx in idx_rows:
        idx_name = str(idx["name"])
        unique = bool(idx["unique"])
        origin = str(idx["origin"] or "")
        partial = bool(idx["partial"])
        if origin == "pk":
            continue
        if partial:
            continue

        idx_cols_rows = sqlite_conn.execute(f'PRAGMA index_info("{idx_name}")').fetchall()
        idx_cols = [str(item["name"]) for item in idx_cols_rows if item["name"]]
        if not idx_cols:
            continue

        pg_index_name = f"{table}_{idx_name}"
        create_idx_sql = sql.SQL("CREATE {} INDEX IF NOT EXISTS {} ON {}.{} ({})").format(
            sql.SQL("UNIQUE") if unique else sql.SQL(""),
            sql.Identifier(pg_index_name),
            sql.Identifier(schema),
            sql.Identifier(table),
            sql.SQL(", ").join(sql.Identifier(c) for c in idx_cols),
        )
        pg_cur.execute(create_idx_sql)


def sync_identity_sequence(
    pg_cur: psycopg.Cursor[Any],
    *,
    schema: str,
    table: str,
    columns: list[ColumnMeta],
) -> None:
    pk_cols = [c for c in columns if c.pk_order > 0]
    if len(pk_cols) != 1:
        return
    pk = pk_cols[0]
    if not is_integer_type(pk.sqlite_type):
        return

    schema_table = f"{schema}.{table}"
    seq_sql = sql.SQL(
        """
        SELECT setval(
          pg_get_serial_sequence(%s, %s),
          COALESCE((SELECT MAX({col}) FROM {tbl}), 1),
          true
        )
        """
    ).format(
        col=sql.Identifier(pk.name),
        tbl=sql.SQL("{}.{}").format(sql.Identifier(schema), sql.Identifier(table)),
    )
    try:
        pg_cur.execute(seq_sql, (schema_table, pk.name))
    except Exception:
        # Some columns may not actually be backed by a sequence.
        pass


def sqlite_type_to_pg(sqlite_type: str) -> str:
    st = (sqlite_type or "").strip().upper()
    if not st:
        return "TEXT"
    if "INT" in st:
        return "BIGINT"
    if any(key in st for key in ("REAL", "FLOA", "DOUB")):
        return "DOUBLE PRECISION"
    if "BLOB" in st:
        return "BYTEA"
    if any(key in st for key in ("NUMERIC", "DECIMAL", "BOOL")):
        return "NUMERIC"
    if any(key in st for key in ("DATE", "TIME")):
        return "TIMESTAMP"
    return "TEXT"


def is_integer_type(sqlite_type: str) -> bool:
    return "INT" in (sqlite_type or "").upper()


if __name__ == "__main__":
    raise SystemExit(main())
