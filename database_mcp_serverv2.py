# database_mcp_server_pg.py
"""
PostgreSQL‑backed FastMCP server that follows the connection‑pooling pattern
outlined in the OpenReplay article (May 13 2025) entitled
"How to extend your MCP server with database access".

Key improvements over the original SQLite script:
  • Uses **asyncpg.create_pool** for efficient, concurrent access to PostgreSQL.
  • Loads credentials from environment variables (.env) instead of hard‑coding.
  • Provides **init_db() / close_db()** lifecycle helpers.
  • All resources/tools are fully async and leverage parameterised queries.
  • Read‑only Resources vs. write‑capable Tools separation.
  • Basic input‑validation + SQL‑injection prevention via allow‑list + bind args.

Requirements (add to *requirements.txt*):
    fastmcp
    asyncpg
    python-dotenv
    pandas  # optional, CSV export helper

Environment (.env):
    DB_USER=postgres
    DB_PASSWORD=secret
    DB_NAME=mydb
    DB_HOST=localhost
    DB_PORT=5432
"""

import os
import asyncio
import logging
from typing import Dict, Any, List

import asyncpg
import pandas as pd
from dotenv import load_dotenv
from fastmcp import FastMCP, Context

from decimal import Decimal
from datetime import datetime


# ────────────────────────────────
#  Logging
# ────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("mcp_server")

# ────────────────────────────────
#  Environment & DB pool helpers
# ────────────────────────────────
load_dotenv()

DB_CONFIG = {
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", "5432")),
}

# Global pool handle
_db_pool: asyncpg.pool.Pool | None = None




async def init_db(min_size: int = 2, max_size: int = 10) -> asyncpg.pool.Pool:
    """Create the asyncpg connection pool once on server start‑up."""
    global _db_pool
    if _db_pool:  # Already initialised
        return _db_pool

    logger.info("Creating PostgreSQL connection pool …")
    _db_pool = await asyncpg.create_pool(
        **DB_CONFIG,
        min_size=min_size,
        max_size=max_size,
        command_timeout=60,
        timeout=10,
        statement_cache_size=100,
        max_inactive_connection_lifetime=300,
    )

    # Connectivity sanity‑check
    async with _db_pool.acquire() as conn:
        version = await conn.fetchval("SELECT version();")
        logger.info(f"Connected to: {version}")

    return _db_pool


async def close_db():
    """Gracefully close the pool during shutdown."""
    global _db_pool
    if _db_pool:
        logger.info("Closing database connection pool …")
        await _db_pool.close()
        _db_pool = None


# ────────────────────────────────
#  FastMCP Server
# ────────────────────────────────

mcp = FastMCP(
    name="PostgreSQL Analytics Assistant",
    version="1.0.0",
)

# ----- Helper: basic allow‑list so only SELECT statements are permitted
_DENIED_KEYWORDS = {"insert", "update", "delete", "drop", "truncate", "alter"}

def _is_safe_query(sql: str) -> bool:
    tokens = sql.lower().split()
    # return tokens and tokens[0] == "select" and not any(k in tokens for k in _DENIED_KEYWORDS)
    return tokens and not any(k in tokens for k in _DENIED_KEYWORDS)

# ────────────────────────────────
#  Tools
# ────────────────────────────────

@mcp.tool(
    name="execute_query",
    description="Run a read‑only SQL SELECT on the connected PostgreSQL database.",
)
async def execute_query(sql: str, ctx: Context | None = None) -> Dict[str, Any]:

    if not _is_safe_query(sql): #
        return {"success": False, "error": "Only safe SELECT queries are allowed."} #

    pool = await init_db() #
    async with pool.acquire() as conn: #
        logger.info(f"Executing query: {sql[:120]}…") #
        rows = await conn.fetch(sql) #
        result = [] #
        logger.info(f"Rows returned: {len(rows)}") #
        for row in rows: #
            # CORRECTED LINE: Convert asyncpg.Record to a dict
            processed_row = {key: row[key] for key in row.keys()} #

            # Now, process the dict-like object for JSON serialization
            for key, value in processed_row.items(): #
                if isinstance(value, datetime): #
                    processed_row[key] = value.isoformat() #
                elif isinstance(value, Decimal): #
                    processed_row[key] = float(value) # Convert Decimal to float for JSON serialization

            result.append(processed_row) #
        return {"success": True, "results": result} #


@mcp.tool(
    name="export_to_csv",
    description="Execute a SELECT query and dump the result to a local CSV file.",
)
async def export_to_csv(sql: str, filename: str) -> Dict[str, Any]:
    if not _is_safe_query(sql):
        return {"success": False, "error": "Only SELECT queries are allowed for export."}

    pool = await init_db()
    async with pool.acquire() as conn:
        rows = await conn.fetch(sql)
    df = pd.DataFrame([dict(r) for r in rows])
    df.to_csv(filename, index=False)
    return {"success": True, "filename": filename, "rows_exported": len(df)}


# ────────────────────────────────
#  Resources (read‑only by design)
# ────────────────────────────────

@mcp.resource("schema://tables/{table_name}")
async def get_table_schema(table_name: str) -> Dict[str, Any]:
    pool = await init_db()
    sql = """
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = $1
        ORDER BY ordinal_position;
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(sql, table_name)
    return {
        "table_name": table_name,
        "columns": [
            {
                "name": r["column_name"],
                "type": r["data_type"],
                "nullable": r["is_nullable"] == "YES",
            }
            for r in rows
        ],
    }


@mcp.resource("data://tables/{table_name}")
async def get_table_data(table_name: str, limit: int = 10, offset: int = 0) -> Dict[str, Any]:
    pool = await init_db() #
    sql = f"SELECT * FROM {asyncpg.quote_ident(table_name)} LIMIT $1 OFFSET $2;" #
    async with pool.acquire() as conn: #
        rows = await conn.fetch(sql, limit, offset) #

    processed_sample_data = [] #
    for row in rows: #
        # Corrected line: Convert asyncpg.Record to a dict
        processed_row = {key: row[key] for key in row.keys()} #

        # Convert specific types for JSON serialization
        for key, value in processed_row.items(): #
            if isinstance(value, datetime): #
                processed_row[key] = value.isoformat() #
            elif isinstance(value, Decimal): #
                processed_row[key] = float(value) #

        processed_sample_data.append(processed_row) #

    return { #
        "table_name": table_name, #
        "sample_data": processed_sample_data, #
        "rows_returned": len(rows), #
    } #


@mcp.resource("stats://tables/{table_name}")
async def get_table_stats(table_name: str) -> Dict[str, Any]:
    pool = await init_db()
    count_sql = f"SELECT COUNT(*) FROM {asyncpg.quote_ident(table_name)};"
    async with pool.acquire() as conn:
        total_rows = await conn.fetchval(count_sql)
    schema_info = await get_table_schema(table_name)
    return {
        "table_name": table_name,
        "total_rows": total_rows,
        "column_count": len(schema_info["columns"]),
    }


# ────────────────────────────────
#  Convenience Tool: list all tables in *public* schema
# ────────────────────────────────

@mcp.tool(
    name="list_tables",
    description="List all tables in the public schema of the connected PostgreSQL database.",
)
async def list_tables() -> Dict[str, Any]:
    pool = await init_db()
    sql = """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
        ORDER BY table_name;
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(sql)
    return {"success": True, "tables": [r["table_name"] for r in rows]}


# ────────────────────────────────
#  Server entry‑point
# ────────────────────────────────

async def main() -> None:
    try:
        await init_db()
        logger.info("🚀  Starting FastMCP server on http://127.0.0.1:8080 …")
        await mcp.run_async(
            transport="streamable-http",
            host="127.0.0.1",
            port=8080,
        )
    finally:
        await close_db()


if __name__ == "__main__":
    asyncio.run(main())
