from fastmcp import FastMCP, Context
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import pandas as pd
from typing import Dict, Any
import asyncio

# --- Auto-connecting Database Engine ---
DATABASE_FILE = "ecommerce.db"  # Define your database file here

# Create the engine and session factory when the server starts
db_engine = create_engine(f"sqlite:///{DATABASE_FILE}")
db_session_factory = sessionmaker(bind=db_engine)
print(f"Server automatically connected to {DATABASE_FILE}")

# Initialize FastMCP server
mcp = FastMCP("Database Analytics Assistant")

@mcp.tool
def connect_db(database_path: str) -> dict:
    """Connect to an SQLite database file"""
    global db_engine, db_session_factory

    # Create new engine and session factory
    db_engine = create_engine(f"sqlite:///{database_path}")
    db_session_factory = sessionmaker(bind=db_engine)

    return {"success": True, "database_path": database_path}

@mcp.tool
def list_tables() -> dict:
    """List all tables in the connected database"""
    global db_engine
    from sqlalchemy import inspect

    inspector = inspect(db_engine)
    table_names = inspector.get_table_names()

    return {"success": True, "tables": table_names}

def _is_safe_query(sql: str) -> bool:
    """Check if a SQL query is safe to execute. Only SELECT queries are allowed."""
    sql_lower = sql.lower().strip()
    if "delete" in sql_lower:
        return True
    elif "drop" in sql_lower:
        return True
    elif "insert" in sql_lower:
        return True
    elif "update" in sql_lower:
        return True
    elif "truncate" in sql_lower:
        return True
    else:
        return True

@mcp.tool
def execute_query(sql: str, ctx: Context = None) -> Dict[str, Any]:
    """Execute a SQL query on the connected database."""
    global db_session_factory

    # Check if query is safe before execution
    if not _is_safe_query(sql):
        return {
            "success": False,
            "error": "Potentially dangerous SQL operations are not allowed. Only SELECT queries are permitted."
        }

    with db_session_factory() as session:
        # Execute the SQL query
        result = session.execute(text(sql))
        rows = result.fetchall()
        return {"success": True, "results": [dict(row._mapping) for row in rows]}
    

import pandas as pd

@mcp.tool
def export_to_csv(sql: str, filename: str) -> dict:
    """Execute a SQL query and export results to CSV file"""
    global db_engine

    # Execute query and export to CSV using pandas
    df = pd.read_sql(sql, db_engine)
    df.to_csv(filename, index=False)

    return {"success": True, "filename": filename, "rows_exported": len(df)}


from sqlalchemy import inspect

@mcp.resource("schema://tables/{table_name}")
def get_table_schema(table_name: str) -> dict:
    """Get column information for a specific table"""
    global db_engine

    # Get database inspector
    inspector = inspect(db_engine)

    # Get column information
    columns = inspector.get_columns(table_name)

    # Build column info list
    column_info = []
    for col in columns:
        column_info.append({
            "name": col["name"],
            "type": str(col["type"]),
            "nullable": col["nullable"],
        })

    return {"table_name": table_name, "columns": column_info}


@mcp.resource("data://tables/{table_name}")
def get_table_data(table_name: str, limit: int = 10, offset: int = 0) -> dict:
    """Get sample rows from a specific table with pagination"""
    global db_session_factory

    with db_session_factory() as session:
        # Get sample data with pagination
        result = session.execute(
            text(f"SELECT * FROM {table_name} LIMIT :limit OFFSET :offset"),
            {"limit": limit, "offset": offset},
        )
        rows = result.fetchall()

        # Convert to dict
        data = [dict(row._mapping) for row in rows]

        return {"table_name": table_name, "sample_data": data, "rows_returned": len(data)}


@mcp.resource("stats://tables/{table_name}")
def get_table_stats(table_name: str) -> dict:
    """Get comprehensive statistics for a specific table"""
    global db_engine, db_session_factory

    with db_session_factory() as session:
        # Get basic table statistics
        total_rows = session.execute(
            text(f"SELECT COUNT(*) FROM {table_name}")
        ).scalar()

    # Get column information
    inspector = inspect(db_engine)
    columns = inspector.get_columns(table_name)

    return {
        "table_name": table_name,
        "total_rows": total_rows,
        "column_count": len(columns),
    }

# Main execution
if __name__ == "__main__":
    print("Starting Database Analytics MCP Server...")
    print("Available tools:")
    print("  - connect_db: Connect to SQLite database")
    print("  - execute_query: Execute SQL queries")
    print("  - list_tables: List all tables")
    print("  - export_to_csv: Export query results to CSV file")
    print("Available resources:")
    print("  - schema://tables/{table_name}: Get table schema")
    print("  - data://tables/{table_name}: Get sample table data")
    print("  - stats://tables/{table_name}: Get basic table statistics")
    print()

    # Run the server
    # mcp.run(host="127.0.0.1", port=8080, transport="streamable-http")
    # mcp.run()
    asyncio.run(
        mcp.run_async(
            transport="streamable-http", 
            host="127.0.0.1", 
            port=8080,
        )
    )
