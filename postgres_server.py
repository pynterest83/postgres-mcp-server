from typing import List, Dict, Any, Optional
from mcp.server.fastmcp import FastMCP
from connection import PostgresConnection, PostgresConfig, PostgresDriver
import asyncio
import dotenv
import os
import mcp.types as types
from pydantic import Field

# Create MCP server instance directly
mcp = FastMCP("PostgreSQL MCP Server")
dotenv.load_dotenv()  # Load environment variables from .env file
config  = PostgresConfig(
    host=os.getenv("POSTGRES_HOST", "localhost"),
    port=int(os.getenv("POSTGRES_PORT", 5432)),
    user=os.getenv("POSTGRES_USER", "user"),
    password=os.getenv("POSTGRES_PASSWORD", "password"),
    database=os.getenv("POSTGRES_DB", "database")
)

ResponseType = List[types.TextContent | types.ImageContent | types.EmbeddedResource]

def format_text_response(text: Any) -> ResponseType:
    """Format a text response."""
    return [types.TextContent(type="text", text=str(text))]


def format_error_response(error: str) -> ResponseType:
    """Format an error response."""
    return format_text_response(f"Error: {error}")

db_connection = PostgresConnection(config)
async def get_sql_driver() -> PostgresDriver:
    return PostgresDriver(db_connection, config)

@mcp.tool(description="Execute any SQL query")
async def execute_sql(
    sql: str = Field(description="SQL to run", default="all"),
) -> ResponseType:
    """Executes a SQL query against the database."""
    try:
        sql_driver = await get_sql_driver()
        rows = await sql_driver.execute_query(sql)  # type: ignore
        if rows is None:
            return format_text_response("No results")
        return format_text_response(list([r.cells for r in rows]))
    except Exception as e:
        return format_error_response(str(e))
    
# run the MCP server
async def main():
    try:
        await db_connection.connect()
        print("PostgreSQL connection established")
    except Exception as e:
        print(f"Failed to connect to PostgreSQL: {str(e)}")
    
    await mcp.run_stdio_async()
    
if __name__ == "__main__":
    asyncio.run(main())
        