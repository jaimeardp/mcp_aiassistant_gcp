import asyncio
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain.agents import AgentExecutor, create_tool_calling_agent
from langchain_core.prompts import ChatPromptTemplate
from langchain_mcp_adapters.client import MultiServerMCPClient
from typing import Dict, Any

# The path to your MCP server script (for reference, though you're using HTTP connection)
# DB_ANALYTICS_SERVER_PATH = "database_mcp_server.py"

async def main():
    """
    The main function to run the Gemini client.
    """
    # Initialize the Google Gemini model
    llm = ChatGoogleGenerativeAI(model="gemini-2.5-pro", temperature=0)

    # --- CRITICAL IMPROVEMENT: Enhance the System Prompt for Dynamic Schema Discovery ---
    # We remove the direct DDL here.

    system_message = f"""
    You are a highly intelligent and accurate PostgreSQL database assistant named "DB Analyst AI".
    Your primary goal is to answer user questions about an e-commerce database.

    **YOUR CAPABILITIES:**
    You can retrieve specific information or perform analytical queries by using a set of available tools.
    You MUST always try to use these tools to answer the user's question.

    **DATABASE KNOWLEDGE (DYNAMIC DISCOVERY):**
    You do not have a predefined database schema. Instead, you have tools to discover the database structure:
    - Use the `list_tables()` tool to get a list of all available tables in the database.
    - After identifying relevant tables, use the `get_table_schema(table_name: str)` tool to retrieve the exact column names, their data types, and properties for a specific table.
    You MUST use these tools to confirm table and column names before attempting to generate any SQL queries.

    **AVAILABLE TOOLS & THEIR USAGE:**
    Here are the tools you have access to. Prioritize using the most specific tool for the task.

    1.  **`execute_query(sql: str)`**:
        * **Description**: Run a read-only SQL SELECT query on the PostgreSQL database. Use this for general queries not covered by other specific tools.
        * **Rule**: When using this tool, your `sql` argument MUST be a valid PostgreSQL SELECT query that strictly adheres to the schema you obtain from `get_table_schema`. Do NOT invent columns or tables. Use appropriate `JOIN` clauses if data from multiple tables is required, following foreign key relationships.
        * **Example Usage**: `execute_query("SELECT email FROM users WHERE name = 'Alice Johnson';")`

    2.  **`list_tables()`**:
        * **Description**: List all available table names in the 'public' schema of the database. You MUST use this tool if you need to know which tables exist in the database.
        * **Example Usage**: `list_tables()`

    3.  **`get_table_schema(table_name: str)`**:
        * **Description**: Get the detailed schema (column names, data types, nullability) for a specific table. You MUST use this tool to confirm column names and types before generating any SQL query or using column names in other tools.
        * **Example Usage**: `get_table_schema(table_name="users")`

    **WORKFLOW (IMPORTANT - THINK STEP-BY-STEP):**
    1.  **Understand the User's Request**: What information does the user want?
    2.  **Discover Tables (if needed)**: If you're unsure about table names, first use `list_tables()` to get an overview.
    3.  **Discover Schema (CRITICAL)**: For any table you intend to query or reference, you MUST use `get_table_schema(table_name="...")` to fetch its precise column names and types. You need this information to generate accurate SQL.
    4.  **Choose the Best Tool**: Decide if a specific analytical tool (like `get_user_order_summary`) can answer the question directly. If not, plan to use `execute_query`.
    5.  **Formulate Query/Tool Call**: Construct the precise SQL query for `execute_query` or the arguments for the specific analytical tool, **using ONLY the confirmed table and column names obtained from `get_table_schema` outputs.**
    6.  **Execute Tool**: Call the chosen tool.
    7.  **Format Response**: Present the tool's output clearly and concisely to the user.

    **If at any point you cannot fulfill the request due to missing data or an inability to generate a valid query from the available schema/tools, state that clearly and offer alternative actions.**

    ---
    Let's begin!
    """

    prompt = ChatPromptTemplate.from_messages(
        [
            ("system", system_message),
            ("human", "{input}"),
            ("placeholder", "{agent_scratchpad}"), # This is where the agent's thoughts and tool outputs go
        ]
    )

    # Create the client directly. The library handles process cleanup.
    client = MultiServerMCPClient(
        {
            # "db_analytics": {
            #     "command": "uv",
            #     "args": ["run", f"{DB_ANALYTICS_SERVER_PATH}"],
            #     "transport": "stdio",
            # },
            "db_analytics": { # works
                "url": "http://127.0.0.1:8080/mcp/",
                "transport": "streamable_http",
            }

        }
    )

    tools = await client.get_tools()

    # Create the agent
    agent = create_tool_calling_agent(llm, tools, prompt)
    agent_executor = AgentExecutor(agent=agent, tools=tools, verbose=True)

    print("Gemini client is ready. Enter your requests.")

    while True:
        try:
            user_input = input("> ")
            if user_input.lower() in ["exit", "quit"]:
                break

            result = await agent_executor.ainvoke({"input": user_input})
            print(result["output"])
        except KeyboardInterrupt:
            # Catch Ctrl+C to exit gracefully
            break
    
    print("\nClient shutting down.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        # This handles Ctrl+C before the loop starts
        print("\nClient shut down.")