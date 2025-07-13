import asyncio
from fastmcp import Client
from fastmcp.integrations.googleai import load_mcp_tools
from langgraph.prebuilt import create_react_agent
from langchain.llms.ollama import Ollama

async def run():
    llm = Ollama(model="mistral")         # local, free
    client = Client("http://localhost:8000/mcp")
    tools = await load_mcp_tools(client)

    agent = create_react_agent(llm, tools)

    print(await agent.ainvoke("¿Qué tablas hay?"))

asyncio.run(run())
