from flask import Flask, jsonify, request
import redis
import subprocess
import os
import re
from dotenv import load_dotenv
from langchain.agents import create_react_agent, AgentExecutor
from langchain import hub
from langchain.tools import Tool
from langchain_google_genai import ChatGoogleGenerativeAI

# Your tool definitions (as provided before)
def start_redis():
    """Starts the Redis server."""
    print("Starting Redis...")
    return "Redis started successfully."

def stop_redis():
    """Stops the Redis server."""
    print("Stopping Redis...")
    return "Redis stopped successfully."

def delete_keys_by_pattern(pattern: str):
    """Deletes Redis keys matching a given pattern."""
    print(f"Deleting keys with pattern: {pattern}")
    return f"Deleted keys matching pattern: {pattern}"

def delete_producer_keys():
    """Deletes all Redis producer keys."""
    print("Deleting producer keys...")
    return "Producer keys deleted."

def delete_consumer_keys():
    """Deletes all Redis consumer keys."""
    print("Deleting consumer keys...")
    return "Consumer keys deleted."

def delete_all_keys():
    """Deletes all keys in Redis."""
    print("Deleting all keys...")
    return "All keys deleted."

def delete_specific_key(key: str):
    """Deletes a specific Redis key."""
    print(f"Deleting key: {key}")
    return f"Deleted key: {key}"

def get_panel_count():
    """Gets the current count of panels in Redis."""
    count = 10
    print(f"Getting panel count: {count}")
    return f"Panel count: {count}"

def get_total_keys():
    """Gets the total number of keys in Redis."""
    total = 100
    print(f"Getting total keys: {total}")
    return f"Total keys: {total}"

def get_panels_length():
    """Gets the length of the panels list in Redis."""
    length = 5
    print(f"Getting panels list length: {length}")
    return f"Panels list length: {length}"

def check_redis_status(*args, **kwargs):
    """Checks the status of the Redis server."""
    # In a real scenario, you would implement the logic to check Redis status
    status = "running"  # Simulate Redis is running
    return f"Redis is {status}."

load_dotenv()
tools = [
    Tool.from_function(func=start_redis, name="start_redis", description="Starts the Redis server."),
    Tool.from_function(func=stop_redis, name="stop_redis", description="Stops the Redis server."),
    Tool.from_function(func=delete_keys_by_pattern, name="delete_keys_by_pattern", description="Deletes Redis keys matching a given pattern."),
    Tool.from_function(func=delete_producer_keys, name="delete_producer_keys", description="Deletes all Redis producer keys."),
    Tool.from_function(func=delete_consumer_keys, name="delete_consumer_keys", description="Deletes all Redis consumer keys."),
    Tool.from_function(func=delete_all_keys, name="delete_all_keys", description="Deletes all keys in Redis."),
    Tool.from_function(func=delete_specific_key, name="delete_specific_key", description="Deletes a specific Redis key."),
    Tool.from_function(func=get_panel_count, name="get_panel_count", description="Gets the current count of panels in Redis."),
    Tool.from_function(func=get_total_keys, name="get_total_keys", description="Gets the total number of keys in Redis."),
    Tool.from_function(func=get_panels_length, name="get_panels_length", description="Gets the length of the panels list in Redis."),
    Tool.from_function(func=check_redis_status, name="check_redis_status", description="Checks the status of the Redis server if it is running or not."),
]
    
llm = ChatGoogleGenerativeAI(model="gemini-2.0-flash-001", google_api_key=os.getenv("GOOGLE_API_KEY"))
prompt = hub.pull("hwchase17/react")
print(prompt)
agent = create_react_agent(llm, tools, prompt)
agent_executor = AgentExecutor(agent=agent, tools=tools, verbose=True)

if __name__ == '__main__':
    user_query = "is redis running?"
    output = agent_executor.invoke({"input": user_query})
    print(output['output'])
