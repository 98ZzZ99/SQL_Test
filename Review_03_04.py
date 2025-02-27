# my_project/Review_03_04.py

import re
import json
import operator
from typing import TypedDict, Annotated, Sequence, List, Dict, Any

from langchain_openai import ChatOpenAI
from langchain_core.messages import (
    BaseMessage,
    HumanMessage,
    AIMessage,
    SystemMessage,
    FunctionMessage,
)

from langchain_core.utils.function_calling import convert_to_openai_function
from langgraph.prebuilt import ToolNode, ToolInvocation
from langgraph.graph import StateGraph, END
from langchain_core.output_parsers import JsonOutputParser
from langchain_core.prompts import ChatPromptTemplate

# ======= Import the tools functions in tools/SQL_tools.py ======= #
from tools.Review_Tools_01_01 import (
    SQLQueryTool,
    SQLSortingTool,
    WorkTimeCalculateTool,
    PlanKPITool,
    RealKPITool,
    QualifiedKPITool,
    AveragingTool,
    ModeTool
)

# 1) Load .env, read OPENAI_API_KEY
from dotenv import load_dotenv
import os

# Load environment variables from .env
load_dotenv()

# Get OPENAI_API_KEY
openai_api_key = os.getenv("OPENAI_API_KEY")

if openai_api_key is None:
    raise ValueError("OPENAI_API_KEY is not set in the .env file")


# 2) Define the State structure used by workflow
class SQLAgentState(TypedDict):
    messages: Annotated[Sequence[BaseMessage], operator.add]
    pending_operations: Annotated[List[Dict[str, Any]], operator.add]
    results: Annotated[List[Dict[str, Any]], operator.add]


# 3) Initializing model, tool, and ToolExecutor
# model = ChatOpenAI(temperature=0.0, streaming=False, openai_api_key=openai_api_key)

tools = [
    SQLQueryTool(),
    SQLSortingTool(),
    WorkTimeCalculateTool(),
    PlanKPITool(),
    RealKPITool(),
    QualifiedKPITool(),
    AveragingTool(),
    ModeTool()
    # ... Other Tools
]

parse_model = ChatOpenAI(
    temperature=0.0,
    streaming=False,
    openai_api_key=openai_api_key
)  # No binding tools

execution_model = ChatOpenAI(
    temperature=0.0,
    streaming=False,
    openai_api_key=openai_api_key
).bind_tools(tools)

tool_node = ToolNode(tools)

# =========== 3) Define the functions of each node  =========== #
def agent_input(state: SQLAgentState) -> Dict:
    messages = state["messages"]
    if not messages:
        return {"messages": [AIMessage(content="No user input. End.")]}

    last_msg = messages[-1]
    if not isinstance(last_msg, HumanMessage):
        return {"messages": [AIMessage(content="Last message not from user. End.")]}

    user_input = last_msg.content

    # 1) Build Prompt
    parse_prompt = ChatPromptTemplate.from_template(
        """
        You are an assistant to parse the user's requirement into JSON.
        The user said: {user_input}

        You MUST return ONLY RAW JSON (no markdown, no explanations, no extra keys).
        The JSON should have the structure:

        {{
          "success": true,
          "operations": [
            {{
              "tool_name": "Query",           // or "Sorting", "Real_KPI", etc
              "args": {{
                "db_path": "E:/LLMTest/Dataset/test_dataset.db",
                "conditions": {{
                  "table": "workers",
                  "fields": ["*"],
                  "where": "Gender='female'"
                }}
              }}
            }},
            {{
              "tool_name": "Sorting",
              "args": {{
                "field_index": 3,
                "reverse": true
              }}
            }},
            ...
          ]
        }}

        DO NOT RETURN ANY FIELD named "action" or "criteria" or anything else;
        ONLY 'tool_name' and 'args' are allowed for each operation.

        Example Input:
        "I want all female workers from E:/LLMTest/Dataset/test_dataset.db and then sort them descending by KPI."

        Example Output:
        {{
          "success": true,
          "operations": [
            {{
              "tool_name": "Query",
              "args": {{
                "db_path": "E:/LLMTest/Dataset/test_dataset.db",
                "conditions": {{
                  "table": "workers",
                  "fields": ["*"],
                  "where": "Gender='female'"
                }}
              }}
            }},
            {{
              "tool_name": "Sorting",
              "args": {{
                "field_index": 3,
                "reverse": true
              }}
            }}
          ]
        }}
        """
    )

    # ---- Log: Print Prompt ----
    final_prompt_text = parse_prompt.format(user_input=user_input)
    print("\n=== Final Prompt Text ===")
    print(final_prompt_text)
    print("=========================")

    # 2) Call LLM to get AIMessage
    # prompt_model_chain = parse_prompt | model  # model=ChatOpenAI(...)
    # parse_prompt | parse_model
    prompt_model_chain = parse_prompt | parse_model
    try:
        # Get AIMessage
        ai_msg = prompt_model_chain.invoke({"user_input": user_input})
    except Exception as e:
        return {"messages": [AIMessage(content=f"LLM error: {e}")]}

    # ---- Log：Print LLM Return ----
    print("\n=== AI Message Returned ===")
    print(repr(ai_msg))  # ai_msg object，like AIMessage
    print("=== AI Message Content ===")
    print(ai_msg.content)
    print("==========================")

    raw_output = ai_msg.content.strip()
    if not raw_output:
        return {"messages": [AIMessage(content="LLM returned empty response.")]}

    # 3) JSON parsing
    try:
        parsed = json.loads(raw_output)
    except Exception as e:
        return {"messages": [AIMessage(content=f"Parsing error: {e}")]}

    # ---- Log: Print the parsed object ----
    print("\n=== Parsed JSON ===")
    print(parsed)
    print("===================")

    if not parsed.get("success", False):
        return {"messages": [AIMessage(content="LLM parse failed: 'success' != true ")]}

    operations = parsed.get("operations", [])
    for op in operations:
        state["pending_operations"].append(op)

    return {"messages": [AIMessage(content="Parsing successful.")]}

def check_agent_input_result(state: SQLAgentState) -> str:
    """
    Determine whether to continue:
    1) If pending_operations is not empty -> continue
    2) If it is empty, end
    """
    if not state["pending_operations"]:
        return "end"
    return "continue"


def single_executor_node(state: SQLAgentState) -> Dict:
    """
    Use one node to execute all pending_operations
    """
    if not state["pending_operations"]:
        return {"messages": [AIMessage(content="No operations to execute.")]}

    while state["pending_operations"]:
        next_op = state["pending_operations"][0]
        tool_name = next_op["tool_name"]
        tool_args = next_op.get("args", {})

        the_tool = None
        for t in tools:
            if t.name == tool_name:
                the_tool = t
                break

        if not the_tool:
            err_msg = f"Tool '{tool_name}' not found in tools."
            state["messages"].append(AIMessage(content=err_msg))
            state["pending_operations"].pop(0)
            continue

        print(f"[DEBUG] Execute {tool_name}._run() with args: {tool_args}")

        if tool_name == "Query":
            try:
                result = the_tool._run(**tool_args)
                print(f"[DEBUG] Query._run() => {result}")
                # Put the results in state["results"]
                state["results"].append({tool_name: result})
                # Record it in messages
                state["messages"].append(AIMessage(content=f"Query done. Rows={len(result)}"))
            except Exception as e:
                state["messages"].append(AIMessage(content=f"Error running 'Query': {e}"))

        elif tool_name == "Sorting":
            try:
                # If there is no data parameter, the result of the previous query is automatically used.
                if "data" not in tool_args:
                    # Assume the last operation must be Query
                    # Or we can iterate over state["results"] to find the last Query
                    last_query_result = None
                    for r in reversed(state["results"]):
                        if "Query" in r:
                            last_query_result = r["Query"]
                            break
                    if last_query_result is None:
                        raise ValueError("No Query result found to sort.")

                    tool_args["data"] = last_query_result

                result = the_tool._run(**tool_args)
                print(f"[DEBUG] Sorting._run() => {result}")
                state["results"].append({tool_name: result})
                state["messages"].append(AIMessage(content=f"Sorting done. Rows={len(result)}"))
            except Exception as e:
                state["messages"].append(AIMessage(content=f"Error running 'Sorting': {e}"))

        else:
            try:
                # Similar processing in other tools
                result = the_tool._run(**tool_args)
                print(f"[DEBUG] {tool_name}._run() => {result}")
                state["results"].append({tool_name: result})
                state["messages"].append(AIMessage(content=f"{tool_name} done."))
            except Exception as e:
                state["messages"].append(AIMessage(content=f"Error running '{tool_name}': {e}"))

        # pop
        state["pending_operations"].pop(0)

    return {"messages": [AIMessage(content="All operations done.")]}


# =========== 4) Build a graphical workflow =========== #
workflow = StateGraph(SQLAgentState)

workflow.add_node("agent_input", agent_input)
workflow.add_node("executor_node", single_executor_node)

workflow.add_conditional_edges(
    "agent_input",
    check_agent_input_result,
    {
        "continue": "executor_node",
        "end": END
    }
)

# All operations are executed at once in executor_node, and then return directly to END without going to other nodes in the return of single_executor_node
workflow.add_edge("executor_node", END)

workflow.set_entry_point("agent_input")

app = workflow.compile()

# =========== 5) Demonstrate =========== #
if __name__ == "__main__":
    user_message = HumanMessage(
        content=(
            "I have a database file in following adress: E:\\LLMTest\\Dataset, name of table is Workers_20012025,"
            "its name is test_dataset.db or test_dataset.sqbpro, I do not know which one is correct. I want to get the data of all female workers firstly. "
            "Besides I need the KPI of all workers, and sort them in descending order."
        )
    )

    # Initial state
    init_state = {
        "messages": [user_message],
        "pending_operations": [],
        "results": []
    }

    # Run workflow
    final_state = app.invoke(init_state)

    # Output
    print("==== Workflow Ended ====")
    print("Final State:", final_state)
    if "results" in final_state:
        print("All results:", final_state["results"])