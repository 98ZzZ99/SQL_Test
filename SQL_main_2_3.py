# LLM_Test/SQL_main_2_2.py

from SQL_utils import map_column_name, patch_query_conditions
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

# ======= Import the tool functions in tools/SQL_tools_2_2.py ======= #
from tools.SQL_tools_2_2 import (
    SQLQueryTool,
    SQLSortingTool,
    WorkTimeCalculateTool,
    AdditionTool,
    SubtractionTool,
    MultiplicationTool,
    DivisionTool,
    AveragingTool,
    ModeTool
)

from SQL_utils import unify_operations

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


# 3) Initializing model and tool
tools = [
    SQLQueryTool(),
    SQLSortingTool(),
    WorkTimeCalculateTool(),
    AdditionTool(),
    SubtractionTool(),
    MultiplicationTool(),
    DivisionTool(),
    AveragingTool(),
    ModeTool()
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


def my_unify_operations(operations: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Here we can add some custom processing based on unify_operations:
    1) If you see 'Subtraction' and output_column = 'Work_Time', forcibly change number_columns to ['End_Time','Start_Time'].
    2) Also make sure that the fields of the previous query contain 'End_Time' and 'Start_Time'.
    """
    print("[DEBUG] my_unify_operations called, original operations =>", operations)

    # First use unify_operations to make basic corrections
    unified_ops = unify_operations(operations)

    # To realize the calculation of the intermediate variable of work_time, but the actual effect is not satisfactory
    # (1) Find Subtraction => Work_Time
    used_work_time = False
    for op in unified_ops:
        if op["tool_name"] == "Subtraction":
            args = op.get("args", {})
            if args.get("output_column", "") == "Work_Time":
                # If number_columns is not 2, change it to End_Time, Start_Time
                if "number_columns" in args and len(args["number_columns"]) != 2:
                    print("[DEBUG] Detected Subtraction => Work_Time, fix columns to End_Time - Start_Time")
                    args["number_columns"] = ["End_Time", "Start_Time"]
                used_work_time = True

    # (2) If do use Work_Time, need to check End_Time / Start_Time when querying.
    if used_work_time:
        # Find the first "Query" operation and complete the fields
        for op in unified_ops:
            if op["tool_name"] == "Query":
                conds = op["args"].get("conditions", {})
                fields = conds.get("fields", [])
                # Add End_Time / Start_Time (if not already there)
                if "End_Time" not in fields:
                    fields.append("End_Time")
                if "Start_Time" not in fields:
                    fields.append("Start_Time")
                conds["fields"] = fields
                print("[DEBUG] Because used Work_Time, we also add End_Time, Start_Time to Query fields =>", fields)
                break

    print("[DEBUG] my_unify_operations final =>", unified_ops)
    return unified_ops


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
        You are an assistant that receives a user's request about database queries and mathematical operations.
        The user said: {user_input}

        Your task:
        1) Analyze the user's request and figure out what tools (Query, Sorting, Addition, Subtraction, Multiplication, Division, Mode, Averaging) are needed, which are binded with the model.
        2) Build a list of operations in JSON form, strictly with the format below.
        3) Return ONLY valid JSON (no markdown, no extra text).

        VERY IMPORTANT NOTE:
        There is NO column named Work_Time in any database or table! 
        If user mention the working time or Work_Time or other similar description, you MUST firstly do a Subtraction operation to produce Work_Time as a new column, like in following example!

        IMPORTANT:
        - If the user references or needs a column that doesn't literally exist, but is obviously a near-synonym or a different case, you should automatically correct it to the actual column name in the table. 
        - tool_name must be exactly one of ["Query","Sorting","Addition","Subtraction","Multiplication","Division","Mode","Averaging"] (case-sensitive).
        - Unless the compute objects/columns are the result of in previous calculation steps (like Work_Time), the only valid compute objects/columns should be one of ["ID","Name","Gender","Start_Time","End_Time","Plan_Number","Real_Number","Qualified_Number"].

        JSON Format:
        {
          "success": true,
          "operations": [
            {
              "tool_name": "...",   // One of ["Query","Sorting","Addition","Subtraction","Multiplication","Division","Mode","Averaging"]
              "args": {
                 // The exact arguments needed by that tool
              }
            },
            ...
          ]
        }

        Result passing / chaining:
        - If you want to use the result from a previous tool, reference it in the "args" explicitly, for example "args": {"data": "$result_of_previous_tool"}.
        - Make sure to specify a logical or descriptive placeholder that indicates you are using previous results.

        Error handling:
        - If a query returns an empty result, subsequent operations might produce 0 or empty results.
        - Tools should handle unexpected data formats gracefully where possible.

        Example Input:
        "I have a SQL database named Workers.db in path D:/Test/Dataset, which has a table named Sheet_17_02_2025. First, select all workers who start work before 9:00, subtract the start time from the end time to get the work time, then divide the number of qualified workpieces Qualified_Number by the work time to get the qualified workpiece KPI Qualified_KPI; then select all workers whose end time is later than 17:00, subtract the start time from the end time to get the work time, then divide the number of qualified workpieces Qualified_Number by the work time to get the qualified workpiece KPI Qualified_KPI; finally, list the data of all the above workers in descending order of Qualified_KPI."

        Example Output:
        {
          "success": true,
          "operations": [
            {
              "tool_name": "Query",
              "args": {
                "db_path": "D:/Test/Dataset/Workers.db",
                "conditions": {
                  "table": "Sheet_17_02_2025",
                  "fields": ["ID","Name","Start_Time","End_Time","Qualified_Number"],
                  "where": "Start_Time < '09:00' OR End_Time > '17:00'"
                }
              }
            },
            {
              "tool_name": "Subtraction",
              "args": {
                "data": "$result_of_previous_tool",
                "number_columns": ["End_Time","Start_Time"],
                "output_column": "Work_Time"
              }
            },
            {
              "tool_name": "Division",
              "args": {
                "data": "$result_of_previous_tool",
                "number_columns": ["Qualified_Number","Work_Time"],
                "output_column": "Qualified_KPI"
              }
            },
            {
              "tool_name": "Sorting",
              "args": {
                "data": "$result_of_previous_tool",
                "field_index": "Qualified_KPI",
                "reverse": true
              }
            }
          ]
        }

        Notes:
        - Return no other fields except "success" and "operations".
        - The structure must match the JSON Format strictly (no markdown, no extra keys).
        """
    )

    # ---- Log: Print Prompt ----
    final_prompt_text = parse_prompt.format(user_input=user_input)
    print("\n=== Final Prompt Text ===")
    print(final_prompt_text)
    print("=========================")

    # 2) Call LLM to get AIMessage
    prompt_model_chain = parse_prompt | parse_model
    try:
        ai_msg = prompt_model_chain.invoke({"user_input": user_input})
    except Exception as e:
        return {"messages": [AIMessage(content=f"LLM error: {e}")]}

    # ---- Log：Print LLM Return ----
    print("\n=== AI Message Returned ===")
    print(repr(ai_msg))
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

    # ---- KEY POINT!!! Make uniform corrections to all operations ----
    operations = my_unify_operations(operations)

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

        # ---- Replace data = "$result_of_previous_tool" with the real data here ----
        if "data" in tool_args and isinstance(tool_args["data"], str) and tool_args["data"] == "$result_of_previous_tool":
            # Find the most recent execution results
            last_result = None
            # Search from back to front
            for r in reversed(state["results"]):
                # r is of the form { "Query": [...], "Subtraction": [...], ...}
                # Here, just take the first value as data
                last_result = list(r.values())[0]
                break
            if last_result is None:
                state["messages"].append(AIMessage(content="No previous result found for substitution."))
                # Skip this operation
                state["pending_operations"].pop(0)
                continue
            tool_args["data"] = last_result
            print("[DEBUG] Replaced data with last_result =>", last_result[:5] if isinstance(last_result, list) else last_result)

        # ---- Perform the operation normally ----
        try:
            result = the_tool._run(**tool_args)
            print(f"[DEBUG] {tool_name}._run() => {result}")
            state["results"].append({tool_name: result})

            if tool_name == "Query":
                state["messages"].append(AIMessage(content=f"Query done. Rows={len(result)}"))
            elif tool_name == "Sorting":
                state["messages"].append(AIMessage(content=f"Sorting done. Rows={len(result)}"))
            else:
                # Other tools
                if isinstance(result, list):
                    state["messages"].append(AIMessage(content=f"{tool_name} done. Rows={len(result)}"))
                else:
                    state["messages"].append(AIMessage(content=f"{tool_name} done."))
        except Exception as e:
            state["messages"].append(AIMessage(content=f"Error running '{tool_name}': {e}"))

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
            "I have a database file in the path E:/LLMTest/Dataset, and is named by test_dataset.db, it has table Workers_20012025. "
            "Subtract Plan_Number from Real_Number, select the worker data with a positive result, and finally display it in descending order. "
        )
    )

    init_state = {
        "messages": [user_message],
        "pending_operations": [],
        "results": []
    }

    final_state = app.invoke(init_state)

    print("==== Workflow Ended ====")
    print("Final State:", final_state)
    if "results" in final_state:
        print("All results:", final_state["results"])


