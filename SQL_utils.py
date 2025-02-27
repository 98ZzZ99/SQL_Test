# my_project/SQL_main_2.py

import sqlite3
import difflib
from typing import Dict, Any, List

########################################
# 1) 全局同义词/大小写/列名映射
########################################

# 数据库实际列名列表（从 CREATE TABLE 可以看出）
REAL_COLUMNS = [
    "ID",
    "Name",
    "Gender",
    "Start_Time",
    "End_Time",
    "Plan_Number",
    "Real_Number",
    "Qualified_Number",
    "Others"
]

# 常见同义词/别称映射到真实列名
synonyms_map = {
    "id": "ID",
    "name": "Name",
    "gender": "Gender",
    "sex": "Gender",
    "start time": "Start_Time",
    "end time": "End_Time",
    "plan number": "Plan_Number",
    "predicted number": "Plan_Number",
    "real number": "Real_Number",
    "actual number": "Real_Number",
    "qualified number": "Qualified_Number",
    "qualified products": "Qualified_Number",
    "qualifiedproducts": "Qualified_Number",
    # ... etc.
}

########################################
# 2) 供 Query 使用的条件修正
########################################

def map_column_name(user_col: str) -> str:
    """
    将用户/模型输入的列名映射到实际数据库列名，先尝试小写匹配 synonyms_map，
    若没找到再用 difflib 进行近似匹配。
    """
    # 1) 若 synonyms_map 有确切的映射，则直接返回
    lower_col = user_col.lower()
    if lower_col in synonyms_map:
        return synonyms_map[lower_col]

    # 2) 否则使用 difflib 在真实列名 (全转小写) 中找最相近者
    real_cols_lower = [c.lower() for c in REAL_COLUMNS]
    matches = difflib.get_close_matches(lower_col, real_cols_lower, n=1, cutoff=0.6)
    if matches:
        matched_lower = matches[0]
        # 找到 matched_lower 在 real_cols_lower 中的索引
        idx = real_cols_lower.index(matched_lower)
        # 映射回真实列名
        return REAL_COLUMNS[idx]

    # 3) 实在找不到，就返回原值
    return user_col

def patch_query_conditions(conditions: Dict[str, Any]) -> Dict[str, Any]:
    """
    对 conditions 里的 table, fields, where 做进一步替换。
    - table 若与实际不符可以自行处理；这里假设 table 就是 "Workers_20012025" 之类不做映射
    - fields 是 list[str]，需要挨个列名修正
    - where 是 str，需要做一些简单正则或 split，找出列名并修正；也可只做大小写替换
      (若要更严格，可以解析 SQL 语句，但相对复杂)
    """
    new_conditions = dict(conditions)  # 复制
    # 1) 修正 fields
    if "fields" in new_conditions and isinstance(new_conditions["fields"], list):
        new_fields = []
        for col in new_conditions["fields"]:
            # map_column_name
            mapped = map_column_name(col)
            new_fields.append(mapped)
        new_conditions["fields"] = new_fields

    # 2) 修正 where 里的列名（简单做法）
    # 比如 "where Gender='Female'" -> "where Gender='Female'"
    # 如果 user 输入 "qualifiedproducts > 10" -> "Qualified_Number > 10"
    # 这里只做最简单替换：按空格/标点分割拿到词，可能并不完美
    if "where" in new_conditions and isinstance(new_conditions["where"], str):
        old_where = new_conditions["where"]
        # 我们简单地对可能的列名进行分词匹配:
        # 例如 old_where = "qualifiedproducts >= 10 and gender='Female'"
        # => words = ["qualifiedproducts", ">=", "10", "and", "gender='Female'"]
        words = old_where.replace("=", " = ").replace(">", " > ").replace("<", " < ") \
                         .replace("(", " ( ").replace(")", " ) ") \
                         .split()
        new_words = []
        for w in words:
            # 去掉标点后再映射
            stripped_w = w.strip("=><()'\"")  # 可能还要更多符号
            mapped = map_column_name(stripped_w)
            # 如果 w 原本带有标点，则还要还原
            # 这里很简略，只要 mapped != stripped_w 就替换，否则不变
            if mapped != stripped_w:
                # 这可能不够完善(例如当 w="qualifiedproducts>10" 没有空格)
                # 实际可以更精细化地处理
                w = w.replace(stripped_w, mapped)
            new_words.append(w)
        new_where = " ".join(new_words)
        new_conditions["where"] = new_where

    return new_conditions


########################################
# 3) Tool 名字的映射 (可选)
########################################

# 比如你想允许 LLM 生成 "arithmetic", "calc", "math" 都映射成 "Arithmetic"
TOOL_SYNONYMS = {
    "arithmetic": "Arithmetic",
    "calc": "Arithmetic",
    "sorting": "Sorting",
    "query": "Query",
    "work_time_calculate": "Work_Time_Calculate",
    # ...
}

def map_tool_name(raw_tool_name: str) -> str:
    """将 LLM 返回的 tool_name 做统一映射，避免大小写或别名问题。"""
    lower_name = raw_tool_name.lower()
    if lower_name in TOOL_SYNONYMS:
        return TOOL_SYNONYMS[lower_name]
    return raw_tool_name  # 如果找不到，就原样返回

########################################
# 4) 统一修正 operations
########################################
def unify_operations(operations: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    对 LLM 返回的全部 operations 做一个统一修正：
    1) 修正 tool_name
    2) 如果是 Query，调用 patch_query_conditions
    3) 其他想统一处理的都可以放这里
    """
    new_ops = []
    for op in operations:
        tool_name = op.get("tool_name", "")
        tool_args = op.get("args", {})

        # a) 修正 tool_name
        fixed_tool_name = map_tool_name(tool_name)
        op["tool_name"] = fixed_tool_name

        # b) 如果是 Query，则对 conditions 做 patch
        if fixed_tool_name == "Query":
            if "conditions" in tool_args and isinstance(tool_args["conditions"], dict):
                tool_args["conditions"] = patch_query_conditions(tool_args["conditions"])

        # c) 也可以做更多处理，比如自动映射 table 名大小写 / synonyms 等
        #    例如 if tool_args.get("conditions", {}).get("table", "").lower() == "workers_20012025"
        #    就替换成 "Workers_20012025"

        # 把处理后的 args 写回去
        op["args"] = tool_args
        new_ops.append(op)

    return new_ops





# from typing import Any
# from langchain.tools import BaseTool
#
# class ArithmeticTool(BaseTool):
#     """
#     A generic arithmetic tool to handle plus/minus/multiply/divide on columns or single values.
#
#     Example 'args':
#     {
#       "operation": "divide",   // could be 'multiply','plus','minus' etc.
#       "number_columns": ["Real_Number","Work_Time"],
#       "output_column": "Real_KPI"
#     }
#     or
#     {
#       "operation": "divide",
#       "number1": 50,
#       "number2": 5
#     }
#     """
#     name: str = "Arithmetic"
#     description: str = (
#         "Perform arithmetic (plus, minus, multiply, divide) on input data. "
#         "Args can contain 'operation' (str), 'number_columns' (List[str]) or single 'number1','number2'."
#     )
#
#     def _run(self, **kwargs) -> Any:
#         print(f"[DEBUG][ArithmeticTool] _run called with args={kwargs}")
#
#         operation = kwargs.get("operation", "").lower()
#
#         # Case 1: single numeric inputs
#         if "number1" in kwargs and "number2" in kwargs:
#             num1 = float(kwargs["number1"])
#             num2 = float(kwargs["number2"])
#             print(f"[DEBUG][ArithmeticTool] single numeric mode => {num1} {operation} {num2}")
#             return self._calc_single(num1, num2, operation)
#
#         # Case 2: columns-based operation (like dividing Real_Number by Work_Time for each row).
#         if "number_columns" in kwargs and "data" in kwargs:
#             data = kwargs["data"]       # shape: List[List[Any]]
#             col_names = kwargs["number_columns"]  # e.g. ["Real_Number","Work_Time"]
#             out_col = kwargs.get("output_column","Result")
#
#             print(f"[DEBUG][ArithmeticTool] columns-based mode => {col_names}, output_col={out_col}, data row samples={data[:3]}")
#
#             # [在此写您需要的逻辑，比如 col_names -> col_index 映射，然后循环 data 做运算...]
#             raise NotImplementedError("Column-based arithmetic not fully implemented. Provide an index-based approach or a mapping yourself.")
#
#         raise ValueError("Invalid arguments for ArithmeticTool. Must have 'number1'/'number2' or 'number_columns'/'data'")
#
#     def _calc_single(self, a: float, b: float, op: str) -> float:
#         if op == "divide":
#             return a / b if b != 0 else 0.0
#         elif op == "multiply":
#             return a * b
#         elif op == "plus":
#             return a + b
#         elif op == "minus":
#             return a - b
#         else:
#             raise ValueError(f"Unknown operation: {op}")
#
#     def _arun(self, *args, **kwargs):
#         raise NotImplementedError("Async run not implemented.")
#
#
# ############################
# # 用法示例
# ############################
#
# def run_example():
#     # 假设从大语言模型拿到:
#     # conditions = {
#     #    "table": "Workers_20012025",
#     #    "fields": ["id", "qualifiedproducts", "NAME"],
#     #    "where": "qualifiedproducts>=100 and gender='female'"
#     # }
#     # 先做映射
#     conditions = {
#         "table": "Workers_20012025",
#         "fields": ["id", "qualifiedproducts", "NAME"],
#         "where": "qualifiedproducts>=100 and gender='female'"
#     }
#
#     fixed_conditions = patch_query_conditions(conditions)
#     print("[DEBUG] after patch =>", fixed_conditions)
#     # => "fields": ["ID", "Qualified_Number", "Name"]
#     # => "where": "Qualified_Number >= 100 and Gender='Female'"
#
#     # 然后就可以执行 SQLQueryTool._run(**{"db_path": "...", "conditions": fixed_conditions})
#     # ...
#
# if __name__ == "__main__":
#     run_example()