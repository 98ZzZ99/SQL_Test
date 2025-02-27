# LLM_Test/tools/SQL_tools_2_2.py

import traceback
from typing import List, Any, Dict
import sqlite3
from langchain.tools import BaseTool


def parse_time_string(time_str: str) -> float:
    """
    Simple example: Parse 'HH:MM' into minutes (float).
    If it is not in 'HH:MM' format, try to convert it directly to float.
    """
    if isinstance(time_str, str) and ":" in time_str:
        hh, mm = time_str.split(":")
        return float(hh) * 60.0 + float(mm)
    else:
        # If not in time format, try to convert it directly to float
        return float(time_str)


class SQLQueryTool(BaseTool):
    name: str = "Query"
    description: str = (
        "Perform a basic SQL query on a given database. "
        "Args should contain 'db_path' and 'conditions' (if any)."
    )

    def _run(self, db_path: str, conditions: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        """
        Used to execute database queries, such as SELECT.
        conditions could be a dict, such as:
        {
            "table": "workers_20012025",
            "fields": ["*"],
            "where": "Gender='female'"
        }
        Return: [ {col1: val1, col2: val2, ...}, ... ]
        """
        print(f"[DEBUG][Query] _run called with db_path={db_path}")
        print(f"[DEBUG][Query] conditions={conditions}")

        if conditions is None:
            conditions = {}

        table = conditions.get("table", "")
        fields = conditions.get("fields", ["*"])
        where_clause = conditions.get("where", None)

        print(f"[DEBUG][Query] table={table}, fields={fields}, where_clause={where_clause}")

        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        sql_fields = ", ".join(fields)
        sql_query = f"SELECT {sql_fields} FROM {table}"
        if where_clause:
            sql_query += f" WHERE {where_clause}"
        print(f"[DEBUG][Query] final SQL => {sql_query}")

        rows = []
        columns = []
        try:
            cursor.execute(sql_query)
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
        except Exception as ex:
            print("[ERROR][Query] Exception during SQL execution:", ex)
            traceback.print_exc()
        finally:
            conn.close()

        # Convert rows to a list [dic]
        dict_list = []
        for row in rows:
            row_dict = dict(zip(columns, row))
            dict_list.append(row_dict)

        print(f"[DEBUG][Query] returned dict_list => {dict_list[:5]}... (show first 5)")
        return dict_list

    def _arun(self, *args, **kwargs):
        print("[WARN][Query] Async run not implemented.")
        raise NotImplementedError("Async run not implemented.")


class SQLSortingTool(BaseTool):
    name: str = "Sorting"
    description: str = (
        "Sort the given list of data by a specified field, "
        "in ascending or descending order."
    )

    def _run(self, data: List[Dict[str, Any]], field_index, reverse: bool = False) -> List[Dict[str, Any]]:
        """
        data: list of dict
        field_index: might be an integer subscript (in old code) or a string
        """
        print(f"[DEBUG][Sorting] _run called with field_index={field_index}, reverse={reverse}")
        print(f"[DEBUG][Sorting] data preview => {data[:3]}")  # Only print the first 3 lines to avoid too much output

        if isinstance(field_index, str):
            # Sort by column name
            sorted_data = sorted(
                data,
                key=lambda x: x.get(field_index, None),
                reverse=reverse
            )
        elif isinstance(field_index, int):
            # In old code, if each row is a list
            # But now we change row to dict and no longer use this pattern
            print("[WARN][Sorting] field_index is int, but data is list[dict]. Handling might fail.")
            sorted_data = data
        else:
            print("[ERROR][Sorting] field_index must be str or int.")
            sorted_data = data

        print(f"[DEBUG][Sorting] sorted_data preview => {sorted_data[:3]}")
        return sorted_data

    def _arun(self, *args, **kwargs):
        print("[WARN][Sorting] Async run not implemented.")
        raise NotImplementedError("Async run not implemented.")


class WorkTimeCalculateTool(BaseTool):
    name: str = "WorkTimeCalculate"
    description: str = "Calculate working time from hh:mm format to total minutes."

    def _run(self, time_data: List[str]) -> List[int]:
        print(f"[DEBUG][WorkTimeCalculate] _run with time_data={time_data[:5]} ... (showing first 5)")
        result = []
        for t_str in time_data:
            hh, mm = t_str.split(":")
            minutes = int(hh) * 60 + int(mm)
            result.append(minutes)
        print(f"[DEBUG][WorkTimeCalculate] result => {result[:5]} ...")
        return result

    def _arun(self, *args, **kwargs):
        print("[WARN][WorkTimeCalculate] Async run not implemented.")
        raise NotImplementedError("Async run not implemented.")


class AdditionTool(BaseTool):
    """
    A tool to perform addition of two numbers or columns.
    Example 'args':
    {
      "number1": 50,
      "number2": 5
    }
    or
    {
      "data": ...,
      "number_columns": ["Plan_Number","Real_Number"],
      "output_column": "ResultOfAddition"
    }
    """
    name: str = "Addition"
    description: str = (
        "Perform addition on input data. "
        "Args can contain 'number1','number2' for direct calculation, "
        "or 'data','number_columns','output_column' for row-wise addition."
    )

    def _run(self, **kwargs) -> Any:
        print(f"[DEBUG][AdditionTool] _run called with args={kwargs}")

        # Case 1: single numeric inputs
        if "number1" in kwargs and "number2" in kwargs:
            num1 = float(kwargs["number1"])
            num2 = float(kwargs["number2"])
            print(f"[DEBUG][AdditionTool] Single input: num1={num1}, num2={num2}")
            result = num1 + num2
            print(f"[DEBUG][AdditionTool] Calculation result={result}")
            return result

        # Case 2: columns-based operation
        if "number_columns" in kwargs and "data" in kwargs and "output_column" in kwargs:
            data = kwargs["data"]
            col1, col2 = kwargs["number_columns"]
            output_col = kwargs["output_column"]
            print(f"[DEBUG][AdditionTool] Column-based addition: col1={col1}, col2={col2}, output_col={output_col}")

            for row in data:
                val1 = parse_time_string(row[col1])
                val2 = parse_time_string(row[col2])
                row[output_col] = val1 + val2
            return data

        print("[ERROR][AdditionTool] Invalid arguments provided.")
        raise ValueError("Invalid arguments for AdditionTool.")


class SubtractionTool(BaseTool):
    """
    A tool to perform subtraction of two numbers or columns.
    Example 'args':
    {
      "number1": 50,
      "number2": 5
    }
    or
    {
      "data": ...,
      "number_columns": ["End_Time","Start_Time"],
      "output_column": "Work_Time"
    }
    """
    name: str = "Subtraction"
    description: str = (
        "Perform subtraction on input data. "
        "Args can contain 'number1','number2' for direct calculation, "
        "or 'data','number_columns','output_column' for row-wise subtraction."
    )

    def _run(self, **kwargs) -> Any:
        print(f"[DEBUG][SubtractionTool] _run called with args={kwargs}")

        # Case 1: single numeric inputs
        if "number1" in kwargs and "number2" in kwargs:
            num1 = float(kwargs["number1"])
            num2 = float(kwargs["number2"])
            print(f"[DEBUG][SubtractionTool] Single input: num1={num1}, num2={num2}")
            result = num1 - num2
            print(f"[DEBUG][SubtractionTool] Calculation result={result}")
            return result

        # Case 2: columns-based operation
        if "number_columns" in kwargs and "data" in kwargs and "output_column" in kwargs:
            data = kwargs["data"]
            col1, col2 = kwargs["number_columns"]
            output_col = kwargs["output_column"]
            print(f"[DEBUG][SubtractionTool] Column-based subtraction: col1={col1}, col2={col2}, output_col={output_col}")

            for row in data:
                val1 = parse_time_string(row[col1])
                val2 = parse_time_string(row[col2])
                row[output_col] = val1 - val2
            return data

        print("[ERROR][SubtractionTool] Invalid arguments provided.")
        raise ValueError("Invalid arguments for SubtractionTool.")


class MultiplicationTool(BaseTool):
    """
    A tool to perform multiplication of two numbers or columns.
    Example 'args':
    {
      "number1": 10,
      "number2": 5
    }
    or
    {
      "data": ...,
      "number_columns": ["A","B"],
      "output_column": "Product"
    }
    """
    name: str = "Multiplication"
    description: str = (
        "Perform multiplication on input data. "
        "Args can contain 'number1','number2' for direct calculation, "
        "or 'data','number_columns','output_column' for row-wise multiplication."
    )

    def _run(self, **kwargs) -> Any:
        print(f"[DEBUG][MultiplicationTool] _run called with args={kwargs}")

        # Case 1: single numeric inputs
        if "number1" in kwargs and "number2" in kwargs:
            num1 = float(kwargs["number1"])
            num2 = float(kwargs["number2"])
            print(f"[DEBUG][MultiplicationTool] Single input: num1={num1}, num2={num2}")
            result = num1 * num2
            print(f"[DEBUG][MultiplicationTool] Calculation result={result}")
            return result

        # Case 2: columns-based operation
        if "number_columns" in kwargs and "data" in kwargs and "output_column" in kwargs:
            data = kwargs["data"]
            col1, col2 = kwargs["number_columns"]
            output_col = kwargs["output_column"]
            print(f"[DEBUG][MultiplicationTool] Column-based multiplication: col1={col1}, col2={col2}, output_col={output_col}")

            for row in data:
                val1 = parse_time_string(row[col1])
                val2 = parse_time_string(row[col2])
                row[output_col] = val1 * val2
            return data

        print("[ERROR][MultiplicationTool] Invalid arguments provided.")
        raise ValueError("Invalid arguments for MultiplicationTool.")


class DivisionTool(BaseTool):
    """
    A tool to perform division of two numbers or columns.
    Example 'args':
    {
      "number1": 50,
      "number2": 5
    }
    or
    {
      "data": ...,
      "number_columns": ["Qualified_Number","Work_Time"],
      "output_column": "Qualified_KPI"
    }
    """
    name: str = "Division"
    description: str = (
        "Perform division on input data. "
        "Args can contain 'number1','number2' for direct calculation, "
        "or 'data','number_columns','output_column' for row-wise division."
    )

    def _run(self, **kwargs) -> Any:
        print(f"[DEBUG][DivisionTool] _run called with args={kwargs}")

        # Case 1: single numeric inputs
        if "number1" in kwargs and "number2" in kwargs:
            num1 = float(kwargs["number1"])
            num2 = float(kwargs["number2"])
            print(f"[DEBUG][DivisionTool] Single input: num1={num1}, num2={num2}")
            if num2 == 0:
                print("[WARN][DivisionTool] Divisor is zero, return 0.0 to avoid crash.")
                return 0.0
            result = num1 / num2
            print(f"[DEBUG][DivisionTool] Calculation result={result}")
            return result

        # Case 2: columns-based operation
        if "number_columns" in kwargs and "data" in kwargs and "output_column" in kwargs:
            data = kwargs["data"]
            col1, col2 = kwargs["number_columns"]
            output_col = kwargs["output_column"]
            print(f"[DEBUG][DivisionTool] Column-based division: col1={col1}, col2={col2}, output_col={output_col}")

            for row in data:
                val1 = parse_time_string(row[col1])
                val2 = parse_time_string(row[col2])
                if val2 == 0:
                    print("[WARN][DivisionTool] Divisor is zero in row, use 0.0 instead.")
                    row[output_col] = 0.0
                else:
                    row[output_col] = val1 / val2

            return data

        print("[ERROR][DivisionTool] Invalid arguments provided.")
        raise ValueError("Invalid arguments for DivisionTool.")


class AveragingTool(BaseTool):
    name: str = "Averaging"
    description: str = (
        "Compute the average of a list of numeric values. "
        "Args should be something like {'data': [1,2,3]}."
    )

    def _run(self, data: List[float]) -> float:
        print(f"[DEBUG][AveragingTool] data => {data[:5]} ...")
        if not data:
            return 0.0
        val = sum(data) / len(data)
        print(f"[DEBUG][AveragingTool] return => {val}")
        return val

    def _arun(self, *args, **kwargs):
        print("[WARN][AveragingTool] Async run not implemented.")
        raise NotImplementedError("Async run not implemented.")


class ModeTool(BaseTool):
    name: str = "Mode"
    description: str = (
        "Find the most common value (mode) of a list of numeric or string values."
    )

    def _run(self, data: List[Any]) -> Any:
        print(f"[DEBUG][ModeTool] data => {data[:5]} ...")
        if not data:
            return None
        from collections import Counter
        c = Counter(data)
        most_common_val, count = c.most_common(1)[0]
        print(f"[DEBUG][ModeTool] return => {most_common_val}, count={count}")
        return most_common_val

    def _arun(self, *args, **kwargs):
        print("[WARN][ModeTool] Async run not implemented.")
        raise NotImplementedError("Async run not implemented.")




