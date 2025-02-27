# my_project/tools/Review_Tools_03_01.py

from typing import List, Any, Dict
import sqlite3
from langchain.tools import BaseTool


class SQLQueryTool(BaseTool):
    name: str = "Query"  # All field definitions, including overrides, require a type annotation ! ! !
    description: str = (
        "Perform a basic SQL query on a given database. "
        "Args should contain 'db_path' and 'conditions' (if any)."
    )

    class SQLQueryTool(BaseTool):
        name: str = "Query"  # All field definitions, including overrides, require a type annotation ! ! !
        description: str = (
            "Perform a basic SQL query on a given database. "
            "Args should contain 'db_path' and 'conditions' (if any)."
        )

        def _run(self, db_path: str, conditions: Dict[str, Any] = None) -> List[Any]:
            """
            Used to execute database queries, such as SELECT.
            conditions could be a dict，for example:
            {
                "table": "workers_20012025",
                "fields": ["*"],
                "where": "Gender='female'"
            }
            """
            print(f"[DEBUG] SQLQueryTool _run called with db_path={db_path}")
            print(f"[DEBUG] conditions={conditions}")
            try:
                conn = sqlite3.connect(db_path)
                cursor = conn.cursor()
                table = conditions.get("table", "")
                fields = conditions.get("fields", ["*"])
                where_clause = conditions.get("where", None)

                conn = sqlite3.connect(db_path)
                cursor = conn.cursor()

                sql_fields = ", ".join(fields)
                sql_query = f"SELECT {sql_fields} FROM {table}"
                if where_clause:
                    sql_query += f" WHERE {where_clause}"

                cursor.execute(sql_query)
                rows = cursor.fetchall()
                print(f"[DEBUG] rows => {rows}")
                conn.close()

                return rows

            except Exception as e:
                print("[ERROR] SQLQueryTool encountered exception:", e)
                return []

        def _arun(self, *args, **kwargs):
            raise NotImplementedError("Async run not implemented.")


class SQLSortingTool(BaseTool):
    name: str = "Sorting"
    description: str = (
        "Sort the given list of data by a specified field, "
        "in ascending or descending order."
    )

    def _run(self, data: List[List[Any]], field_index: int, reverse: bool = False) -> List[List[Any]]:
        """
        data: in the form of[[col1, col2, ...], [col1, col2, ...], ...]
        field_index: Sort by the column, for example, 1 represents data[i][1]
        reverse: True indicates descending order
        """
        sorted_data = sorted(data, key=lambda x: x[field_index], reverse=reverse)
        return sorted_data

    def _arun(self, *args, **kwargs):
        raise NotImplementedError("Async run not implemented.")


class WorkTimeCalculateTool(BaseTool):
    name: str = "Work_Time_Calculate"
    description: str = "Calculate working time from hh:mm format to total minutes."

    def _run(self, time_data: List[str]) -> List[int]:
        """
        time_data: ["09:00", "18:30", ...]
        Return the corresponding working hours (in minutes), for example [540, 1110, ...]
        """
        result = []
        for t_str in time_data:
            hh, mm = t_str.split(":")
            minutes = int(hh) * 60 + int(mm)
            result.append(minutes)
        return result

    def _arun(self, *args, **kwargs):
        raise NotImplementedError("Async run not implemented.")


class PlanKPITool(BaseTool):
    name: str = "Plan_KPI"
    description: str = "Calculate Plan KPI = Plan_Number / Work_Time"

    def _run(self, plan_number: int, work_time: int) -> float:
        if work_time == 0:
            return 0.0
        return plan_number / float(work_time)

    def _arun(self, *args, **kwargs):
        raise NotImplementedError("Async run not implemented.")


class RealKPITool(BaseTool):
    name: str = "Real_KPI"
    description: str = "Calculate Real KPI = Real_Number / Work_Time"

    def _run(self, real_number: int, work_time: int) -> float:
        if work_time == 0:
            return 0.0
        return real_number / float(work_time)

    def _arun(self, *args, **kwargs):
        raise NotImplementedError("Async run not implemented.")


class QualifiedKPITool(BaseTool):
    name: str = "Qualified_KPI"
    description: str = "Calculate Qualified KPI = Qualified_Number / Work_Time"

    def _run(self, q_number: int, work_time: int) -> float:
        if work_time == 0:
            return 0.0
        return q_number / float(work_time)

    def _arun(self, *args, **kwargs):
        raise NotImplementedError("Async run not implemented.")


class AveragingTool(BaseTool):
    name: str = "Averaging"
    description: str = (
        "Compute the average of a list of numeric values. "
        "Args should be something like {'data': [1,2,3]}."
    )

    def _run(self, data: List[float]) -> float:
        if not data:
            return 0.0
        return sum(data) / len(data)

    def _arun(self, *args, **kwargs):
        raise NotImplementedError("Async run not implemented.")


class ModeTool(BaseTool):
    name: str = "Mode"
    description: str = (
        "Find the most common value (mode) of a list of numeric or string values."
    )

    def _run(self, data: List[Any]) -> Any:
        """
        data can be a number or a string, etc. Here we simply use collections.Counter to count the value with the highest number of occurrences.
        """
        if not data:
            return None
        from collections import Counter
        c = Counter(data)
        # most_common(1) -> [(value, count)]
        return c.most_common(1)[0][0]

    def _arun(self, *args, **kwargs):
        raise NotImplementedError("Async run not implemented.")