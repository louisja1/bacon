import sqlglot
import time
from datetime import datetime, timedelta

import sqlglot.expressions

def parse_one_imdb_or_stats_or_dsb(sql):
    start_ts = time.time()
    parsed = sqlglot.parse_one(sql)

    sqlglot_tables = parsed.find_all(sqlglot.expressions.Table)
    tab_to_alias = {}
    alias_to_tab = {}
    for t in sqlglot_tables:
        tab_to_alias[t.name] = t.alias
        alias_to_tab[t.alias] = t.name

    if parsed.find(sqlglot.expressions.Where):
        where_clause = parsed.find(sqlglot.expressions.Where).sql().lstrip("WHERE").strip()
    else:
        where_clause = None

    join_conds = []
    column_to_selections = {}

    if where_clause:
        if " OR " in where_clause or " NOT " in where_clause:
            raise ValueError(f"Cannot handle NOT/OR")
        conditions = [chunk.strip() for chunk in where_clause.split("AND")]

        for cond in conditions:
            if ">=" in cond:
                op = ">="
            elif "<=" in cond:
                op = "<="
            elif ">" in cond:
                op = ">"
            elif "<" in cond:
                op = "<"
            elif "=" in cond:
                op = "="
            else: 
                raise ValueError(f"Condition not supported: {cond}")
            left, right = cond.split(op)[0].strip().lower(), cond.split(op)[1].strip().lower()
            n_dot_in_left = left.count(".")
            n_dot_in_right = right.count(".")
            if n_dot_in_left > 1 or n_dot_in_right > 1:
                raise ValueError(f"Condition not supported: {cond}")
            if n_dot_in_left != 1:
                raise ValueError(f"Condition not supported: {cond}")
            if n_dot_in_left + n_dot_in_right == 2 and op != "=":
                # it is a join but not equality join
                raise ValueError(f"Join type not supported: {cond}")
            if n_dot_in_left < n_dot_in_right:
                left, right = right, left
                n_dot_in_left, n_dot_in_right = n_dot_in_right, n_dot_in_left
            if n_dot_in_right == 1 and right.split(".")[0] < left.split(".")[0]:
                left, right = right, left
                n_dot_in_left, n_dot_in_right = n_dot_in_right, n_dot_in_left

            if n_dot_in_left + n_dot_in_right == 2:
                join_conds.append((left, op, right))
            else:
                if left not in column_to_selections:
                    column_to_selections[left] = []
                column_to_selections[left].append(op)
                column_to_selections[left].append(right)

        for left in column_to_selections:
            if len(column_to_selections[left]) == 2:
                pass
            elif len(column_to_selections[left]) == 4:
                if column_to_selections[left][2] == ">" or column_to_selections[left][2] == ">=":
                    column_to_selections[left][0], column_to_selections[left][1] = column_to_selections[left][2], column_to_selections[left][3]
                if column_to_selections[left][2] not in ["<", "<="]:
                    raise ValueError(f"Single-table selection not supported: {left} {column_to_selections[left]}")
            else:
                raise ValueError(f"Single-table selection not supported: {left} {column_to_selections[left]}")
    return time.time() - start_ts, join_conds, column_to_selections, tab_to_alias, alias_to_tab

def parse(val_in_str, col_datatype):
    if col_datatype in ["integer", "smallint"]:
        val = int(val_in_str)
    elif col_datatype == "timestamp without time zone":
        val = datetime.strptime(val_in_str.split("'")[1], '%Y-%m-%d %H:%M:%S')
    elif col_datatype == "numeric":
        val = float(val_in_str)
    else:
        raise ValueError(f"Value {val_in_str} cannot be parsed with datatype {col_datatype}")
    return val

def nxt_val(val, col_datatype):
    if col_datatype in ["integer", "smallint"]:
        return val + 1
    elif col_datatype == "timestamp without time zone":
        return val + timedelta(microseconds=1)
    elif col_datatype == "numeric":
        return val + 0.01
    else:
        raise ValueError(f"Value {val} cannot be parsed with datatype {col_datatype}")

def prev_val(val, col_datatype):
    if col_datatype in ["integer", "smallint"]:
        return val - 1
    elif col_datatype == "timestamp without time zone":
        return val - timedelta(microseconds=1)
    elif col_datatype == "numeric":
        return val - 0.01
    else:
        raise ValueError(f"Value {val} cannot be parsed with datatype {col_datatype}")
