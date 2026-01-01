import psycopg2
import numpy as np
import json

def get_connection(dbname, autocommit=False):
    with open("../db_connection_conf.json") as f:
        conf = json.load(f)
    
    conf = {**conf, "database" : dbname}
    conn = psycopg2.connect(**conf)
    conn.set_session(autocommit=autocommit)
    cursor = conn.cursor()
    return conn, cursor

def close_connection(conn, cursor):
    cursor.close()
    conn.close()

def run_counting_sql(
    dbname, 
    sql, 
    disable_parallelism=True, 
    timeout_in_sec=None
):
    conn, cursor = get_connection(
        dbname=dbname, 
        autocommit=False
    )
    if disable_parallelism:
        cursor.execute(
            "SET max_parallel_workers_per_gather = 0;"
        )
    if timeout_in_sec is not None:
        cursor.execute(
            f"SET statement_timeout = {timeout_in_sec * 1000};"
        )
    cursor.execute(sql)
    count = cursor.fetchone()[0]
    close_connection(conn=conn, cursor=cursor)
    return count

def run_conditional_aggregation(
    dbname, 
    sql, 
    disable_parallelism=True, 
    timeout_in_sec=None
):
    conn, cursor = get_connection(
        dbname=dbname, 
        autocommit=False
    )
    if disable_parallelism:
        cursor.execute(
            "SET max_parallel_workers_per_gather = 0;"
        )
    if timeout_in_sec is not None:
        cursor.execute(
            f"SET statement_timeout = {timeout_in_sec * 1000};"
        )
    cursor.execute(sql)
    counts = cursor.fetchone()
    close_connection(conn=conn, cursor=cursor)
    return counts

def parse_plan(node):
    node_type = node['Node Type'].replace(" ", "")
    alias = f"({node['Alias']})" if 'Alias' in node else ""
    subnodes = [
        parse_plan(subnode) for subnode in node.get('Plans', [])
    ]

    if ('Join' in node_type or 'Loop' in node_type) \
        and len(subnodes) == 2:
        return f"{node_type}({subnodes[0]},{subnodes[1]})"
    elif 'Scan' in node_type:
        return f"{node_type}{alias}"
    else:
        return f"{node_type}({''.join(subnodes)})"
    
def get_latency(
    dbname, 
    sql, 
    n_repetitions=3, 
    disable_parallelism=True, 
    print_n_rows=False, 
    extract_plan_info=False,
    timeout_in_sec=None
):
    conn, cursor = get_connection(dbname=dbname, autocommit=False)
    if disable_parallelism:
        cursor.execute("SET max_parallel_workers_per_gather = 0;")
    if timeout_in_sec is not None:
        cursor.execute(
            f"SET statement_timeout = {timeout_in_sec * 1000};"
        )
    all_latencies = []
    for T in range(n_repetitions):
        cursor.execute(
            "EXPLAIN (ANALYZE, SUMMARY, COSTS, FORMAT JSON) " + sql
        )
        query_plan = cursor.fetchall()
        all_latencies.append(
            float(query_plan[0][0][0]['Execution Time']) / 1000.
            # ms to s
        ) 
        if print_n_rows:
            n_rows = int(query_plan[0][0][0]['Plan']['Actual Rows'])
        if extract_plan_info and T == 0:
            plan_info = parse_plan(query_plan[0][0][0]['Plan'])
    close_connection(conn=conn, cursor=cursor)
    if extract_plan_info:
        return np.median(all_latencies), plan_info
    elif print_n_rows:
        return np.median(all_latencies), n_rows
    else:
        return np.median(all_latencies)
    
def turn_on_extensions(cursor):
    # create the extensions for bucketization
    for _, data_type_alias in [
        ("integer", "int"), 
        ("timestamp without time zone", "ts"), 
        ("numeric", "float")
    ]:
        cursor.execute(
            "CREATE EXTENSION IF NOT EXISTS "
            f"find_bucket_{data_type_alias};"
        )

def turn_off_extensions(cursor):
    # drop the extensions for bucketization
    for _, data_type_alias in [
        ("integer", "int"), 
        ("timestamp without time zone", "ts"), 
        ("numeric", "float")
    ]:
        cursor.execute(
            "DROP EXTENSION IF EXISTS "
            f"find_bucket_{data_type_alias};"
        )
