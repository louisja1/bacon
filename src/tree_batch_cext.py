# Process all the counting queries by tree
# Move more loads to PG side

from tqdm import tqdm
from datetime import datetime
from collections import defaultdict
import numpy as np
import time
import json
import sys
import copy
from utility import parse_one_imdb_or_stats_or_dsb
from db_connection import (
    get_connection, 
    close_connection,
    turn_on_extensions,
    turn_off_extensions
)
from workload_summary import WorkloadSummary
from itertools import product, islice
from numba import njit


def add_the_product_of_multiple_hists_to(
    singletons, 
    unified_hist
):
    for combination in product(
        *(singleton.keys() for singleton in singletons)
    ):
        cnt = 1
        for i, singleton in enumerate(singletons):
            cnt *= singleton[combination[i]]
        unified_hist[sum(combination, ())] += cnt

# Input:
# `point_chunks`: shape (n_points, D), the coordinates
# `weights_chunk`: shape (n_points,), the weights of 
#     each coordinate
# `lower_bounds`: shape (n_queries, D), the lower bounds 
#     of each query
# `upper_bounds`: shape (n_queries, D), the upper bounds 
#     of each query

@njit
def batch_query(
    points_chunk, 
    weights_chunk, 
    lower_bounds, 
    upper_bounds
):
    n_points = points_chunk.shape[0]
    n_queries = lower_bounds.shape[0]
    D = points_chunk.shape[1]

    query_weight_sums = np.zeros(
        n_queries, 
        dtype=np.int64
    )

    for i in range(n_points):
        for q in range(n_queries):
            inside = True
            for d in range(D):
                if points_chunk[i, d] < lower_bounds[q, d] \
                    or points_chunk[i, d] > upper_bounds[q, d]:
                    inside = False
                    break
            if inside:
                query_weight_sums[q] += weights_chunk[i]

    return query_weight_sums

# run the recursive algorithm on a node `alias` over the tree
#     originated from the JoinPattern `jp`
def run(
    alias, 
    conn, 
    jp, 
    pattern_id,
    workload_summary, 
    alias_to_sel_col,
    alias_to_tab, 
    col_to_func,
    given_vals, 
    given_vals_as_a_set,
    batch_size,
    info,
):
    if given_vals is not None:
        unified_hist = defaultdict(
            lambda: defaultdict(int)
        ) # {given_val : {}}
        if len(given_vals) == 0:
            return unified_hist
    else:
        unified_hist = defaultdict(int)
        
    if given_vals is not None and len(given_vals) <= batch_size:
        scan_cursor = conn.cursor()
    else:
        scan_cursor = conn.cursor(name="scan_cursor_of_" + alias)
    
    # constant optimization
    this_alias_sel_cols = alias_to_sel_col[alias]
    this_alias_join_cols = jp.alias_to_join_col[alias]
    n_join_col = len(this_alias_join_cols)


    ######################
    # SQL looks like
    # SELECT 
    #     <join_cols>, <bkt_ids>, COUNT(*) as cnt
    # FROM (
    #     SELECT
    #            join_col0, 
    #            ..., 
    #            join_col(N-1), 
    #            col_to_func[sel_col0](
    #               sel_col0, %s, flag
    #            ) AS bkt(0)_id, 
    #            ..., 
    #            col_to_func[sel_col(M-1)](
    #               sel_col(M-1), %s, flag
    #            ) AS bkt(M - 1)_id
    #         FROM 
    #             alias_to_tab[alias] alias
    #         WHERE 
    #             jp.alias_to_join_col "within the range of" given_vals 
    #             AND "other join cols" IS NOT NULL
    # )
    # WHERE <bkt_ids> IS NOT NULL
    # GROUP BY <join_cols>, <bkt_ids>
    # ORDER BY <join_cols>
    # ;

    # -- SELECT CLAUSE
    outer_q = "SELECT \n\t" 
    q = "SELECT \n\t" 
    select_chunks = []
    outer_select_chunks = []
    params = []
    for join_col in this_alias_join_cols:
        pure_col = join_col.split(".")[1] # remove the prefix "{alias}."
        select_chunks.append(pure_col)
        outer_select_chunks.append(pure_col)
    
    n_bkt_ids = 0
    for i, sel_col in enumerate(this_alias_sel_cols):
        pure_col = sel_col.split(".")[1] # remove the prefix "{alias}."
        # flag: TRUE/FALSE denoting whether missing values are considered
        flag = str(
            workload_summary\
                .id_to_cols_to_bucketization[pattern_id][sel_col]\
                    .flag_for_queries_wo_sel_on_this_col
        ).upper() 
        select_chunks.append(
            f"{col_to_func[sel_col]}({pure_col}, %s, {flag}) AS bkt{i}_id"
        )
        outer_select_chunks.append(
            f"bkt{i}_id"
        )
        n_bkt_ids += 1
        params.append([
            [lb, ub] 
            for lb, ub in workload_summary\
                .id_to_cols_to_bucketization[pattern_id][sel_col]\
                    .chunks
        ])
    q += "\n\t, ".join(select_chunks)
    
    # -- FROM CLAUSE & WHERE CLAUSE 
    q += f"\n FROM {alias_to_tab[alias]} {alias}"
    q += "\n WHERE "
    need_post_check = False
    if given_vals is not None:
        if len(given_vals) >= 100:
            # when there are more than 100 given vals, we query against the 
            #     range [min(given_vals), max(given_vals)] and postfilter 
            #     the tuples with out-of-range join vals
            q += (
                f"{this_alias_join_cols[0]} >= {given_vals[0]}"
                f" AND {this_alias_join_cols[0]} <= {given_vals[-1]}"
            )
            need_post_check = True
        else:
            # otherwise, it will be a OR-joint chunk
            q += "(" \
                + " OR ".join([
                    this_alias_join_cols[0] + " = " + str(given_val) 
                    for given_val in given_vals
                ]) + ")"
    else:
        # for the root node, we just want the 1st join column to be not null
        q += this_alias_join_cols[0] + " IS NOT NULL"
    if len(this_alias_join_cols) > 1:
        # for the rest join columns (if any), they just need to be not null
        q += " AND " + " AND ".join([
            join_col + " IS NOT NULL" 
            for join_col in this_alias_join_cols[1:]
        ])

    # -- ORDER BY CLAUSE 
    pure_join_cols = []
    for join_col in this_alias_join_cols:
        pure_join_cols.append(join_col.split(".")[1])
    outer_q = f"SELECT " + ", ".join(outer_select_chunks) 
    if len(outer_select_chunks) > 0:
        outer_q += ", "
    outer_q += "COUNT(*) as cnt"
    outer_q += f" FROM ({q})"
    if n_bkt_ids > 0: 
        outer_q += " WHERE " + " AND ".join(f"bkt{i}_id IS NOT NULL" for i in range(n_bkt_ids))
    if len(outer_select_chunks) > 0:
        outer_q += " GROUP BY " + ", ".join(outer_select_chunks)
    outer_q += " ORDER BY " + ", ".join(pure_join_cols)
    outer_q += ";"

    enumeration_start_ts = time.time()
    scan_cursor.execute(outer_q, tuple(params))
    info["total_q"] += 1
    buffer = scan_cursor.fetchmany(batch_size)
    info["enumeration"] += time.time() - enumeration_start_ts

    join_col_to_id = {
        join_col : i for i, join_col in enumerate(this_alias_join_cols)
    }
    lst_signature = None
    # defaultdict(int)
    my_bucketization = {} 
    # {child : cnt} for every child in jp.alias_to_child[alias];
    #     this mapping is used to help adjusting the order of enumerating
    #     children for the current node (a.k.a `alias`). Intuitively, we
    #     believe that child that misses more joins so far (i.e., does not
    #     have tuples to join with the tuples of `alias` that have been
    #     enumerated already) also has the potential to miss more for the
    #     following join values to enumerate in `alias`.
    child_to_skip_cnt = defaultdict(int) 
    children = jp.alias_to_child[alias]
    child_to_id = {child : i for i, child in enumerate(children)}
    # a list of children in the order of enumeration
    child_enumeration_order = copy.deepcopy(children)
    n_child = len(child_enumeration_order)

    while len(buffer) > 0:
        for row in buffer:
            # if None in row:
            #     continue
            join_signature = row[:n_join_col]
            if need_post_check:
                if join_signature[0] not in given_vals_as_a_set:
                    continue
            if join_signature != lst_signature:
                if len(my_bucketization) >= batch_size:
                    # process a batch of join signatures at once
                    singletons = [None] * n_child
                    for child in child_enumeration_order:
                        _id = child_to_id[child]
                        join_col_idx = join_col_to_id[
                            jp.alias_to_child_join_col[alias][child]
                        ]
                        requested_vals_as_a_set = set(
                            signature[join_col_idx] 
                            for signature in my_bucketization 
                        )
                        sorted_requested_vals = sorted(
                            requested_vals_as_a_set
                        )
                        singletons[_id] = run(
                            alias=child,
                            conn=conn,
                            jp=jp,
                            pattern_id=pattern_id,
                            workload_summary=workload_summary,
                            alias_to_sel_col=alias_to_sel_col,
                            alias_to_tab=alias_to_tab,
                            col_to_func=col_to_func,
                            given_vals=sorted_requested_vals,
                            given_vals_as_a_set=requested_vals_as_a_set,
                            batch_size=batch_size,
                            info=info,
                        )
                        for signature in list(my_bucketization.keys()):
                            _key = signature[join_col_idx]
                            if _key not in singletons[_id] \
                                or len(singletons[_id][_key]) == 0:
                                del my_bucketization[signature]
                                child_to_skip_cnt[child] += 1

                    producing_start_ts = time.time()
                    child_to_join_col_idx = {
                        child: join_col_to_id[
                            jp.alias_to_child_join_col[alias][child]
                        ]
                        for child in jp.alias_to_child_join_col[alias]
                    }
                    for signature in my_bucketization:
                        this_child_to_val = {
                            child: signature[child_to_join_col_idx[child]]
                            for child in child_to_join_col_idx
                        }
                        if given_vals is None:
                            add_the_product_of_multiple_hists_to(
                                [my_bucketization[signature]] + 
                                [
                                    singletons[i][this_child_to_val[child]] 
                                    if this_child_to_val[child] in singletons[i] 
                                    else defaultdict(int) 
                                    for i, child in enumerate(children)
                                ], 
                                unified_hist
                            )
                        else:
                            add_the_product_of_multiple_hists_to(
                                [my_bucketization[signature]] + 
                                [
                                    singletons[i][this_child_to_val[child]] 
                                    if this_child_to_val[child] in singletons[i] 
                                    else defaultdict(int) 
                                    for i, child in enumerate(children)
                                ], 
                                unified_hist[signature[0]]
                            )
                    info["producing"] += time.time() - producing_start_ts
                    my_bucketization.clear()
                    child_enumeration_order.sort(
                        key=lambda x: child_to_skip_cnt[x], 
                        reverse=True
                    )
                my_bucketization[join_signature] = defaultdict(int)

            bucketization_start_ts = time.time()
            my_bucketization[join_signature][row[n_join_col:-1]] += row[-1]
            info["bucketization"] += time.time() - bucketization_start_ts
            lst_signature = join_signature    
        enumeration_start_ts = time.time()
        buffer = scan_cursor.fetchmany(batch_size)
        info["enumeration"] += time.time() - enumeration_start_ts

    scan_cursor.close()
    if len(my_bucketization) > 0:
        singletons = [None] * n_child
        for child in child_enumeration_order:
            _id = child_to_id[child]
            join_col_idx = join_col_to_id[
                jp.alias_to_child_join_col[alias][child]
            ]
            requested_vals_as_a_set = set(
                signature[join_col_idx] 
                for signature in my_bucketization 
            )
            sorted_requested_vals = sorted(requested_vals_as_a_set)
            singletons[_id] = run(
                alias=child,
                conn=conn,
                jp=jp,
                pattern_id=pattern_id,
                workload_summary=workload_summary,
                alias_to_sel_col=alias_to_sel_col,
                alias_to_tab=alias_to_tab,
                col_to_func=col_to_func,
                given_vals=sorted_requested_vals,
                given_vals_as_a_set=requested_vals_as_a_set,
                batch_size=batch_size,
                info=info,
            )
            for signature in list(my_bucketization.keys()):
                _key = signature[join_col_idx]
                if _key not in singletons[_id]\
                    or len(singletons[_id][_key]) == 0:
                    del my_bucketization[signature]
                    child_to_skip_cnt[child] += 1

        producing_start_ts = time.time()
        child_to_join_col_idx = {
            child: join_col_to_id[
                jp.alias_to_child_join_col[alias][child]
            ]
            for child in jp.alias_to_child_join_col[alias]
        }
        for signature in my_bucketization:
            this_child_to_val = {
                child: signature[child_to_join_col_idx[child]]
                for child in child_to_join_col_idx
            }
            if given_vals is None:
                add_the_product_of_multiple_hists_to(
                    [my_bucketization[signature]] + 
                    [
                        singletons[i][this_child_to_val[child]] 
                        if this_child_to_val[child] in singletons[i] 
                        else defaultdict(int) 
                        for i, child in enumerate(children)
                    ], 
                    unified_hist
                )
            else:
                add_the_product_of_multiple_hists_to(
                    [my_bucketization[signature]] + 
                    [
                        singletons[i][this_child_to_val[child]] 
                        if this_child_to_val[child] in singletons[i] 
                        else defaultdict(int) 
                        for i, child in enumerate(children)
                    ], 
                    unified_hist[signature[0]]
                )
        info["producing"] += time.time() - producing_start_ts
        my_bucketization.clear()
    
    return unified_hist

def process_a_jp(
    conn,
    alias_to_tab,
    jp,
    workload_summary,
    pattern_id,
    batch_size,
    answer,
    is_single_table_jp_and_only_on_aliases=None
):
    # -- preparation
    alias_to_sel_col = defaultdict(list)
    for sel_col in workload_summary\
        .id_to_cols_with_selection[pattern_id]:
        alias = sel_col.split(".")[0]
        if alias not in alias_to_sel_col:
            alias_to_sel_col[alias] = []
        alias_to_sel_col[alias].append(sel_col)
    
    col_to_func = {} # col -> func name
    col_to_bucket_data_type = {} # col -> datatype of buckets

    for alias in alias_to_sel_col:
        for sel_col in alias_to_sel_col[alias]:
            workload_summary.process_a_selected_col(
                pattern_id=pattern_id, 
                col=sel_col, 
                batch_size=50000
            )
            cur_type = workload_summary.col_to_type[sel_col]
            if cur_type in ["integer", "smallint"]:
                cur_type = "INTEGER"
            elif cur_type == "timestamp without time zone":
                cur_type = cur_type.upper()
            elif cur_type == "numeric":
                cur_type = "double precision"
            else:
                raise NotImplementedError
            col_to_bucket_data_type[sel_col] = cur_type
            if cur_type == "INTEGER":
                data_type_alias = "int"
            elif cur_type == "double precision":
                data_type_alias = "float"
            else:
                data_type_alias = "ts"
            col_to_func[sel_col] = \
                "find_bucket_" + data_type_alias
    
    # -- branch according to the number of tables involved
    if jp.n_tables == 1:
        # Handle each single-table using such a SQL
        # 
        # WITH <all selection column buckets definitions>
        # SELECT 
        #     col_to_func[sel_col0](sel_col0, %s, flag) AS bkt(0)_id, 
        #     ..., 
        #     col_to_func[sel_col(M-1)](sel_col(M-1), %s, flag) AS bkt(M - 1)_id,
        #     SUM(grouped_cnt)
        # FROM (
        #     SELECT 
        #           sel_col0,
        #           ...,
        #           sel_col(M-1),
        #           COUNT(*) AS grouped_cnt
        #     FROM <alias> 
        #     GROUP BY sel_col0, ..., sel_col(M - 1)
        # )
        # GROUP BY bkt(0)_id, ..., bkt(M - 1)_id;
        #
        # NOTE: we don't do a inner pre-aggregation for 
        #     multi-table queries, because aggregation over 
        #     one or more join columns are usually too slow.

        for alias in workload_summary.id_to_alias_to_qid[
            pattern_id
        ]:
            if is_single_table_jp_and_only_on_aliases is not None:
                if alias \
                    not in is_single_table_jp_and_only_on_aliases:
                    continue # we skip this alias
                
            this_alias_computation_start_ts = time.time()
            overhead_for_enumerating_the_rows = 0.
            n_sel_col = len(alias_to_sel_col[alias])

            if alias in alias_to_sel_col and n_sel_col > 0:
                # WITH (i.e., defining the buckets as constant array)
                single_table_scan_sql = "\n WITH "
                with_clauses = []
                for sel_col in alias_to_sel_col[alias]:
                    # remove the prefix "{alias}."
                    pure_col = sel_col.split(".")[1] 
                    bucket_datatype = col_to_bucket_data_type[sel_col]
                    with_clause = f"\n\t {pure_col}_bucket_def AS ( "
                    with_clause += "SELECT ARRAY["
                    with_clause += ", ".join([
                        "\n\t\t\tARRAY["
                        f"'{lb}'::{bucket_datatype}, '{ub}'::{bucket_datatype}]" 
                        for lb, ub in workload_summary\
                            .id_to_cols_to_bucketization[pattern_id][sel_col]\
                                .chunks
                    ])
                    with_clause += f"\n\t]::{bucket_datatype}[][] AS arr)"
                    with_clauses.append(with_clause)
                single_table_scan_sql += ", ".join(with_clauses)
            else:
                single_table_scan_sql = ""

            # SELECT
            single_table_scan_sql += "SELECT \n\t" 
            select_chunks = []
            for i, sel_col in enumerate(alias_to_sel_col[alias]):
                # remove the prefix "{alias}."
                pure_col = sel_col.split(".")[1] 
                # TRUE/FALSE denoting whether missing values are considered
                flag = str(
                    workload_summary\
                        .id_to_cols_to_bucketization[pattern_id][sel_col]\
                            .flag_for_queries_wo_sel_on_this_col
                ).upper()
                select_chunks.append(
                    f"{col_to_func[sel_col]}("
                    f"{pure_col}, {pure_col}_bucket_def.arr, {flag}"
                    f") AS bkt{i}_id"
                )
            select_chunks.append("SUM(grouped_cnt)::INTEGER AS cnt")
            single_table_scan_sql += "\n\t, ".join(select_chunks)

            # FROM
            single_table_scan_sql += "\nFROM " 
            select_chunks_in_subquery = []
            for sel_col in alias_to_sel_col[alias]:
                # remove the prefix "{alias}."
                pure_col = sel_col.split(".")[1] 
                select_chunks_in_subquery.append(pure_col)
            select_chunks_in_subquery.append("COUNT(*) AS grouped_cnt")

            # use a inner sub-query to aggregate the tuples over 
            #     selction columns first
            single_table_scan_sql += (
                f"(SELECT {', '.join(select_chunks_in_subquery)} "
                f"FROM {alias_to_tab[alias]} {alias}"
            ) 
            if n_sel_col > 0:
                single_table_scan_sql += \
                    f" GROUP BY {', '.join(alias_to_sel_col[alias])})"
            else:
                single_table_scan_sql += ")"
            for sel_col in alias_to_sel_col[alias]:
                # remove the prefix "{alias}."
                pure_col = sel_col.split(".")[1] 
                single_table_scan_sql += f", {pure_col}_bucket_def"

            if n_sel_col > 0:
                # GROUP BY
                single_table_scan_sql += f"\nGROUP BY " + ", ".join([
                    f"bkt{i}_id" 
                    for i in range(n_sel_col)
                ])
                single_table_scan_sql += ";"

            scan_cursor = conn.cursor(name="scan_cursor_of_single_table_jp") 
            # {(chunk_id0, chunk_id1, ...) : cnt}
            unified_hist = defaultdict(int) 

            enumerating_the_rows_start_ts = time.time()
            scan_cursor.execute(single_table_scan_sql)
            buffer = scan_cursor.fetchmany(batch_size)
            overhead_for_enumerating_the_rows += \
                time.time() - enumerating_the_rows_start_ts
            bucketization_start_ts = time.time()
            bucketization_overhead = 0.
            while len(buffer) > 0:
                for row in buffer:
                    if None in row:
                        continue
                    unified_hist[row[:-1]] = row[-1]
                enumerating_the_rows_start_ts = time.time()
                buffer = scan_cursor.fetchmany(batch_size)
                overhead_for_enumerating_the_rows += \
                    time.time() - enumerating_the_rows_start_ts
                bucketization_overhead -= \
                    time.time() - enumerating_the_rows_start_ts
            bucketization_overhead += \
                time.time() - bucketization_start_ts
            scan_cursor.close()
            
            print(
                f"\tthe overhead of enumerating all rows in {alias}"
                f" is {overhead_for_enumerating_the_rows} seconds."
            )
            print(
                f"\tbucketization overhead is {bucketization_overhead}. "
                f"There are {len(unified_hist)} buckets of "
                f"dim {len(list(unified_hist.keys())[0])}"
            )
            computing_result_start_ts = time.time()
            lower_bounds = []
            upper_bounds = []
            for qid in workload_summary\
                .id_to_alias_to_qid[pattern_id][alias]:
                lower_bounds.append([])
                upper_bounds.append([])
                for i, col in enumerate(alias_to_sel_col[alias]):
                    lower_bounds[-1].append(
                        workload_summary\
                            .id_to_cols_to_qid_and_chunks[
                                pattern_id
                            ][col][qid][0]
                    )
                    upper_bounds[-1].append(
                        workload_summary\
                            .id_to_cols_to_qid_and_chunks[
                                pattern_id
                            ][col][qid][1]
                    )
            lower_bounds = np.array(lower_bounds)
            upper_bounds = np.array(upper_bounds)
            
            query_weight_sums = np.zeros(
                len(workload_summary.id_to_alias_to_qid[pattern_id][alias]), 
                dtype=np.int64
            )

            points = np.array(
                list(unified_hist.keys()), dtype=np.int32
            )   # shape (N,D)
            weights = np.array(
                list(unified_hist.values()), dtype=np.int64
            )  # shape (N,)

            N = points.shape[0]
            # for start in range(0, N, batch_size):
            #     end = min(start + batch_size, N)
            #     query_weight_sums += batch_query(
            #         points[start : end], 
            #         weights[start : end], 
            #         lower_bounds, 
            #         upper_bounds
            #     ) 
            query_weight_sums += batch_query(
                points, 
                weights, 
                lower_bounds, 
                upper_bounds
            ) 

            for i, qid in enumerate(
                workload_summary.id_to_alias_to_qid[pattern_id][alias]
            ):
                answer[qid] = query_weight_sums[i]
            print(
                "\tcomputing the result by the unified histogram takes "
                f"{time.time() - computing_result_start_ts:.6f} seconds"
            )
            print(
                f"\t[{alias}] computation overhead: "
                f"{time.time() - this_alias_computation_start_ts:.6f}"
            )
    else: # not a single-table jp
        jp._build_the_tree(alias_to_tab=alias_to_tab)
        is_a_star, _ = jp.is_a_star()

        if is_a_star: 
            # Handle a star-join such a SQL
            # 
            # SELECT 
            #    col_to_func[sel_col0](sel_col0, %s, flag) AS bkt(0)_id, 
            #    ..., 
            #    col_to_func[sel_col(M-1)](sel_col(M-1), %s, flag) AS bkt(M - 1)_id
            # FROM <all aliases>
            # WHERE <join conditions>
            #
            # NOTE: we don't do a inner pre-aggregation here, since ther will 
            #     be many selection columns if we merge multiple tables together

            alias_enumerate_order = jp._tree_preorder()
            select_chunks = []
            params = []
            from_chunks = []

            for alias in alias_enumerate_order:
                this_alias_sel_cols = alias_to_sel_col[alias]
                for i, sel_col in enumerate(this_alias_sel_cols):
                    # flag: TRUE/FALSE denoting whether missing values are considered
                    flag = str(
                        workload_summary\
                            .id_to_cols_to_bucketization[pattern_id][sel_col]\
                                .flag_for_queries_wo_sel_on_this_col
                    ).upper() 
                    select_chunks.append(
                        f"{col_to_func[sel_col]}({sel_col}, %s, {flag}) AS bkt{i}_id"
                    ) 
                    params.append([
                        [lb, ub] 
                        for lb, ub in workload_summary\
                            .id_to_cols_to_bucketization[pattern_id][sel_col]\
                                .chunks
                    ]) 
                from_chunks.append(f"{alias_to_tab[alias]} {alias}")
            
            scan_sql = "SELECT \n\t"
            scan_sql += "\n\t, ".join(select_chunks)
            scan_sql += "\n FROM " + ", ".join(from_chunks)
            scan_sql += "\n WHERE " + jp.get_join_clause() + ";"
            
            scan_cursor = conn.cursor(name="scan_cursor_of_star") 
            # {(chunk_id0, chunk_id1, ...) : cnt}
            unified_hist = defaultdict(int) 

            enumerating_the_rows_start_ts = time.time()
            scan_cursor.execute(scan_sql, tuple(params))
            buffer = scan_cursor.fetchmany(batch_size)
            overhead_for_enumerating_the_rows = \
                time.time() - enumerating_the_rows_start_ts
            bucketization_start_ts = time.time()
            bucketization_overhead = 0.
            while len(buffer) > 0:
                for row in buffer:
                    if None in row:
                        continue
                    unified_hist[row] += 1
                enumerating_the_rows_start_ts = time.time()
                buffer = scan_cursor.fetchmany(batch_size)
                overhead_for_enumerating_the_rows += \
                    time.time() - enumerating_the_rows_start_ts
                bucketization_overhead -= \
                    time.time() - enumerating_the_rows_start_ts
            bucketization_overhead += \
                time.time() - bucketization_start_ts
            scan_cursor.close()
            
            print(
                f"\tthe overhead of enumerating all rows in {alias}"
                f" is {overhead_for_enumerating_the_rows} seconds."
            )
            print(
                f"\tbucketization overhead is {bucketization_overhead}. "
                f"There are {len(unified_hist)} buckets of "
                f"dim {len(list(unified_hist.keys())[0])}"
            )
            computing_result_start_ts = time.time()
            
            lower_bounds = []
            upper_bounds = []
            for qid in workload_summary.id_to_qid[pattern_id]:
                lower_bounds.append([])
                upper_bounds.append([])
                for alias in alias_enumerate_order:
                    if alias in alias_to_sel_col:
                        for i, col in enumerate(alias_to_sel_col[alias]):
                            lower_bounds[-1].append(
                                workload_summary.id_to_cols_to_qid_and_chunks[
                                    pattern_id
                                ][col][qid][0]
                            )
                            upper_bounds[-1].append(
                                workload_summary.id_to_cols_to_qid_and_chunks[
                                    pattern_id
                                ][col][qid][1]
                            )
            lower_bounds = np.array(lower_bounds)
            upper_bounds = np.array(upper_bounds)

            query_weight_sums = np.zeros(
                len(workload_summary.id_to_qid[pattern_id]), 
                dtype=np.int64
            )

            points = np.array(
                list(unified_hist.keys()), dtype=np.int32
            )   # shape (N,D)
            weights = np.array(
                list(unified_hist.values()), dtype=np.int64
            )  # shape (N,)

            N = points.shape[0]
            # for start in range(0, N, batch_size):
            #     end = min(start + batch_size, N)
            #     query_weight_sums += batch_query(
            #         points[start : end], 
            #         weights[start : end], 
            #         lower_bounds, 
            #         upper_bounds
            #     ) 
            query_weight_sums += batch_query(
                points, 
                weights, 
                lower_bounds, 
                upper_bounds
            ) 

            for i, qid in enumerate(workload_summary.id_to_qid[pattern_id]):
                answer[qid] = query_weight_sums[i]

            print(
                "\tcomputing the result by the unified histogram takes "
                f"{time.time() - computing_result_start_ts:.6f} seconds"
            )
            del unified_hist
        else: # multi-way join pattern but not a star
            info = {
                "enumeration" : 0., 
                "bucketization" : 0., 
                "producing" : 0., 
                "total_q" : 0, 
                "cache_hit" : 0
            }

            unified_hist = run(
                alias=jp.root_alias,
                conn=conn,
                jp=jp,
                pattern_id=pattern_id,
                workload_summary=workload_summary,
                alias_to_sel_col=alias_to_sel_col,
                alias_to_tab=alias_to_tab,
                col_to_func=col_to_func,
                # the parent node forces the first column in 
                #     jp.alias_to_join_col[alias] has value "given_vals"
                given_vals=None, 
                given_vals_as_a_set=None,
                batch_size=batch_size,
                info=info,
            )
            print(
                "\tthe overhead of enumerating all rows in is "
                f"{info['enumeration']:.6f} seconds."
            )
            print(
                f"\tbucketization overhead is {info['bucketization']:.6f}"
            )
            print(
                f"\tget the product of buckets take {info['producing']:.6f} "
                "seconds"
            )
            print(f"\trun {info['total_q']} single-table sorting queries")
            print(f"\tcache has {info['cache_hit']} hits")
            print("\tnow start to compute the counts ......")
            print(
                f"\tThere are {len(unified_hist)} buckets of "
                f"dim {len(list(unified_hist.keys())[0])}"
            )

            alias_enumerate_order = jp._tree_preorder()

            computing_result_start_ts = time.time()
            lower_bounds = []
            upper_bounds = []
            for qid in workload_summary.id_to_qid[pattern_id]:
                lower_bounds.append([])
                upper_bounds.append([])
                for alias in alias_enumerate_order:
                    if alias in alias_to_sel_col:
                        for i, col in enumerate(alias_to_sel_col[alias]):
                            lower_bounds[-1].append(
                                workload_summary.id_to_cols_to_qid_and_chunks[
                                    pattern_id
                                ][col][qid][0]
                            )
                            upper_bounds[-1].append(
                                workload_summary.id_to_cols_to_qid_and_chunks[
                                    pattern_id
                                ][col][qid][1]
                            )
            lower_bounds = np.array(lower_bounds)
            upper_bounds = np.array(upper_bounds)

            query_weight_sums = np.zeros(
                len(workload_summary.id_to_qid[pattern_id]), 
                dtype=np.int64
            )

            points = np.array(
                list(unified_hist.keys()), dtype=np.int32
            )   # shape (N,D)
            weights = np.array(
                list(unified_hist.values()), dtype=np.int64
            )  # shape (N,)

            N = points.shape[0]
            # for start in range(0, N, batch_size):
            #     end = min(start + batch_size, N)
            #     query_weight_sums += batch_query(
            #         points[start : end], 
            #         weights[start : end], 
            #         lower_bounds, 
            #         upper_bounds
            #     ) 
            query_weight_sums += batch_query(
                points, 
                weights, 
                lower_bounds, 
                upper_bounds
            ) 

            for i, qid in enumerate(workload_summary.id_to_qid[pattern_id]):
                answer[qid] = query_weight_sums[i]
            print(
                "\tcomputing the result by the unified histogram takes "
                f"{time.time() - computing_result_start_ts:.6f} seconds"
            )
            del unified_hist

if __name__ == "__main__":
    json_filename = sys.argv[1]
    with open(json_filename) as json_fin:
        config = json.load(json_fin)
        path_to_sql = config["path_to_sql"]
        dir_to_output = config["dir_to_output"]
        group_by = config["group_by"]
        assert group_by in ["join pattern", "query pattern"]
        batch_size = config["batch_size"]

        if "debug" in config:
            debug = config["debug"]
        else:
            debug = False
        if debug:
            path_to_output = dir_to_output + "tree_batch_cext_DEBUG.debug"
        else:
            path_to_output = dir_to_output + "tree_batch_cext_" \
             + path_to_sql.split("/")[-1].split(".sql")[0] 
            if group_by == "query pattern":
                path_to_output += "_byQP"
            path_to_output += ".ans"

        info_at_the_beginning = \
            f"#timestamp:{datetime.now()}\n#header:count,secs\n" 

        with open(path_to_sql, "r") as fin, \
            open(path_to_output, "w") as fout:
            fout.write(info_at_the_beginning)
            line_id = -1
            dbname = None
            parsing_overhead = 0.
            loading_overhead = 0.
            qid = 0
            workload_summary = WorkloadSummary(group_by=group_by)
            sqls = []
            tab_to_alias = {}
            alias_to_tab = {}

            for line in tqdm(fin.readlines()):
                line_id += 1
                if line.startswith("--db:"):
                    dbname = line.strip().split("--db:")[1]
                elif line.startswith("--from:"):
                    pass
                elif line.startswith("--description:"):
                    pass
                elif line.upper().startswith("SELECT COUNT(*)"):
                    if dbname is None:
                        raise ValueError(f"Did not specify a DB")
                    elif dbname in ["imdb", "stats", "dsb-sf2"]:
                        sqls.append(line.strip().lower())
                        (
                            this_parsing_overhead, 
                            join_conds, 
                            column_to_selections, 
                            cur_tab_to_alias, 
                            cur_alias_to_tab
                        ) = parse_one_imdb_or_stats_or_dsb(sql=sqls[-1])
                        for k, v in cur_tab_to_alias.items():
                            if k not in tab_to_alias:
                                tab_to_alias[k] = v
                        for k, v in cur_alias_to_tab.items():
                            if k not in alias_to_tab:
                                alias_to_tab[k] = v
                    else:
                        raise NotImplementedError
                    parsing_overhead += this_parsing_overhead
                    loading_start_ts = time.time()
                    workload_summary.insert(
                        dbname=dbname,
                        qid=qid,
                        join_conds=join_conds,
                        column_to_selections=column_to_selections,
                        this_query_tab_to_alias=cur_tab_to_alias,
                    )
                    qid += 1
                    loading_overhead += time.time() - loading_start_ts
                else:
                    raise ValueError(
                        f"Cannot parse line {line_id}: line"
                    )
            print("total parsing overhead", parsing_overhead)
            print("total loading overhead", loading_overhead)

            computation_overhead = 0.
            conn, cursor = get_connection(dbname=dbname)
            cursor.execute("SET max_parallel_workers_per_gather = 0;")
            workload_summary.get_column_metadata(
                cursor=cursor, 
                alias_to_tab=alias_to_tab
            )
            turn_on_extensions(cursor)

            answer = {} # qid -> cnt
            for pattern, pattern_id in workload_summary.iterate_by_group():
                if workload_summary.group_by == "join pattern":
                    jp = pattern
                else:
                    jp = pattern.join_pattern
                print(
                    f"===== Pattern {pattern_id}/{workload_summary.n_groups}, "
                    f"proccessing {len(workload_summary.id_to_qid[pattern_id])}"
                    f" SQLs that belong to {workload_summary.group_by}:", 
                    pattern
                )

                this_step_computation_start_ts = time.time()
                for qid in workload_summary.id_to_qid[pattern_id]:
                    if qid in answer:
                        raise ValueError("Something wrong with the JoinPattern")
                    else:
                        answer[qid] = 0

                process_a_jp(
                    conn=conn,
                    alias_to_tab=alias_to_tab,
                    jp=jp,
                    workload_summary=workload_summary,
                    pattern_id=pattern_id,
                    batch_size=batch_size,
                    answer=answer
                )

                print(
                    "\tcomputation overhead: "
                    f"{time.time() - this_step_computation_start_ts:.6f}"
                )
                computation_overhead += \
                    time.time() - this_step_computation_start_ts

            turn_off_extensions(cursor)

            print(f"total computation overhead: {computation_overhead:.6f}")
            close_connection(conn, cursor)
            for qid in range(len(sqls)):
                fout.write(f"{answer[qid]},None\n")