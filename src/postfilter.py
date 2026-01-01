# Process all the counting queries by postfiltering
# For each JoinPattern, compute the full join results, stream by the content, 
# check each query that belongs to this JoinPattern

import time
import json
import sys
from tqdm import tqdm
from datetime import datetime
import psycopg2

from utility import parse_one_imdb_or_stats_or_dsb
from db_connection import (
    run_conditional_aggregation, 
    get_latency,
    get_connection
)
from workload_summary import WorkloadSummary

# process a JoinPattern by Postfiltering
def process_a_jp(
    jp,
    pattern_id,
    workload_summary,
    alias_to_tab,
    dbname,
    timeout_in_sec,
    answer,
    # see why we need this in hybrid.py; 
    #     we do not use it by Postfilter itself, 
    #     and it set as None here
    is_single_table_jp_and_only_on_aliases=None
):
    pg_limits_on_n_selections = 1664

    alias_enumerate_order = list(
        jp.alias_to_join_col.keys()
    )
    n_none = 0
    total_latency = 0.
    additional_overhead_at_least = 0.
                
    if jp.n_tables == 1:
        # Handle each single-table
        for alias in workload_summary.id_to_alias_to_qid[
            pattern_id
        ]:
            if is_single_table_jp_and_only_on_aliases is not None:
                if alias \
                    not in is_single_table_jp_and_only_on_aliases:
                    continue # we skip this alias
            selection_chunk = []
            for qid in workload_summary\
                .id_to_alias_to_qid[pattern_id][alias]:
                if workload_summary.qid_to_selection[qid] == "":
                    selection_chunk.append("COUNT(*)")
                else:
                    selection_chunk.append(
                        f"COUNT(CASE WHEN "
                        f"{workload_summary.qid_to_selection[qid]}"
                        f" THEN 1 END)"
                    )
            
            this_alias_latency = 0.
            this_alias_n_none = 0
            this_alias_additional_overhead_at_least = 0.
            for k in range(0, len(selection_chunk), pg_limits_on_n_selections):
                st = k
                en = min(len(selection_chunk), k + pg_limits_on_n_selections)

                sql = "SELECT " + ", ".join(
                    selection_chunk[st : en]
                ) + " FROM " + alias_to_tab[alias] + " " + alias + ";"
                flag_for_timeout = False
                latency = None
                try:
                    # get the count
                    counts = run_conditional_aggregation(
                        dbname=dbname, 
                        sql=sql, 
                        timeout_in_sec=timeout_in_sec
                    )
                    # get the latency
                    latency, _ = get_latency(
                        dbname=dbname, 
                        sql=sql, 
                        n_repetitions=1, 
                        extract_plan_info=True,
                        timeout_in_sec=timeout_in_sec,
                    )
                except psycopg2.Error as e:
                    flag_for_timeout = True
                if flag_for_timeout:
                    for i in range(st, en):
                        qid = workload_summary\
                            .id_to_alias_to_qid[pattern_id][alias][i]
                        answer[qid] = None
                    this_alias_n_none += en - st
                    this_alias_additional_overhead_at_least += timeout_in_sec
                else:
                    for i in range(st, en):
                        qid = workload_summary\
                            .id_to_alias_to_qid[pattern_id][alias][i]
                        answer[qid] = counts[i - st]
                    this_alias_latency += latency
            print(
                f"\t[{alias}] computation overhead: "
                f"{this_alias_latency:.6f} "
                f"with {this_alias_n_none} x Nones "
                f"(which are at least {this_alias_additional_overhead_at_least:.6f})"
            )
            total_latency += this_alias_latency
            n_none += this_alias_n_none
            additional_overhead_at_least += this_alias_additional_overhead_at_least
    else:
        join_chunk = " AND ".join([
            f"{tmp[0]} {tmp[1]} {tmp[2]}" for tmp in list(jp.join_pattern)
        ])
        selection_chunk = []
        for qid in workload_summary.id_to_qid[pattern_id]:
            if workload_summary.qid_to_selection[qid] == "":
                selection_chunk.append("COUNT(*)")
            else:
                selection_chunk.append(
                    f"COUNT(CASE WHEN "
                    f"{workload_summary.qid_to_selection[qid]}"
                     " THEN 1 END)"
                )

        for k in range(0, len(selection_chunk), pg_limits_on_n_selections):
            st = k
            en = min(len(selection_chunk), k + pg_limits_on_n_selections)

            this_selection_chunk = ", ".join(selection_chunk[st : en])
            where_chunk = ", ".join([
                f"{alias_to_tab[alias]} {alias}" 
                for alias in alias_enumerate_order
            ])
            sql = "SELECT " + this_selection_chunk \
                + " FROM " + where_chunk \
                    + " WHERE " + join_chunk + ";"
            flag_for_timeout = False
            latency = None
            try:
                # get the count
                counts = run_conditional_aggregation(
                    dbname=dbname, 
                    sql=sql, 
                    timeout_in_sec=timeout_in_sec
                )
                # get the latency
                latency, _ = get_latency(
                    dbname=dbname, 
                    sql=sql, 
                    n_repetitions=1, 
                    extract_plan_info=True,
                    timeout_in_sec=timeout_in_sec,
                )
            except psycopg2.Error as e:
                flag_for_timeout = True
            if flag_for_timeout:
                for i in range(st, en):
                    qid = workload_summary.id_to_qid[pattern_id][i]
                    answer[qid] = None
                n_none += len(workload_summary.id_to_qid[pattern_id])
                additional_overhead_at_least += timeout_in_sec
            else:
                for i in range(st, en):
                    qid = workload_summary.id_to_qid[pattern_id][i]
                    answer[qid] = counts[i - st]
                total_latency += latency
    return (
        total_latency,
        n_none,
        additional_overhead_at_least
    )

if __name__ == "__main__":
    json_filename = sys.argv[1]
    with open(json_filename) as json_fin:
        config = json.load(json_fin)
        path_to_sql = config["path_to_sql"]
        dir_to_output = config["dir_to_output"]
        group_by = config["group_by"]
        assert group_by in ["join pattern", "query pattern"]
        if "debug" in config:
            debug = config["debug"]
        else:
            debug = True
        if "timeout_in_sec" in config:
            timeout_in_sec = config["timeout_in_sec"]
        else:
            timeout_in_sec = None

        if debug:
            path_to_output = dir_to_output + "postfilter_DEBUG.debug"
        else:
            path_to_output = dir_to_output + "postfilter_" \
                + path_to_sql.split("/")[-1].split(".sql")[0]
            if group_by == "query pattern":
                path_to_output += "_byQP"
            path_to_output += ".ans"

        info_at_the_beginning = \
            f"#timestamp:{datetime.now()}\n#header:count\n"

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

            conn, cursor = get_connection(dbname=dbname)
            cursor.execute("SET max_parallel_workers_per_gather = 0;")

            computation_overhead = 0.
            n_none = 0
            additional_overhead_at_least = 0.
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
                
                for qid in workload_summary.id_to_qid[pattern_id]:
                    if qid in answer:
                        raise ValueError(
                            "Something wrong with the JoinPattern"
                        )
                    else:
                        answer[qid] = 0

                (
                    total_latency,
                    this_n_none,
                    this_additional_overhead_at_least
                ) = process_a_jp(
                    jp=jp,
                    pattern_id=pattern_id,
                    workload_summary=workload_summary,
                    alias_to_tab=alias_to_tab,
                    dbname=dbname,
                    timeout_in_sec=timeout_in_sec,
                    answer=answer
                )
                print(
                    "\tcomputation overhead: "
                    f"{total_latency:.6f} "
                    f"with {this_n_none} x Nones "
                     "(which are at least "
                    f"{this_additional_overhead_at_least:.6f})"
                )
                computation_overhead += total_latency
                n_none += this_n_none
                additional_overhead_at_least += \
                    this_additional_overhead_at_least

            print(
                "total computation overhead: "
                f"{computation_overhead:.6f} "
                f"with {n_none} x Nones "
                 "(which are at least "
                f"{additional_overhead_at_least:.6f})"
            )
            for qid in range(len(sqls)):
                fout.write(f"{answer[qid]}\n")

