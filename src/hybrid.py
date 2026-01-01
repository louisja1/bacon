# Process the counting queries per join pattern;
# Use Optimizer to decide the algorithm for each;
# Optimizer relies on a pre-trained ML model to decide baselines
#     or tree_batch_cext;
# Among the baselines, use PG's cost estimate to decide postfilter
#     or sequential_processing;
# NOTE: ML's choice is a perssimistic decision (i.e., preferring 
#     tree_batch_cext unless baselines MUST be okay, e.g., 
#     two-table PK-FK join).
#     Given that it is a perssimistic decision, using either 
#     baseline won't be too bad. So relying on PG's cost estimate 
#     (even when the CEs are inaccurate) should be enough to decide 
#     between selection-first (i.e., using sequential_processing) 
#     or join-first (i.e., using postfilter).


import time
import json
import sys
from tqdm import tqdm
from datetime import datetime
from collections import defaultdict

import sequential_processing
import postfilter
import tree_batch_cext
from utility import parse_one_imdb_or_stats_or_dsb
from db_connection import (
    get_connection, 
    close_connection,
    turn_on_extensions,
    turn_off_extensions
)
from workload_summary import WorkloadSummary
from optimizer import Optimizer, Algorithm, Selector

if __name__ == "__main__":
    json_filename = sys.argv[1]
    with open(json_filename) as json_fin:
        config = json.load(json_fin)
        path_to_sql = config["path_to_sql"]
        dir_to_output = config["dir_to_output"]
        group_by = config["group_by"]
        assert group_by == "join pattern"
        path_to_model = config["path_to_model"]

        assert "params" in config
        for alg, must_have in [
            ("seqproc", ["timeout_in_sec"]), 
            ("postfilter", ["timeout_in_sec"]), 
            ("tree_batch_cext", ["batch_size"])
        ]:
            assert alg in config["params"]
            for ele in must_have:
                assert ele in config["params"][alg], \
                    f"must specify <{ele}> for {alg}"
        
        
        if "debug" in config:
            debug = config["debug"]
        else:
            debug = True

        if debug:
            path_to_output = dir_to_output \
                + "hybrid_DEBUG.debug"
        else:
            path_to_output = dir_to_output \
                + "hybrid_" \
                + path_to_sql.split("/")[-1].split(".sql")[0]
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
            workload_summary.get_column_metadata(
                cursor=cursor, 
                alias_to_tab=alias_to_tab
            )

            computation_overhead = 0.
            optimization_overhead = 0.
            answer = {} # qid -> cnt
            optimizer = Optimizer(
                cursor=cursor,
                alias_to_tab=alias_to_tab
            )
            # the number of timeout sql
            n_none = 0
            # at least how much time needed for the timeouts
            additional_overhead_at_least = 0. 
            # whether extensions are set up (to be used by tree_batch_cext)
            extensions_are_on = False

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
                this_step_n_none = 0
                this_step_additional_overhead_at_least = 0.

                for qid in workload_summary.id_to_qid[pattern_id]:
                    if qid in answer:
                        raise ValueError(
                            "Something wrong with the JoinPattern"
                        )
                    else:
                        answer[qid] = 0
                
                alias_to_sel_col = defaultdict(list)
                for sel_col in workload_summary\
                    .id_to_cols_with_selection[pattern_id]:
                    alias = sel_col.split(".")[0]
                    if alias not in alias_to_sel_col:
                        alias_to_sel_col[alias] = []
                    alias_to_sel_col[alias].append(sel_col)
                
                this_step_optimization_start_ts = time.time()
                choices = optimizer.select_algorithm(
                    source_jp=jp,
                    pattern_id=pattern_id,
                    alias_to_sel_col=alias_to_sel_col,
                    workload_summary=workload_summary,
                    model_name=path_to_model,
                    sqls=sqls,
                    alias_to_tab=alias_to_tab,
                )
                this_step_optimization_overhead = time.time() \
                    - this_step_optimization_start_ts
                
                for alg in choices:
                    print(f"  Running {alg}...")
                    if jp.n_tables != 1:
                        assert choices[alg] is None \
                            and len(choices) == 1 
                        is_single_table_jp_and_only_on_aliases = None
                    else:
                        # As Postfilter and TreeBatchCext handle single-table
                        #     JP on each alias separately, we use this arg to
                        #     mask out the non-chosen aliases for them.
                        is_single_table_jp_and_only_on_aliases = set(choices[alg])

                    if alg == Algorithm.SeqProc:
                        # Because, we might run the same counting sql for 
                        #     multiple times to get different info or an 
                        #     average measurement, so we "stop the clock" 
                        #     and use the total query latency to replace 
                        #     this slack time.
                        pause_ts = time.time()
                        total_latency = 0.
                        timeout_in_sec \
                            = config["params"]["seqproc"]["timeout_in_sec"]
                        
                        if jp.n_tables == 1:
                            qids_to_process = []
                            for alias in workload_summary\
                                .id_to_alias_to_qid[pattern_id]:
                                if is_single_table_jp_and_only_on_aliases is not None \
                                    and alias not in is_single_table_jp_and_only_on_aliases:
                                    continue
                                qids_to_process.extend(
                                    workload_summary\
                                        .id_to_qid_single_table_pattern_on_alias[
                                            pattern_id
                                        ][alias]
                                )
                        else:   
                            qids_to_process = workload_summary.id_to_qid[pattern_id]
                        for qid in qids_to_process:
                            (
                                is_timeout, 
                                count, 
                                latency, 
                                _
                            ) = sequential_processing.process_a_sql(
                                sqls[qid],
                                dbname=dbname,
                                timeout_in_sec=timeout_in_sec
                            )
                            if is_timeout:
                                answer[qid] = None
                                this_step_n_none += 1
                                this_step_additional_overhead_at_least \
                                    += timeout_in_sec
                            else:
                                answer[qid] = count
                                total_latency += latency
                        # replace it with total latency
                        this_step_computation_overhead = \
                            pause_ts - this_step_computation_start_ts \
                            + total_latency
                    elif alg == Algorithm.PostFilter:
                        # Same here: replace the slack time with query 
                        #     latency
                        pause_ts = time.time()
                        timeout_in_sec \
                            = config["params"]["postfilter"][
                                "timeout_in_sec"
                            ]
                        (
                            _total_latency,
                            _n_none,
                            _additional_overhead_at_least
                        ) = postfilter.process_a_jp(
                            jp=jp,
                            pattern_id=pattern_id,
                            workload_summary=workload_summary,
                            alias_to_tab=alias_to_tab,
                            dbname=dbname,
                            timeout_in_sec=timeout_in_sec,
                            answer=answer, # in-place fill-up
                            is_single_table_jp_and_only_on_aliases=\
                                is_single_table_jp_and_only_on_aliases
                        )
                        this_step_n_none += _n_none
                        this_step_computation_overhead = pause_ts \
                            - this_step_computation_start_ts \
                            + _total_latency
                        this_step_additional_overhead_at_least \
                            += _additional_overhead_at_least
                    elif alg == Algorithm.TreeBatchCext:
                        if not extensions_are_on:
                            turn_on_extensions(cursor)
                            extensions_are_on = True
                        batch_size \
                            = config["params"]["tree_batch_cext"][
                                "batch_size"
                            ] 
                        tree_batch_cext.process_a_jp(
                            conn=conn,
                            alias_to_tab=alias_to_tab,
                            jp=jp,
                            workload_summary=workload_summary,
                            pattern_id=pattern_id,
                            batch_size=batch_size,
                            answer=answer,
                            is_single_table_jp_and_only_on_aliases=\
                                is_single_table_jp_and_only_on_aliases
                        )
                        this_step_computation_overhead = time.time() \
                            - this_step_computation_start_ts
                    else:
                        assert True == False

                print(
                    "\toptimization overhead: "
                    f"{this_step_optimization_overhead:.6f}"
                )
                print(
                    "\tcomputation overhead: "
                    f"{this_step_computation_overhead:.6f} "
                    f"with {this_step_n_none} x Nones "
                    "(which are at least "
                    f"{this_step_additional_overhead_at_least:.6f})"
                )
                optimization_overhead \
                    += this_step_optimization_overhead
                computation_overhead \
                    += this_step_computation_overhead
                n_none += this_step_n_none
                additional_overhead_at_least \
                    += this_step_additional_overhead_at_least

            turn_off_extensions(cursor)
            print(
                "total optimization overhead: "
                f"{optimization_overhead:.6f}"
            )
            print(
                "total computation overhead: "
                f"{computation_overhead:.6f} "
                f"with {n_none} x Nones "
                 "(which are at least "
                f"{additional_overhead_at_least:.6f})"
            )
            close_connection(conn, cursor)
            for qid in range(len(sqls)):
                fout.write(f"{answer[qid]}\n")

