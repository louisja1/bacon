# Process all the counting queries by tree
# Move more loads to PG side

from tqdm import tqdm
import csv
from utility import parse_one_imdb_or_stats_or_dsb

from workload_summary import WorkloadSummary

results_dir = "../results/"
workload_dir = "../workload/"

algorithms = [
    "seq",
    "postfilter",
    "tree_batch_cext",
    "hybrid",
    "seq_parallel",
    "postfilter_parallel",
    "tree_batch_cext_parallel",
    # "materialized" only has results for synthetic and dsb_grasp_20k
    "postfilter_materialized",
]

workloads = [
    "synthetic",
    # "scale",
    # "job_light_single",
    # "job_light_join",
    # "stats_ceb_single",
    # "stats_ceb_join",
    # "stats_ceb",
    # "job_light",
    "dsb_grasp_20k",
    ## following: no results on hybrid
    # "job_light_1k", 
    # "job_light_2k", 
    # "job_light_4k", 
]

header = [
    "pattern_id",
    "join_pattern",
    "n_tables",
    "n_queries"
]
for algorithm in algorithms:
    if algorithm in ["seq", "postfilter", "seq_parallel", "postfilter_parallel", "postfilter_materialized"]:
        header += [
            "sec_" + algorithm,
            "none_" + algorithm,
            "additional_at_least_" + algorithm,
        ]
    elif algorithm == "hybrid":
        header += [
            "sec_" + algorithm,
            "switch_to_" + algorithm,
        ]
    else:
        header += ["sec_" + algorithm]

def none_or_float(x):
    if x == "None":
        return x
    return float(x)

def sum_with_none(l):
    _sum = 0.
    _none_cnt = 0
    for ele in l:
        if ele == "None":
            _none_cnt += 1
        else:
            _sum += ele
    return _sum, _none_cnt

def parse_result(result_file, algorithm):
    if "parallel" in algorithm:
        assert (
            result_file.endswith("_parallel.ans") or result_file.endswith("_parallel.print")
        ) and result_file.startswith(algorithm.replace("_parallel", ""))
    elif "materialized" in algorithm:
        assert result_file.endswith("_materialized.print") and result_file.startswith(algorithm.replace("_materialized", ""))
    else:
        assert result_file.startswith(algorithm)
    overheads = []
    nones = []
    additional = []
    alias_to_subresults = {}
    switch_to = []

    with open(results_dir + result_file, "r") as fin:
        if algorithm in ["seq", "seq_parallel"]:
            # return the time overhead for each qid sequentially
            for line in list(fin.readlines())[2:]:
                overheads.append(none_or_float(line.strip().split(",")[1]))
        elif algorithm in ["postfilter", "postfilter_parallel", "postfilter_materialized"]:
            # return the time overhead for each pattern_id sequentially
            flag_for_single_tab_jp = False
            for line in fin.readlines():
                if line.startswith("===== Pattern"):
                    if line.strip().endswith("join pattern: ()"):
                        flag_for_single_tab_jp = True
                    else:
                        flag_for_single_tab_jp = False
                elif flag_for_single_tab_jp and line.startswith("\t["):
                    _alias = line.split("[")[1].split("]")[0]
                    alias_to_subresults[_alias] = (
                        float(line.strip().split("computation overhead: ")[1].split(" ")[0]),
                        int(line.strip().split(" x Nones")[0].split(" ")[-1]),
                        float(line.strip().split(" ")[-1].rstrip(")"))
                    )
                elif line.startswith("\tcomputation overhead: "):
                    overheads.append(float(line.strip().split(": ")[1].split(" ")[0]))
                    nones.append(int(line.strip().split(" x Nones")[0].split(" ")[-1]))
                    additional.append(float(line.strip().split(" ")[-1].rstrip(")")))
                    if nones[-1] > 0:
                        overheads[-1] = "None"
        elif algorithm in ["tree_batch_cext", "tree_batch_cext_parallel"]:
            # return the time overhead for each pattern_id sequentially
            flag_for_single_tab_jp = False
            for line in fin.readlines():
                if line.startswith("===== Pattern"):
                    if line.strip().endswith("join pattern: ()"):
                        flag_for_single_tab_jp = True
                    else:
                        flag_for_single_tab_jp = False
                elif flag_for_single_tab_jp and line.startswith("\t["):
                    _alias = line.split("[")[1].split("]")[0]
                    alias_to_subresults[_alias] = (
                        float(line.strip().split("computation overhead: ")[1].split(" ")[0]),
                    )
                elif line.startswith("\tcomputation overhead: "):
                    overheads.append(float(line.strip().split(": ")[1].split(" ")[0]))
        elif algorithm == "hybrid":
            # return the time overhead for each pattern_id sequentially
            flag_for_single_tab_jp = False
            cur_switch_to = []
            for line in fin.readlines():
                if line.startswith("===== Pattern"):
                    if len(cur_switch_to) > 0:
                        switch_to.append(cur_switch_to)
                    cur_switch_to = []
                    if line.strip().endswith("join pattern: ()"):
                        flag_for_single_tab_jp = True
                    else:
                        flag_for_single_tab_jp = False
                elif flag_for_single_tab_jp and line.startswith("\t["):
                    _alias = line.split("[")[1].split("]")[0]
                    alias_to_subresults[_alias] = (
                        float(line.strip().split("computation overhead: ")[1].split(" ")[0]),
                    )
                elif line.strip().startswith("Running"):
                    cur_switch_to.append(line.strip().split("Running ")[1].rstrip("..."))
                elif line.startswith("\tcomputation overhead: "):
                    overheads.append(float(line.strip().split(": ")[1].split(" ")[0]))
            if len(cur_switch_to) > 0:
                switch_to.append(cur_switch_to)
        else:
            assert True == False
    return overheads, nones, additional, alias_to_subresults, switch_to

if __name__ == "__main__":
    for workload in workloads:
        workload_file = workload_dir + workload + ".sql"
        output_file = results_dir + workload + ".tsv"

        with open(workload_file, "r") as fin, open(output_file, "w", newline='') as fout:
            tsv_writer = csv.DictWriter(fout, fieldnames=header, delimiter='\t')
            tsv_writer.writeheader()

            algorithm_to_overheads = {}
            algorithm_to_nones = {}
            algorithm_to_additional = {}
            algorithm_to_alias_subresults = {}
            algorithm_to_switch_to = {}

            for algorithm in algorithms:
                result_file = algorithm + "_" + workload
                if algorithm == "seq":
                    result_file += ".ans"
                elif algorithm in ["postfilter", "tree_batch_cext", "hybrid"]:
                    result_file += ".print"
                elif algorithm == "seq_parallel":
                    result_file = result_file.replace("_parallel", "") + "_parallel.ans"
                elif algorithm == "tree_batch_cext_parallel":
                    result_file = result_file.replace("_parallel", "") + "_parallel.print"
                elif algorithm == "postfilter_parallel":
                    result_file = result_file.replace("_parallel", "") + "_parallel.print"
                elif algorithm == "postfilter_materialized":
                    result_file = result_file.replace("_materialized", "") + "_materialized.print"
                else:
                    assert True == False 
                (
                    algorithm_to_overheads[algorithm],
                    algorithm_to_nones[algorithm],
                    algorithm_to_additional[algorithm],
                    algorithm_to_alias_subresults[algorithm],
                    algorithm_to_switch_to[algorithm]
                ) = parse_result(
                    result_file, 
                    algorithm
                )
                
            line_id = -1
            dbname = None
            qid = 0
            workload_summary = WorkloadSummary(group_by="join pattern")
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
                            _, 
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
                    workload_summary.insert(
                        dbname=dbname,
                        qid=qid,
                        join_conds=join_conds,
                        column_to_selections=column_to_selections,
                        this_query_tab_to_alias=cur_tab_to_alias,
                    )
                    qid += 1
                else:
                    raise ValueError(f"Cannot parse line {line_id}: line")

            tsv_out = []
            for pattern, pattern_id in workload_summary.iterate_by_group():
                if workload_summary.group_by == "join pattern":
                    jp = pattern
                else:
                    jp = pattern.join_pattern

                res = {
                    "pattern_id" : pattern_id,
                    "join_pattern" : str(jp),
                    "n_tables" : jp.n_tables,
                    "n_queries" : len(workload_summary.id_to_qid[pattern_id]),
                }
                for algorithm in algorithms:
                    if algorithm in ["seq", "seq_parallel"]:
                        _sum, _none_cnt = sum_with_none([
                            algorithm_to_overheads[algorithm][qid]
                            for qid in workload_summary.id_to_qid[pattern_id]
                        ])
                        res["sec_" + algorithm] = _sum
                        res["none_" + algorithm] = _none_cnt
                        res["additional_at_least_" + algorithm] = _none_cnt * 900.
                        if _none_cnt > 0:
                            res["sec_" + algorithm] = "None"
                    elif algorithm in ["postfilter", "postfilter_parallel", "postfilter_materialized"]:
                        res["sec_" + algorithm] = \
                            algorithm_to_overheads[algorithm][pattern_id]
                        res["none_" + algorithm] = \
                            algorithm_to_nones[algorithm][pattern_id]
                        res["additional_at_least_" + algorithm] = \
                            algorithm_to_additional[algorithm][pattern_id]
                    elif algorithm in ["tree_batch_cext", "tree_batch_cext_parallel"]:
                        res["sec_" + algorithm] = \
                            algorithm_to_overheads[algorithm][pattern_id]
                    elif algorithm == "hybrid":
                        res["sec_" + algorithm] = \
                            algorithm_to_overheads[algorithm][pattern_id]
                        res["switch_to_" + algorithm] = \
                            ", ".join(algorithm_to_switch_to[algorithm][pattern_id])
                    else:
                        assert True == False
                tsv_out.append(res)
                if jp.n_tables == 1:
                    # postfilter
                    assert len(algorithm_to_alias_subresults["postfilter"]) \
                        == len(algorithm_to_alias_subresults["tree_batch_cext"])
                    for alias in algorithm_to_alias_subresults["postfilter"]:
                        tsv_out.append({
                            "pattern_id" : str(pattern_id) + f"({alias})",
                            "join_pattern" : f"({alias})",
                            "n_tables" : jp.n_tables,
                            "n_queries" : len(workload_summary.id_to_alias_to_qid[pattern_id][alias]),
                            "sec_postfilter": algorithm_to_alias_subresults["postfilter"][alias][0],
                            "none_postfilter": algorithm_to_alias_subresults["postfilter"][alias][1],
                            "additional_at_least_postfilter": algorithm_to_alias_subresults["postfilter"][alias][2],
                            "sec_postfilter_parallel": algorithm_to_alias_subresults["postfilter_parallel"][alias][0],
                            "none_postfilter_parallel": algorithm_to_alias_subresults["postfilter_parallel"][alias][1],
                            "additional_at_least_postfilter_parallel": algorithm_to_alias_subresults["postfilter_parallel"][alias][2],
                            "sec_postfilter_materialized": algorithm_to_alias_subresults["postfilter_materialized"][alias][0],
                            "none_postfilter_materialized": algorithm_to_alias_subresults["postfilter_materialized"][alias][1],
                            "additional_at_least_postfilter_materialized": algorithm_to_alias_subresults["postfilter_materialized"][alias][2],
                            "sec_tree_batch_cext": algorithm_to_alias_subresults["tree_batch_cext"][alias][0],
                            "sec_tree_batch_cext_parallel": algorithm_to_alias_subresults["tree_batch_cext_parallel"][alias][0],
                        })
                        if "seq" in algorithms:
                            tsv_out[-1]["sec_seq"] = ""
                            tsv_out[-1]["none_seq"] = ""
                            tsv_out[-1]["additional_at_least_seq"] = ""
                        if "hybrid" in algorithms:
                            tsv_out[-1]["sec_hybrid"] = algorithm_to_alias_subresults["hybrid"][alias][0]
                            tsv_out[-1]["switch_to_hybrid"] = ""

            tsv_writer.writerows(tsv_out)

                

              