# truth_files = [
#     "seq_synthetic.ans",
#     "seq_scale.ans",
#     "seq_job_light_single.ans",
#     "seq_job_light_join.ans",
#     "seq_stats_ceb.ans",
#     "seq_stats_ceb_single.ans",
#     "seq_stats_ceb_join.ans",
#     "seq_job_light.ans",
#     "seq_dsb_grasp_20k.ans",
#     "seq_job_light_1k.ans",
#     "seq_job_light_2k.ans",
#     "seq_job_light_4k.ans",
# ]
# to_check_files = [
#     "postfilter_synthetic.ans",
#     "postfilter_scale.ans",
#     "postfilter_job_light_single.ans",
#     "postfilter_job_light_join.ans",
#     "postfilter_stats_ceb.ans",
#     "postfilter_stats_ceb_single.ans",
#     "postfilter_stats_ceb_join.ans",
#     "postfilter_job_light.ans",
#     "postfilter_dsb_grasp_20k.ans",
#     "postfilter_job_light_1k.ans",
#     "postfilter_job_light_2k.ans",
#     "postfilter_job_light_4k.ans",
# ]
# workload_files = [
#     "../workload/synthetic.sql",
#     "../workload/scale.sql",
#     "../workload/job_light_single.sql",
#     "../workload/job_light_join.sql",
#     "../workload/stats_ceb.sql",
#     "../workload/stats_ceb_single.sql",
#     "../workload/stats_ceb_join.sql",
#     "../workload/job_light.sql",
#     "../workload/dsb_grasp_20k.sql",
#     "../workload/job_light_1k.sql",
#     "../workload/job_light_2k.sql",
#     "../workload/job_light_4k.sql",
# ]

truth_files = [
    "seq_synthetic.ans",
    "seq_scale.ans",
    "seq_job_light_join.ans",
    "seq_job_light_single.ans",
    "seq_stats_ceb.ans",
    "seq_stats_ceb_single.ans",
    "seq_stats_ceb_join.ans",
    "seq_job_light.ans",
    "seq_dsb_grasp_20k.ans",
    "seq_job_light_1k.ans",
    "seq_job_light_2k.ans",
    "seq_job_light_4k.ans",
]
to_check_files = [
    "tree_batch_cext_synthetic.ans",
    "tree_batch_cext_scale.ans",
    "tree_batch_cext_job_light_join.ans",
    "tree_batch_cext_job_light_single.ans",
    "tree_batch_cext_stats_ceb.ans",
    "tree_batch_cext_stats_ceb_single.ans",
    "tree_batch_cext_stats_ceb_join.ans",
    "tree_batch_cext_job_light.ans",
    "tree_batch_cext_dsb_grasp_20k.ans",
    "tree_batch_cext_job_light_1k.ans",
    "tree_batch_cext_job_light_2k.ans",
    "tree_batch_cext_job_light_4k.ans",
]
workload_files = [
    "../workload/synthetic.sql",
    "../workload/scale.sql",
    "../workload/job_light_join.sql",
    "../workload/job_light_single.sql",
    "../workload/stats_ceb.sql",
    "../workload/stats_ceb_single.sql",
    "../workload/stats_ceb_join.sql",
    "../workload/job_light.sql",
    "../workload/dsb_grasp_20k.sql",
    "../workload/job_light_1k.sql",
    "../workload/job_light_2k.sql",
    "../workload/job_light_4k.sql",
]

# truth_files = [
#     # "seq_stats_ceb_join.ans",
#     # "seq_stats_ceb_join.ans",
#     "seq_stats_ceb.ans",
#     # "seq_stats_ceb_single.ans",
# ]
# to_check_files = [
#     # "seq_DEBUG.debug",
#     # "postfilter_DEBUG.debug",
#     "tree_batch_cext_DEBUG.debug",
#     # "hybrid_DEBUG.debug",
# ]
# workload_files = [
#     # "../workload/stats_ceb_join.sql",
#     # "../workload/stats_ceb_join.sql",
#     "../workload/stats_ceb.sql",
#     # "../workload/stats_ceb_single.sql",
# ]

# truth_files = [
#     "seq_synthetic.ans",
#     "seq_scale.ans",
#     "seq_job_light_join.ans",
#     "seq_job_light_single.ans",
#     "seq_stats_ceb.ans",
#     "seq_stats_ceb_single.ans",
#     "seq_stats_ceb_join.ans",
#     "seq_job_light.ans",
#     "seq_dsb_grasp_20k.ans",
# ]
# to_check_files = [
#     "hybrid_synthetic.ans",
#     "hybrid_scale.ans",
#     "hybrid_job_light_join.ans",
#     "hybrid_job_light_single.ans",
#     "hybrid_stats_ceb.ans",
#     "hybrid_stats_ceb_single.ans",
#     "hybrid_stats_ceb_join.ans",
#     "hybrid_job_light.ans",
#     "hybrid_dsb_grasp_20k.ans",
# ]
# workload_files = [
#     "../workload/synthetic.sql",
#     "../workload/scale.sql",
#     "../workload/job_light_join.sql",
#     "../workload/job_light_single.sql",
#     "../workload/stats_ceb.sql",
#     "../workload/stats_ceb_single.sql",
#     "../workload/stats_ceb_join.sql",
#     "../workload/job_light.sql",
#     "../workload/dsb_grasp_20k.sql",
# ]


skip=["None", "0"] # a list of str, what are skipping for correctness checking

for i in range(len(truth_files)):
    truth_file = truth_files[i]
    to_check_file = to_check_files[i]
    workload_file = workload_files[i]

    assert truth_file.startswith("seq_") and truth_file.endswith(".ans")
    assert to_check_file.endswith(".ans") or to_check_file.endswith(".debug")

    with open(truth_file, "r") as truth_fin, open(to_check_file, "r") as to_check_fin, open(workload_file, "r") as workload_fin:
        truth_cnts = []
        for line in truth_fin.readlines():
            if line.startswith("#"):
                continue
            truth_cnts.append(line.strip().split(",")[0])
        to_check_cnts = []
        for line in to_check_fin.readlines():
            if line.startswith("#"):
                continue
            to_check_cnts.append(line.strip().split(",")[0])
        assert len(truth_cnts) == len(to_check_cnts)
        fail_ids = []
        for i in range(len(truth_cnts)):
            if to_check_cnts[i] in skip or truth_cnts[i] in skip:
                continue
            if truth_cnts[i] != to_check_cnts[i]:
                fail_ids.append(i)
        if len(fail_ids) == 0:
            print(f"{to_check_file} is identical to {truth_file}, while skipping '{skip}'")
        else:
            print(f"Fail on {len(fail_ids)}")
            print("First few incorrect query ids", fail_ids[:min(10, len(fail_ids))])
            # sqls = []
            # for line in workload_fin.readlines():
            #     if line.startswith("--"):
            #         continue
            #     sqls.append(line.strip())
            # for x in fail_ids:
            #     print(sqls[x] + "||" + truth_cnts[x])

