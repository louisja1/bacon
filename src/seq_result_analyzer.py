from schema_related import JoinPattern
from utility import parse_one_imdb_or_stats_or_dsb
from tqdm import tqdm

workload_and_answer_files = [
    # ("../workload/synthetic.sql", "../results/seq_synthetic.ans"),
    # ("../workload/job_light_single.sql", "../results/seq_job_light_single.ans"),
    # ("../workload/job_light_join.sql", "../results/seq_job_light_join.ans"),
    # ("../workload/scale.sql", "../results/seq_scale.ans"),
    ("../workload/stats_ceb_single.sql", "../results/seq_stats_ceb_single.ans"),
    # ("../workload/stats_ceb_join.sql", "../results/seq_stats_ceb_join.ans"),
]

if __name__ == "__main__":
    for workload_file, answer_file in workload_and_answer_files:
        with open(workload_file, "r") as wokload_fin, open(answer_file, "r") as answer_fin:
            qid_to_jp = []
            jp_to_nqid = {}

            line_id = -1
            dbname = None
            for line in tqdm(wokload_fin.readlines()):
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
                    elif dbname in ["imdb", "stats"]:
                        this_sql = line.strip()
                        _, join_conds, _, _, _ = parse_one_imdb_or_stats_or_dsb(sql=this_sql)
                    else:
                        raise NotImplementedError
                    cur_jp = JoinPattern(join_conds=join_conds, dbname=dbname)
                    qid_to_jp.append(cur_jp)
                    if cur_jp not in jp_to_nqid:
                        jp_to_nqid[cur_jp] = 0
                    jp_to_nqid[cur_jp] += 1
                else:
                    raise ValueError(f"Cannot parse line {line_id}: line")

            qid = 0
            jp_to_total_execution_time = {}
            jp_to_number_of_none = {}
            for line in tqdm(answer_fin.readlines()):
                if line.startswith("#"):
                    pass
                else:
                    if line.strip().split(",")[1] == "None":
                        secs = None
                    else:
                        true_cnt = int(line.strip().split(",")[0])
                        secs = float(line.strip().split(",")[1])
                    if qid_to_jp[qid] not in jp_to_total_execution_time:
                        jp_to_total_execution_time[qid_to_jp[qid]] = 0.
                    if qid_to_jp[qid] not in jp_to_number_of_none:
                        jp_to_number_of_none[qid_to_jp[qid]] = 0
                    if secs is None:
                        jp_to_number_of_none[qid_to_jp[qid]] += 1
                    else:
                        jp_to_total_execution_time[qid_to_jp[qid]] += secs
                    qid += 1
            for i, jp in enumerate(jp_to_total_execution_time):
                # print(f"JoinPattern [{jp}] has {jp_to_nqid[jp]} queries, and take {jp_to_total_execution_time[jp]:.6f} secs + {jp_to_number_of_none[jp]} x Nones in total")
                print(f"{i}\t{jp}\t{jp_to_nqid[jp]}\t{jp_to_total_execution_time[jp]:.6f} + {jp_to_number_of_none[jp]} x Nones")
            
            print(f"total execution time: {sum([jp_to_total_execution_time[jp] for jp in jp_to_total_execution_time]):.6f}")
            print(f"total number of Nones: {sum([jp_to_number_of_none[jp] for jp in jp_to_number_of_none])}")