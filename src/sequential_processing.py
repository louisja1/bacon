# Process all the counting queries in sequence

from tqdm import tqdm
from datetime import datetime
import json
import psycopg2
import sys

from db_connection import run_counting_sql, get_latency

# @return: flag_for_timeout, count, latency, plan_info
def process_a_sql(
    sql,
    dbname,
    timeout_in_sec,
):
    try:
        # get the count
        count = run_counting_sql(
            dbname=dbname, 
            sql=sql, 
            timeout_in_sec=timeout_in_sec
        )
        # get the latency
        latency, plan_info = get_latency(
            dbname=dbname, 
            sql=sql, 
            n_repetitions=1, 
            extract_plan_info=True, 
            timeout_in_sec=timeout_in_sec
        )
        return False, count, latency, plan_info
    except psycopg2.Error as e:
        return True, None, None, None

if __name__ == "__main__":
    json_filename = sys.argv[1]
    with open(json_filename) as json_fin:
        config = json.load(json_fin)
        path_to_sql = config["path_to_sql"]
        dir_to_output = config["dir_to_output"]
        if "debug" in config:
            debug = config["debug"]
        else:
            debug = True
        if "skip_first_k_queries" in config:
            skip_first_k_queries = config["skip_first_k_queries"]
        else:
            skip_first_k_queries = None
        if "timeout_in_sec" in config:
            timeout_in_sec = config["timeout_in_sec"]
        else:
            timeout_in_sec = None

        if debug:
            path_to_output = dir_to_output\
                + "seq_DEBUG.debug"
        else:
            path_to_output = dir_to_output + "seq_"\
                + path_to_sql.split("/")[-1].split(".sql")[0] + ".ans"

        info_at_the_beginning = \
            f"#timestamp:{datetime.now()}\n#header:count,secs,plan_info\n"


        with open(path_to_sql, "r") as fin, \
            open(path_to_output, "w") as fout:

            fout.write(info_at_the_beginning)
            line_id = -1
            qid = 0
            dbname = None
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
                    sql = line.strip()
                    if skip_first_k_queries is None \
                        or qid + 1 > skip_first_k_queries:
                        (
                            is_timeout, 
                            count, 
                            latency, 
                            plan_info
                        ) = process_a_sql(
                            sql=sql,
                            dbname=dbname,
                            timeout_in_sec=timeout_in_sec
                        ) 
                        if is_timeout:
                            fout.write("None,None,None\n")
                        else:
                            fout.write(
                                f"{count},{latency:.6f},{plan_info}\n"
                            )
                    else:
                        fout.write("None,None,None\n")
                    qid += 1
                else:
                    raise ValueError(
                        f"Cannot parse line {line_id}: line"
                    )

