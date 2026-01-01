# given a query workload, generate more queries
# accordingly.

import random
from tqdm import tqdm
from collections import defaultdict

from workload_summary import WorkloadSummary
from utility import parse_one_imdb_or_stats_or_dsb
from db_connection import (
    get_connection, 
    close_connection,
)

path_to_sql = "../workload/job_light.sql"
sizes = [1000, 2000, 4000]
random_seed = 42
pool_size = 50000

random.seed(random_seed)

col_to_pool = {}

def get_random_val(
    cursor,
    tab,
    alias,
    col,
    at_least
):
    if col not in col_to_pool:
        cursor.execute(
            f"SELECT * FROM ( "
            f"SELECT DISTINCT {col} FROM {tab} {alias} "
            f"WHERE {col} IS NOT NULL "
            f") ORDER BY random() "
            f"LIMIT {pool_size};"
        )
        l = cursor.fetchall()
        col_to_pool[col] = [x[0] for x in l]

    length = len(col_to_pool[col])
    while True:
        pos = random.randint(0, length - 1)
        if at_least is None \
            or col_to_pool[col][pos] >= at_least:
            return col_to_pool[col][pos]


if __name__ == "__main__":
    with open(path_to_sql, "r") as fin:
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
                workload_summary.insert(
                    dbname=dbname,
                    qid=qid,
                    join_conds=join_conds,
                    column_to_selections=column_to_selections,
                    this_query_tab_to_alias=cur_tab_to_alias,
                )
                qid += 1
            else:
                raise ValueError(
                    f"Cannot parse line {line_id}: line"
                )
        
        n_q = qid
        
        conn, cursor = get_connection(dbname=dbname)
        workload_summary.get_column_metadata(
            cursor=cursor, 
            alias_to_tab=alias_to_tab
        )
        cursor.execute("SELECT setseed(%s);", (random.random(),))
        
        for pattern_id in workload_summary.id_to_qid:
            alias_to_sel_col = defaultdict(list)
            for sel_col in workload_summary\
                .id_to_cols_with_selection[pattern_id]:
                alias = sel_col.split(".")[0]
                if alias not in alias_to_sel_col:
                    alias_to_sel_col[alias] = []
                alias_to_sel_col[alias].append(sel_col)

            for alias in alias_to_sel_col:
                for sel_col in alias_to_sel_col[alias]:
                    workload_summary.process_a_selected_col(
                        pattern_id=pattern_id, 
                        col=sel_col, 
                        batch_size=50000
                    )
        
        for new_size in sizes:
            new_file = path_to_sql.replace(
                ".sql", f"_{new_size // 1000}k.sql"
            )
            with open(new_file, "w") as fout:
                fout.write(f"--db:{dbname}\n")
                fout.write(f"--from:src/generate_more_query_according_to.py\n")
                fout.write(
                    f"--description:{new_size} new queries generated "
                    f"with random seed {random_seed}\n"
                )

                # randomly choose the references for each new query
                modified_according_to = [
                    random.randrange(n_q) for _ in range(new_size)
                ]

                qid_to_jp_id = {
                    qid : id
                    for id, qids in workload_summary\
                        .id_to_qid.items()
                    for qid in qids
                }
                jp_id_to_jp = {
                    jp_id : jp
                    for jp, jp_id in workload_summary\
                        .jp_to_id.items()
                }

                for qid in tqdm(modified_according_to):
                    pattern_id = qid_to_jp_id[qid]
                    pattern = jp_id_to_jp[pattern_id]

                    select_chunks = []
                    for sel_col in workload_summary\
                        .id_to_cols_to_qid_and_chunks[pattern_id]:
                            if qid in workload_summary\
                                .id_to_cols_to_qid_and_chunks[
                                    pattern_id
                                ][sel_col]:
                                lb = 0
                                ub = len(
                                    workload_summary\
                                        .id_to_cols_to_bucketization[
                                            pattern_id
                                        ][sel_col].chunks
                                ) - 1

                                old_le, old_ri = workload_summary\
                                    .id_to_cols_to_qid_and_chunks[
                                        pattern_id
                                    ][sel_col][qid]
                                
                                diff = \
                                    workload_summary\
                                    .id_to_cols_to_bucketization[
                                        pattern_id
                                    ][sel_col].chunks[old_ri][1]\
                                    - \
                                    workload_summary\
                                    .id_to_cols_to_bucketization[
                                        pattern_id
                                    ][sel_col].chunks[old_le][0]

                                alias = sel_col.split(".")[0]

                                if old_le == old_ri:
                                    new_v = get_random_val(
                                        cursor,
                                        alias_to_tab[alias],
                                        alias,
                                        sel_col,
                                        None,
                                    )
                                    select_chunks.append(
                                        f"{sel_col} = {new_v}"
                                    )
                                    continue

                                if old_le != lb:
                                    new_le = get_random_val(
                                        cursor,
                                        alias_to_tab[alias],
                                        alias,
                                        sel_col,
                                        None,
                                    )
                                    select_chunks.append(
                                        f"{sel_col} >= {new_le}"
                                    )
                                else:
                                    new_le = None

                                if old_ri != ub:
                                    if new_le is None:
                                        new_ri = get_random_val(
                                            cursor,
                                            alias_to_tab[alias],
                                            alias,
                                            sel_col,
                                            new_le,
                                        )
                                    else:
                                        new_ri = min(
                                            workload_summary.col_to_max[sel_col],
                                            new_le + diff
                                        )
                                    select_chunks.append(
                                        f"{sel_col} < {new_ri}"
                                    )
                                else:
                                    new_ri = None

                    new_sql = "SELECT COUNT(*) FROM "
                    new_sql += pattern.get_from_clause(alias_to_tab)
                    join_clause = pattern.get_join_clause()
                    if len(join_clause) > 0:
                        new_sql += " WHERE " + join_clause
                        if len(select_chunks) > 0:
                            new_sql += " AND " + " AND ".join(select_chunks)
                    else:
                        if len(select_chunks) > 0:
                            new_sql += " WHERE " + " AND ".join(select_chunks)
                    new_sql += ";"
                    fout.write(new_sql + "\n")
                                
        close_connection(conn, cursor)

            
        
        