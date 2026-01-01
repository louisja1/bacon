# NOTE: dsb_grasp.csv in the same directory is downloaded from 
#     https://github.com/shoupzwu/GRASP/blob/master/queries/dsb.csv
#    , which is a special format for training NN-based cardinality 
#    estimators (e.g. GRASP, MSCN, etc.). This code will convert it
#    to standard SQL format. And it will be dsb_grasp.sql in the same
#    directory.

# NOTE:
# (1) seems like GRASP has a numerical/categorical column definition
#        for `cd.cd_education_status`, `cd.cd_marital_status`, 
#        `cd.cd_gender`, `s.s_state`, which are defined as char(...) in 
#        both DSB and TPC_DS. So we just drop the lines having this column.

import random

input_csv = 'dsb_grasp.csv'
full_output_sql = 'dsb_grasp.sql'

outputs = [
    {
        "output_sql": "../dsb_grasp_20k.sql",
        "random_seed": 42,
        "num_queries": 20000,
    },
]

def seeded_sample_from_list(input_list, sample_size, random_seed):
    rng = random.Random(random_seed)
    return rng.sample(input_list, sample_size)

with (
    open(input_csv, 'r') as fin, 
    open(full_output_sql, 'w') as fout,
):
    sqls = []
    for line in fin.readlines():
        if 'cd_education_status' in line \
            or 'cd_marital_status' in line \
                or 'cd_gender' in line \
                    or 's_state' in line:
            continue # see NOTE (1) above
        
        assert line.count('#') == 3
        # each line has exactly three '#' to separate three parts: tables
        #     in FROM clause, join conditions, selection conditions, true 
        #     card.
        parts = line.strip().split('#')

        select_clause = "SELECT COUNT(*)"
        from_clause = " FROM " + parts[0].strip()
        join_part = " AND ".join(parts[1].strip().split(","))
        
        elems = parts[2].strip().split(",")
        assert len(elems) % 3 == 0 
        # each selection condition has three parts: column, operator, 
        #     value
        selection_part = " AND ".join(
            f"{elems[i]} {elems[i+1]} {elems[i+2]}"
            for i in range(0, len(elems), 3)
        )
        
        if len(join_part) > 0 or len(selection_part) > 0:
            where_clause = " WHERE "
            if len(join_part) > 0:
                where_clause += join_part
            if len(selection_part) > 0:
                if len(join_part) > 0:
                    where_clause += " AND "
                where_clause += selection_part
        sqls.append(select_clause + from_clause + where_clause + ";")
        fout.write(select_clause + from_clause + where_clause + ";\n")

    for output_elems in outputs:
        output_sql = output_elems["output_sql"]
        random_seed = output_elems["random_seed"]
        num_queries = output_elems["num_queries"]

        with open(output_sql, "w") as fout_sampled:
            fout_sampled.write("--db:dsb-sf2\n")
            fout_sampled.write(
                "--from:https://github.com/shoupzwu/GRASP/blob/master/queries/dsb.csv\n"
            )
            fout_sampled.write(
                f"--description:{num_queries} queries, "
                f"random_seed={random_seed}. "
                "See `workload/raw/dsb_grasp_csv_to_sql.py` for details\n"
            )

            sampled_sqls = seeded_sample_from_list(
                input_list=sqls,
                sample_size=num_queries,
                random_seed=random_seed
            )
            
            for _sql in sampled_sqls:
                fout_sampled.write(_sql + "\n")
