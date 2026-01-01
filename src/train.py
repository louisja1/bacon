# 1. Collect training data
# 2. Train and dump

from tqdm import tqdm
from datetime import datetime
from collections import defaultdict

import json
import os
import sys
import json
import pandas as pd
import numpy as np
import time

from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import precision_recall_curve

import pickle
import matplotlib.pyplot as plt

from utility import parse_one_imdb_or_stats_or_dsb
from db_connection import get_connection, close_connection
from tree_batch_cext import WorkloadSummary
from optimizer import Optimizer, get_features, extract_one, Selector

def generate_training_data(
    paths_to_sql,
    dir_to_training_data
):
    for sql_file in paths_to_sql:
        sql_file_base = os.path.basename(sql_file)
        training_data_file = dir_to_training_data\
            + os.path.splitext(sql_file_base)[0] 
        training_data_file += ".json"
        
        if os.path.exists(training_data_file):
            continue # skip the collection if exists
        
        with open(sql_file, "r") as fin:
            line_id = -1
            dbname = None
            qid = 0
            workload_summary = WorkloadSummary(
                group_by="join pattern"
            )
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

            conn, cursor = get_connection(dbname=dbname)
            cursor.execute("SET max_parallel_workers_per_gather = 0;")
            workload_summary.get_column_metadata(
                cursor=cursor, 
                alias_to_tab=alias_to_tab
            )

            optimizer = Optimizer(
                cursor, 
                alias_to_tab,
                load_accurate_unfiltered_join_size=False
            )
            
            json_out = []
            for pattern, pattern_id in workload_summary.iterate_by_group():
                if workload_summary.group_by == "join pattern":
                    jp = pattern
                else:
                    jp = pattern.join_pattern
                
                if jp.n_tables == 1:
                    participated_alias_list = list(
                        workload_summary.id_to_alias_to_qid[pattern_id].keys()
                    )
                else:
                    participated_alias_list = list(jp.alias_to_join_col.keys())
                
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

                if jp.n_tables == 1:
                    # split this pattern into multiple single-table instances
                    json_out.extend(
                        [
                            optimizer.collect_stats(
                                pattern_id=pattern_id,
                                source_jp=jp,
                                participated_alias_list=[alias],
                                alias_to_sel_col=alias_to_sel_col,
                                workload_summary=workload_summary,
                                alias_to_tab=alias_to_tab,
                            )
                            for alias in participated_alias_list
                        ]
                    )
                else:
                    json_out.append(
                        optimizer.collect_stats(
                            pattern_id=pattern_id,
                            source_jp=jp,
                            participated_alias_list=participated_alias_list,
                            alias_to_sel_col=alias_to_sel_col,
                            workload_summary=workload_summary,
                            alias_to_tab=alias_to_tab,
                        )
                    )
            
            close_connection(conn, cursor)

            with open(training_data_file, "w") as fout:
                json.dump(json_out, fout, indent=2)

        
def generate_training_label(
    paths_to_sql,
    dir_to_training_data
):
    for sql_file in paths_to_sql:
        sql_file_base = os.path.basename(sql_file)
        training_label_file = dir_to_training_data\
            + os.path.splitext(sql_file_base)[0] + "_label.json"
        aggregated_result_file = "../results/"\
            + os.path.splitext(sql_file_base)[0] + ".tsv"
        
        if os.path.exists(training_label_file):
            continue # skip the collection if exists
        
        df = pd.read_csv(aggregated_result_file, sep="\t")
        json_out = {
            "label_identification" : {
                "1": "seqproc wins over tree_batch_cext with a "
                    "significant margin (and also postfilter), "
                    "which is either: 1) more than 1.5x faster than "
                    "tree_batch_cext; or 2) more than 10 seconds "
                    "faster than tree_batch_cext.",
                "2": "postfilter wins over tree_batch_cext with a "
                    "significant margin (and also seqproc), "
                    "which is either: 1) more than 1.5x faster than "
                    "tree_batch_cext; or 2) more than 10 seconds "
                    "faster than tree_batch_cext.",
                "0": "otherwise, use tree_batch_cext as a conservative "
                    "choice."
            }
        }

        def is_safe(baseline_time, tree_batch_cext_time):
            return baseline_time != "None" and (
                float(baseline_time) * 1.5 < float(tree_batch_cext_time) 
                or float(baseline_time) + 10. < float(tree_batch_cext_time)
            )
            
        json_out["labels"] = []
        sec_seqproc = df["sec_seq"].tolist()
        sec_postfilter = df["sec_postfilter"].tolist()
        sec_tree_batch_cext = df["sec_tree_batch_cext"].tolist()
        assert len(sec_seqproc) == len(sec_tree_batch_cext)
        assert len(sec_postfilter) == len(sec_tree_batch_cext)

        for i in range(len(sec_postfilter)):
            if df.iloc[i]["join_pattern"] == "()":
                # skip the single-table JP, because it is split
                #     into multiple instances
                continue
            
            safe1 = is_safe(sec_seqproc[i], sec_tree_batch_cext[i])
            safe2 = is_safe(sec_postfilter[i], sec_tree_batch_cext[i])
            
            if safe1 and not safe2:
                # Label as 1 (indicating we should use seqproc)
                json_out["labels"].append(1)
            elif safe2 and not safe1:
                # Label as 2 (indicating we should use postfilter)
                json_out["labels"].append(2)
            elif safe1 and safe2:
                # If both are safe, choose the faster one
                if float(sec_seqproc[i]) < float(sec_postfilter[i]):
                    json_out["labels"].append(1)
                else:
                    json_out["labels"].append(2)
            else:
                # Otherwise, label as 0 (indicating we should 
                #     use tree_batch_cext as a conservative choice)
                json_out["labels"].append(0)

        with open(training_label_file, "w") as fout:
            json.dump(json_out, fout, indent=2)

def _get_training_conf():
    features = get_features()

    # train-validation split ratio
    train_val_split = 0.25 # the ratio of validation data

    # random forest classifier
    classifier = "RandomForestClassifier"
    n_estimators = 100
    max_depth = None
    min_samples_leaf = 2
    class_weight = {
        0 : 1, 
        1 : 2,
        2 : 2
    }  
    # misclassifying class 1 or 2 (i.e., mistakingly predicting 
    #     either baseline wins) is treated 3x worse

    # high-precision calibration
    desired_threshold = 0.9

    return (
        features, 
        train_val_split,
        classifier,
        n_estimators,
        max_depth,
        min_samples_leaf,
        class_weight,
        desired_threshold
    )

def train_and_dump(
    paths_to_training_sql,
    dir_to_models,
    random_state=315
):
    start_time = time.time()
    timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")

    model_file = dir_to_models + "/" + timestamp_str + ".pkl"
    metadata_file = dir_to_models + "/" + timestamp_str + "_metadata.txt"

    (
        features, 
        train_val_split,
        classifier,
        n_estimators,
        max_depth,
        min_samples_leaf,
        class_weight,
        desired_threshold
    ) = _get_training_conf()

    meta_json = {
        "training_sql" : paths_to_training_sql,
        "features" : features,
        "train_val_split" : train_val_split,
        "classifier" : classifier,
        "classifier_params" : {
            "n_estimators" : n_estimators,
            "max_depth" : max_depth,
            "min_samples_leaf" : min_samples_leaf,
            "class_weight" : class_weight
        },
        "precision_threshold_for_calibration" : \
            desired_threshold
    }

    with open(model_file, "wb") as fmodel, \
        open(metadata_file, "w") as fmeta:

        data = []
        label = []
        for sql_file in paths_to_training_sql:
            sql_file_base = os.path.basename(sql_file)
            data_file = dir_to_training_data\
                + os.path.splitext(sql_file_base)[0]
            data_file += ".json"
            label_file = dir_to_training_data\
                + os.path.splitext(sql_file_base)[0] + "_label.json"
            
            with open(data_file, "r") as fdata, \
                open(label_file, "r") as flabel:
                data_json = json.load(fdata)
                label_json = json.load(flabel)

                for instance in data_json:
                    data.append(extract_one(instance))
                label.extend(label_json["labels"])
                assert len(data) == len(label)
        
        # label_to_cnt = {}
        # for ele in label:
        #     if ele not in label_to_cnt:
        #         label_to_cnt[ele] = 0
        #     label_to_cnt[ele] += 1
        # print("!!!label distribution:", label_to_cnt)

        X = pd.DataFrame(data, columns=features)
        y = np.array(label)
        X_train, X_val, y_train, y_val = train_test_split(
            X, 
            y, 
            test_size=train_val_split, 
            random_state=random_state, 
            stratify=y
        )

        model = RandomForestClassifier(
            n_estimators=n_estimators,
            max_depth=max_depth,
            min_samples_leaf=min_samples_leaf,
            class_weight=class_weight,  
            random_state=random_state
        )

        model.fit(X_train, y_train)

        def choose_threshold_for_class(
            model, 
            X_val, 
            y_val, 
            class_id, 
            desired_threshold
        ):
            p_val = model.predict_proba(X_val)[:, class_id]
            precision, recall, thresholds = precision_recall_curve(
                (y_val == class_id).astype(int), 
                p_val
            )

            # we utilize the precision-recall curve to find the best threshold
            #  that yields at least desired_precision on validation
            valid_idxs = np.where(
                precision[:-1] >= desired_threshold
            )[0]

            if len(valid_idxs) == 0:
                # cannot reach desired precision on validation; 
                #     pick the max precision threshold
                best_idx = np.argmax(precision[:-1])
            else:
                # pick highest threshold among valid indices to
                #     maximize conservativeness
                best_idx = valid_idxs[-1]

            return thresholds[best_idx]


        # Till this point, the model will predict the class having the highest 
        #   probability. To make it more conservative,  we will choose a threshold 
        #   that yields high precision (i.e., almost no false positives) on the 
        #   validation set.

        threshold_for_class_1 = choose_threshold_for_class(
            model, 
            X_val, 
            y_val, 
            1, 
            desired_threshold
        )
        threshold_for_class_2 = choose_threshold_for_class(
            model, 
            X_val, 
            y_val, 
            2, 
            desired_threshold
        )

        meta_json["threshold_for_class_1"] = threshold_for_class_1
        meta_json["threshold_for_class_2"] = threshold_for_class_2

        meta_json["end_to_end_training_time"] = \
            time.time() - start_time

        json.dump(meta_json, fmeta, indent=2)
        pickle.dump(
            Selector(
                model, 
                threshold_for_class_1, 
                threshold_for_class_2
            ),
            fmodel    
        )
        return model_file

def print_model_importance(
    model_file
):
    importance_file = model_file.replace(".pkl", "_importance.txt")
    features = get_features()

    with open(model_file, "rb") as fmodel, \
        open(importance_file, "w") as fout:
        model = pickle.load(fmodel).model
        importances = model.feature_importances_
        importance_dict = {
            name: float(val)
            for name, val in zip(features, importances)
        }
        json.dump(importance_dict, fout, indent=2)
    
# def _evaluate_model(
#     model_name,
#     paths_to_evaluation_sql
# ):
#     # test against the rest
#     with open(model_name, "rb") as fmodel:
#         selector = pickle.load(fmodel)
#         features = get_features()

#         _cnt = 0
#         for sql_file in paths_to_evaluation_sql:
#             sql_file_base = os.path.basename(sql_file)
#             data_file = dir_to_training_data\
#                 + os.path.splitext(sql_file_base)[0] + ".json"
#             label_file = dir_to_training_data\
#                 + os.path.splitext(sql_file_base)[0] + "_label.json"
#             aggregated_result_file = "../results/"\
#                 + os.path.splitext(sql_file_base)[0] + ".tsv"
            
#             df = pd.read_csv(aggregated_result_file, sep="\t")
#             sec_seqproc = df[
#                 df["join_pattern"] != "()"
#             ]["sec_seq"].tolist()
#             sec_postfilter = df[
#                 df["join_pattern"] != "()"
#             ]["sec_postfilter"].tolist()
#             sec_tree_batch_cext = df[
#                 df["join_pattern"] != "()"
#             ]["sec_tree_batch_cext"].tolist()

#             with open(data_file, "r") as fdata, \
#                 open(label_file, "r") as flabel:
#                 data_json = json.load(fdata)
#                 label_json = json.load(flabel)

#                 data = []
#                 label = []

#                 for instance in data_json:
#                     data.append(extract_one(instance))
#                 label.extend(label_json["labels"])
#                 assert len(data) == len(label)

#                 print(f"sql file: {sql_file}")
#                 X_test = pd.DataFrame(data, columns=features)
#                 y_pred = selector.predict(X_test)

#                 for i in range(len(data_json)):
#                     flag = False
#                     if y_pred[i] == 1:
#                         flag = True
#                         choosed_time = sec_seqproc[i]
#                     elif y_pred[i] == 2:
#                         flag = True
#                         choosed_time = sec_postfilter[i]
#                     else:
#                         choosed_time = sec_tree_batch_cext[i]

#                     if flag:
#                         _cnt += 1
#                         switched_to = "seqproc" if y_pred[i] == 1 else "postfilter"
#                         print(
#                             f"{sql_file}\tpattern[{i}/{len(data_json)}]: "
#                             f"switched to {switched_to}"
#                         )
#                         if choosed_time == "None" \
#                             or (
#                                 sec_tree_batch_cext[i] != "None" 
#                                 and float(choosed_time) \
#                                     > float(sec_tree_batch_cext[i])
#                             ):
#                             print(
#                                 f"{sql_file}\tpattern[{i}/{len(data_json)}]: "
#                                 f"switched to {switched_to} and had overhead {choosed_time}, "
#                                 f"which is worse than our approach with {sec_tree_batch_cext[i]}"
#                             )
#         print("# of switch to baseline", _cnt)         

if __name__ == "__main__":
    json_filename = sys.argv[1]
    assert json_filename.startswith("../input_configs/train")

    with open(json_filename) as json_fin:
        config = json.load(json_fin)
        paths_to_sql = config["paths_to_sql"]
        paths_to_training_sql = config["paths_to_training_sql"]
        dir_to_training_data = config["dir_to_training_data"]
        dir_to_models = config["dir_to_models"]

        generate_training_data(
            paths_to_sql,
            dir_to_training_data
        )

        generate_training_label(
            paths_to_sql,
            dir_to_training_data
        )

        model_name = train_and_dump(
            paths_to_training_sql,
            dir_to_models,
            random_state=315
        )

        print_model_importance(
            model_name
        )

        # _evaluate_model(
        #     model_name,
        #     [
        #         sql 
        #         for sql in paths_to_sql 
        #         if sql not in paths_to_training_sql
        #     ]
        # )

            