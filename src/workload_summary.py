# Summarize the workload
# 1. group the workload by join/query pattern
# 2. prepare information per pattern/column/selection
# 3. collect metadata
# 4. call `process_a_selected_column()` for creating buckets

from collections import defaultdict

from bucketization import Bucketization
from schema_related import JoinPattern, QueryPattern
from utility import parse, nxt_val, prev_val

import copy

class WorkloadSummary:
    def __init__(self, group_by):
        self.group_by = group_by
        if self.group_by == "join pattern":
            self.jp_to_id = {} # JoinPattern to id
        elif self.group_by == "query pattern":
            self.qp_to_id = {} # QueryPattern to id
        else:
            raise NotImplementedError()
        self.n_groups = 0
        
        # {id: [qid0, qid1, ...]}
        self.id_to_qid = {} 
         # {id : True/False}
        self.id_to_is_no_join = {}
        # {id: {alias : [qid0, qid1, ...]}}
        self.id_to_alias_to_qid = {} 

        # {id : {col -> [(qid, selections), ...]}}
        self.id_to_cols_with_selection = {}
        # {id : {col -> bucketization}}
        self.id_to_cols_to_bucketization = {} 
        # {id : {col -> {qid : (x, y), ...}}}, representing 
        #     that qid's predicate on col can be resolved by 
        #     the sum of chunk[x] ... chunk[y] inclusively
        self.id_to_cols_to_qid_and_chunks = {} 

        # {qid : "X AND Y", ...}
        self.qid_to_selection = {} 

        # {(pattern_id, sel_column) -> is processed}
        self.is_processed = {}

    def insert(
        self,
        dbname,
        qid,
        join_conds, 
        column_to_selections,
        this_query_tab_to_alias,
    ): 
        if self.group_by == "join pattern":
            jp = JoinPattern(
                join_conds=copy.deepcopy(join_conds),
                dbname=dbname
            )
            if jp not in self.jp_to_id:
                self.jp_to_id[jp] = self.n_groups
                self.id_to_cols_with_selection[self.n_groups] = {}
                self.n_groups += 1
            cur_id = self.jp_to_id[jp]
        elif self.group_by == "query pattern":
            jp = JoinPattern(
                join_conds=copy.deepcopy(join_conds),
                dbname=dbname
            )
            if jp.n_tables == 1:
                # we intend to handle all single-table queries in one group
                # so we keep the list of selected columns empty here
                qp = QueryPattern(jp, list())
            else:
                qp = QueryPattern(jp, list(column_to_selections.keys()))
            if qp not in self.qp_to_id:
                self.qp_to_id[qp] = self.n_groups
                self.id_to_cols_with_selection[self.n_groups] = {}
                self.n_groups += 1
            cur_id = self.qp_to_id[qp]
        else:
            raise NotImplementedError
        
        if cur_id not in self.id_to_qid:
            self.id_to_qid[cur_id] = []
        self.id_to_qid[cur_id].append(qid)

        if jp.n_tables == 1:
            self.id_to_is_no_join[cur_id] = True
            if len(this_query_tab_to_alias) != 1:
                raise ValueError(f"Query {qid} has parsing error")
            alias = list(this_query_tab_to_alias.values())[0]
            if cur_id not in self.id_to_alias_to_qid:
                self.id_to_alias_to_qid[cur_id] = {}
            if alias not in self.id_to_alias_to_qid[cur_id]:
                self.id_to_alias_to_qid[cur_id][alias] = []
            self.id_to_alias_to_qid[cur_id][alias].append(qid)
        else:
            self.id_to_is_no_join[cur_id] = False

        for col, selections in column_to_selections.items():
            if col not in self.id_to_cols_with_selection[cur_id]:
                self.id_to_cols_with_selection[cur_id][col] = []
            self.id_to_cols_with_selection[cur_id][col].append(
                [qid, copy.deepcopy(selections)]
            )

        all_conditions = []
        for column, selections in column_to_selections.items():
            all_conditions.append(
                " AND ".join([
                    f"{column} {selections[i]} {selections[i + 1]}" 
                    for i in range(0, len(selections), 2)
                ])
            )
        self.qid_to_selection[qid] = " AND ".join(all_conditions)

    def get_column_metadata(self, cursor, alias_to_tab):
        self.col_to_type = {}
        self.col_to_min = {}
        self.col_to_max = {}
        for id in self.id_to_cols_with_selection:
            for col in self.id_to_cols_with_selection[id]:
                if col not in self.col_to_type:
                    cursor.execute(
                        "SELECT data_type FROM information_schema.columns"
                        " WHERE table_name = %s AND column_name = %s", 
                        (alias_to_tab[col.split(".")[0]], col.split(".")[1])
                    )
                    self.col_to_type[col] = cursor.fetchone()[0]
                    if self.col_to_type[col] not in [
                        "integer", "smallint", "timestamp without time zone",
                        "numeric"
                    ]:
                        raise ValueError(
                            f"Column {col} datatype {self.col_to_type[col]}"
                             " not supported"
                        )
                    cursor.execute(
                        "SELECT "
                        f" MIN({col.split('.')[1]}), MAX({col.split('.')[1]})"
                        f" FROM {alias_to_tab[col.split('.')[0]]};"
                    )
                    self.col_to_min[col], self.col_to_max[col] \
                        = cursor.fetchone()
                    if self.col_to_type[col] == "numeric":
                        self.col_to_min[col] = float(self.col_to_min[col])
                        self.col_to_max[col] = float(self.col_to_max[col])

    def process_a_selected_col(self, pattern_id, col, batch_size):
        if (pattern_id, col) in self.is_processed:
            return
        self.is_processed[(pattern_id, col)] = True
        
        col_datatype = self.col_to_type[col]
        for ii in range(len(self.id_to_cols_with_selection[pattern_id][col])):
            sels = self.id_to_cols_with_selection[pattern_id][col][ii][1]
            if len(sels) == 2:
                if sels[0] == ">":
                    self.id_to_cols_with_selection[pattern_id][col][ii][1] = [
                        ">=", 
                        nxt_val(parse(sels[1], col_datatype), col_datatype), 
                        "<", 
                        nxt_val(self.col_to_max[col], col_datatype)
                    ]
                elif sels[0] == ">=":
                    self.id_to_cols_with_selection[pattern_id][col][ii][1] = [
                        sels[0], 
                        parse(sels[1], col_datatype), 
                        "<", 
                        nxt_val(self.col_to_max[col], col_datatype)
                    ]
                elif sels[0] == "<":
                    self.id_to_cols_with_selection[pattern_id][col][ii][1] = [
                        ">=", 
                        self.col_to_min[col], 
                        sels[0], 
                        parse(sels[1], col_datatype)
                    ]
                elif sels[0] == "<=":
                    self.id_to_cols_with_selection[pattern_id][col][ii][1] = [
                        ">=", 
                        self.col_to_min[col], 
                        "<", 
                        nxt_val(parse(sels[1], col_datatype), col_datatype)
                    ]
                elif sels[0] == "=":
                    self.id_to_cols_with_selection[pattern_id][col][ii][1] = [
                        ">=", 
                        parse(sels[1], col_datatype), 
                        "<", 
                        nxt_val(parse(sels[1], col_datatype), col_datatype)
                    ]
            elif len(sels) == 4:
                if sels[0] == ">":
                    (
                        self.id_to_cols_with_selection[pattern_id][col][ii][1][0], 
                        self.id_to_cols_with_selection[pattern_id][col][ii][1][1]
                    ) = ">=", nxt_val(parse(sels[1], col_datatype), col_datatype)
                elif sels[0] == ">=":
                    self.id_to_cols_with_selection[pattern_id][col][ii][1][1] = \
                        parse(sels[1], col_datatype)
                else:
                    raise ValueError(f"Selection {sels} on column {col} not supported")
                if sels[2] == "<=":
                    (
                        self.id_to_cols_with_selection[pattern_id][col][ii][1][2], 
                        self.id_to_cols_with_selection[pattern_id][col][ii][1][3]
                    ) = "<", nxt_val(parse(sels[3], col_datatype), col_datatype)
                elif sels[2] == "<":
                    self.id_to_cols_with_selection[pattern_id][col][ii][1][3] = \
                        parse(sels[3], col_datatype)
                else:
                    raise ValueError(f"Selection {sels} on column {col} not supported")
            else:
                raise ValueError(f"Selection {sels} on column {col} not supported")
            self.id_to_cols_with_selection[pattern_id][col][ii][1][1] = max(
                self.id_to_cols_with_selection[pattern_id][col][ii][1][1], 
                self.col_to_min[col]
            )
            self.id_to_cols_with_selection[pattern_id][col][ii][1][3] = min(
                self.id_to_cols_with_selection[pattern_id][col][ii][1][3], 
                nxt_val(self.col_to_max[col], col_datatype)
            )
        
        list_of_two_sels = [
            (
                self.id_to_cols_with_selection[pattern_id][col][ii][1][1], 
                self.id_to_cols_with_selection[pattern_id][col][ii][1][3]
            ) 
            for ii in range(
                len(self.id_to_cols_with_selection[pattern_id][col])
            )
        ]

        flag_for_queries_wo_sel_on_this_col = False
        if self.id_to_is_no_join[pattern_id]:
            if len(self.id_to_cols_with_selection[pattern_id][col]) \
                < len(self.id_to_alias_to_qid[pattern_id][col.split(".")[0]]):
                flag_for_queries_wo_sel_on_this_col=True
        else:
            if len(self.id_to_cols_with_selection[pattern_id][col]) \
                < len(self.id_to_qid[pattern_id]):
                flag_for_queries_wo_sel_on_this_col=True

        # for values (maybe NULL) that do not belong to any bucket, we put them into bucket0

        bucketization = Bucketization(
            list_of_two_sels=list_of_two_sels,
            flag_for_queries_wo_sel_on_this_col=flag_for_queries_wo_sel_on_this_col,
            batch_size=batch_size,
        )

        if pattern_id not in self.id_to_cols_to_bucketization:
            self.id_to_cols_to_bucketization[pattern_id] = {}
        self.id_to_cols_to_bucketization[pattern_id][col] = bucketization

        if pattern_id not in self.id_to_cols_to_qid_and_chunks:
            self.id_to_cols_to_qid_and_chunks[pattern_id] = {}
        if col not in self.id_to_cols_to_qid_and_chunks[pattern_id]:
            self.id_to_cols_to_qid_and_chunks[pattern_id][col] = {}
        for qid, sels in self.id_to_cols_with_selection[pattern_id][col]:
            if sels[1] >= sels[3]:
                # this (qid, selection) pair has an empty answer
                # so we by default set the chunk range as an invalid one
                self.id_to_cols_to_qid_and_chunks[pattern_id][col][qid] = (1, 0)
            else:
                x, y = bucketization.map_a_range(lb = sels[1], ub = sels[3])
                self.id_to_cols_to_qid_and_chunks[pattern_id][col][qid] = (x + 1, y + 1) # 1-based buckets
        
        # add a full domain selection to those queries without selection on this column
        if self.id_to_is_no_join[pattern_id]:
            # it is single-table JoinPattern
            cur_alias = col.split(".")[0]
            for qid in self.id_to_alias_to_qid[pattern_id][cur_alias]:
                if qid not in self.id_to_cols_to_qid_and_chunks[pattern_id][col]:
                    self.id_to_cols_to_qid_and_chunks[pattern_id][col][qid] = (
                        0, # we also include the 0-bucket to count the values that are not in any buckets
                        bucketization.size()
                    )
        else:
            for qid in self.id_to_qid[pattern_id]:
                if qid not in self.id_to_cols_to_qid_and_chunks[pattern_id][col]:
                    self.id_to_cols_to_qid_and_chunks[pattern_id][col][qid] = (
                        0, # we also include the 0-bucket to count the values that are not in any buckets
                        bucketization.size()
                    )

    def iterate_by_group(self):
        if self.group_by == "join pattern":
            for item in self.jp_to_id.items():
                yield item
        elif self.group_by == "query pattern":
            for item in self.qp_to_id.items():
                yield item
        else:
            raise NotImplementedError

    # Used by Optimizer.collect_stats() only
    # for this pattern, each qid, for each selection column, has its 
    #     chunk range, i.e, chunk[x] ... chunk[y]. We compute the ratio
    #     of (y - x + 1) / total_chunks of this column. We take the product
    #    of these ratios among all selection columns as the coverage ratio.
    # Return: either
    # (1) if jp is a single-table jp, 
    #        {alias -> a list of coverage ratio, one for each qid}
    # (2) else, just a list of coverage ratio, one for each qid
    def get_query_coverage_of_a_pattern(self, pattern_id):
        sel_col_to_total_chunks = {
            sel_col: self.id_to_cols_to_bucketization[
                pattern_id
            ][sel_col].size()
            for sel_col in self.id_to_cols_to_qid_and_chunks[
                pattern_id
            ]
        }
        if self.id_to_is_no_join[pattern_id]:
            alias_to_sel_col = defaultdict(list)
            for sel_col in self.id_to_cols_with_selection[
                pattern_id
            ]:
                alias = sel_col.split(".")[0]
                if alias not in alias_to_sel_col:
                    alias_to_sel_col[alias] = []
                alias_to_sel_col[alias].append(sel_col)

            alias_to_coverage_ratios = {}
            for alias in self.id_to_alias_to_qid[pattern_id]:
                alias_to_coverage_ratios[alias] = []
                for qid in self.id_to_alias_to_qid[pattern_id][alias]:
                    ratio = 1.
                    for col in alias_to_sel_col[alias]:
                        x, y = self.id_to_cols_to_qid_and_chunks[
                            pattern_id
                        ][col][qid]
                        ratio *= (y - x + 1) \
                            / sel_col_to_total_chunks[col]
                    alias_to_coverage_ratios[alias].append(ratio)
            return alias_to_coverage_ratios
        else:
            coverage_ratios = []
            for qid in self.id_to_qid[pattern_id]:
                ratio = 1.
                for col, total_chunks in sel_col_to_total_chunks.items():
                    x, y = self.id_to_cols_to_qid_and_chunks[
                        pattern_id
                    ][col][qid]
                    ratio *= (y - x + 1) / total_chunks
                coverage_ratios.append(ratio)
            return coverage_ratios
        
    # Used by Optimizer.collect_stats() only
    # for this pattern, for each selection condition (i.e., a dimension),
    #     we have a bunch of queries, each covering a chunk range. We 
    #    compute the "slack size" (i.e., not covered by any query) for each 
    #    of this dimension.
    # Slack size is defined as the number of chunks not covered by any 
    #     query divided by the total number of chunks for this dimension.
    # Return: either
    # (1) if jp is a single-table jp, 
    #        {alias -> a list of slack size, one for each dimension}
    # (2) else, just a list of slack size, one for each dimension
    def get_slack_size_of_a_pattern(self, pattern_id):
        sel_col_to_total_chunks = {
            sel_col: self.id_to_cols_to_bucketization[
                pattern_id
            ][sel_col].size()
            for sel_col in self.id_to_cols_to_qid_and_chunks[
                pattern_id
            ]
        }
        if self.id_to_is_no_join[pattern_id]:
            alias_to_sel_col = defaultdict(list)
            for sel_col in self.id_to_cols_with_selection[
                pattern_id
            ]:
                alias = sel_col.split(".")[0]
                if alias not in alias_to_sel_col:
                    alias_to_sel_col[alias] = []
                alias_to_sel_col[alias].append(sel_col)

            alias_to_slack_sizes = {}
            for alias in self.id_to_alias_to_qid[pattern_id]:
                alias_to_slack_sizes[alias] = []
                for col in alias_to_sel_col[alias]:
                    alias_to_slack_sizes[alias].append(0)
                    bounds = []
                    for qid in self.id_to_alias_to_qid[pattern_id][alias]:
                        x, y = self.id_to_cols_to_qid_and_chunks[
                            pattern_id
                        ][col][qid]
                        bounds.append((x, 1))
                        bounds.append((y + 1, - 1))
                    j = -1
                    _cnt = 0
                    for i in range(sel_col_to_total_chunks[col]):
                        while j + 1 < len(bounds) and bounds[j][0] == i:
                            j += 1
                            _cnt += bounds[j][1]
                        if _cnt == 0:
                            alias_to_slack_sizes[alias][-1] += 1
                    alias_to_slack_sizes[alias][-1] /= \
                        sel_col_to_total_chunks[col]
            return alias_to_slack_sizes
        else:
            slack_sizes = []
            for col, total_chunks in sel_col_to_total_chunks.items():
                slack_sizes.append(0)
                bounds = []
                for qid in self.id_to_qid[pattern_id]:
                    x, y = self.id_to_cols_to_qid_and_chunks[
                        pattern_id
                    ][col][qid]
                    bounds.append((x, 1))
                    bounds.append((y + 1, - 1))
                j = -1
                _cnt = 0
                for i in range(total_chunks):
                    while j + 1 < len(bounds) and bounds[j][0] == i:
                        j += 1
                        _cnt += bounds[j][1]
                    if _cnt == 0:
                        slack_sizes[-1] += 1
                slack_sizes[-1] /= total_chunks
            return slack_sizes

    def print_summary(self):
        print("---------- Workload Summary ----------")
        print(
            f"# of groups (grouped by {self.group_by}): "
            f"{self.n_groups}"
        )
        for pattern, pattern_id in self.iterate_by_group():
            if self.group_by == "query pattern":
                jp = pattern.join_pattern
            print("===============")
            print(f"\tPattern [{pattern_id}]: " + str(pattern))
            print("\t# of tables in the join pattern", jp.n_tables)
            print(
                "\t# of columns with selections", 
                len(self.id_to_cols_with_selection[pattern_id])
            )
            print(
                "\tthese columns are", 
                list(self.id_to_cols_with_selection[pattern_id].keys())
            )
        print("--------------------------------------")