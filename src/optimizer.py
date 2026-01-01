# Tiny optimizer: estimate cost through a ML model

from enum import Enum
import math
import pulp
import re
import pickle
import numpy as np
import pandas as pd
import psycopg2

from schema_related import JoinPattern

class Algorithm(Enum):
    SeqProc = 1
    PostFilter = 2
    TreeBatchCext = 3
    
    def __str__(self):
        return self.name

class Selector:
    def __init__(
        self, 
        model, 
        threshold1, 
        threshold2
    ):
        self.model = model
        self.threshold1 = threshold1
        self.threshold2 = threshold2
    
    def predict(self, X):
        p1 = self.model.predict_proba(X)[:, 1]
        p2 = self.model.predict_proba(X)[:, 2]
        N = X.shape[0]
        res = np.zeros(N, dtype=int)
        
        seqproc_mask = (p1 >= self.threshold1) \
            & (p1 >= p2)
        postfilter_mask = (p2 >= self.threshold2) \
            & (p2 >= p1)
        
        res[seqproc_mask] = 1
        res[postfilter_mask] = 2
        return res

# ==== featurization ====
def _sum(l):
    if len(l) == 0:
        return 0.
    return 1. * sum(l)

def _max(l):
    if len(l) == 0:
        return 0.
    return 1. * max(l)

def _min(l):
    if len(l) == 0:
        return 0.
    return 1. * min(l)

def _mean(l):
    if len(l) == 0:
        return 0.
    return np.mean(l)

def extract_one(
    instance
):
    return [
        instance["n_tables"],
        instance["n_queries"],
        _sum(instance["base_table_sizes"]),
        _max(instance["base_table_sizes"]),
        _mean(instance["base_table_sizes"]),
        np.log1p(_sum(instance["intermediate_join_sizes"])), 
        np.log1p(_max(instance["intermediate_join_sizes"])),
        np.log1p(_mean(instance["intermediate_join_sizes"])),
        _sum(instance["costs_of_scanning"]),
        _max(instance["costs_of_scanning"]),
        _mean(instance["costs_of_scanning"]),
        _sum(instance["numbers_of_selection_columns"]),
        _max(instance["numbers_of_selection_columns"]),
        _mean(instance["numbers_of_selection_columns"]),
        np.prod(instance["numbers_of_buckets"]),
        instance["sum_query_coverage_ratio"],
        instance["max_query_coverage_ratio"],
        instance["mean_query_coverage_ratio"],    
    ]

def get_features():
    return [
        "n_tables", 
        "n_queries",
        "sum_base_table_size",
        "max_base_table_size",
        "mean_base_table_size",
        "log_sum_intermediate_join_size", 
        "log_max_intermediate_join_size", 
        "log_mean_intermediate_join_size", 
        "sum_cost_of_scanning",
        "max_cost_of_scanning",
        "mean_cost_of_scanning",
        "sum_number_of_selection_column",
        "max_number_of_selection_column",
        "mean_number_of_selection_column",
        "prod_number_of_bucket",
        "sum_query_coverage_ratio",
        "max_query_coverage_ratio",
        "mean_query_coverage_ratio",
    ]

class Optimizer:
    def __init__(
        self, 
        cursor, 
        alias_to_tab, 
        load_accurate_unfiltered_join_size=False # by default, we do not 
        #    pay the cost and load accurate unfiltered join size. This is
        #    an optimizer option.
    ):
        self.cursor = cursor
        self.alias_to_tab = alias_to_tab
        self.load_accurate_unfiltered_join_size = \
            load_accurate_unfiltered_join_size

        self.db_to_jp_to_unfiltered_join_size = {}
        if self.load_accurate_unfiltered_join_size:
            for db in ["imdb", "stats", "dsb-sf2"]:
                with open(
                    f"../results/tree_batch_cext_"
                    f"{db}_all_jp_queries.print", 
                    "r"
                ) as fjp, open(
                    f"../results/tree_batch_cext_"
                    f"{db}_all_jp_queries.ans",
                    "r"
                ) as fcount:
                    list_of_jps = []
                    for line in fjp.readlines():
                        if line.startswith("===== Pattern"):
                            jp_in_str = line.strip().split(": ")[1]
                            list_of_jps.append(
                                JoinPattern(
                                    list(eval(jp_in_str)),
                                    db
                                )
                            )
                    list_of_counts = []
                    line_id = -1
                    for line in fcount.readlines():
                        line_id += 1
                        if line_id >= 2:
                            list_of_counts.append(
                                int(line.strip().split(",")[0])
                            )
                    assert len(list_of_jps) == len(list_of_counts)
                    self.db_to_jp_to_unfiltered_join_size[db] = {}
                    for i in range(len(list_of_jps)):
                        self.db_to_jp_to_unfiltered_join_size[db][
                            list_of_jps[i]
                        ] = list_of_counts[i]

        self.alias_to_base_table_size = {}

        # Column stats collected by pg
        # We assume it is sufficiently close to the truth at 
        # the point when we collect them. See defintiions 
        # in https://www.postgresql.org/docs/current/view-pg-stats.html
        self.col_to_null_frac = {}
        self.col_to_n_distinct = {}
        self.col_to_mcv_to_mcf = {}
        # the upperbound frequency of rows with non-null but infrequent values
        self.col_to_freq_of_infreq = {} 
        # number of rows with non-null but infrequent values that are not in mcv
        self.col_to_n_infreq_rows = {} 
    
        self.file_to_model = {} # path_to_model -> model

    def _get_base_table_size(
        self,
        alias
    ):
        if alias not in self.alias_to_base_table_size:
            self.cursor.execute(
                f"SELECT reltuples::INT FROM pg_class WHERE "
                f"relname = '{self.alias_to_tab[alias]}';"
            )
            self.alias_to_base_table_size[alias] = self.cursor.fetchone()[0]
        return self.alias_to_base_table_size[alias]
    
    def _expression_to_a_join_cond(self, expr):
        if ">=" in expr:
            op = ">="
        elif "<=" in expr:
            op = "<="
        elif ">" in expr:
            op = ">"
        elif "<" in expr:
            op = "<"
        elif "=" in expr:
            op = "="
        else: 
            raise ValueError(f"Join expression not supported: {expr}")
        left, right = (
            expr.split(op)[0].strip().lower(), 
            expr.split(op)[1].strip().lower()
        )
        n_dot_in_left = left.count(".")
        n_dot_in_right = right.count(".")
        if n_dot_in_left > 1 or n_dot_in_right > 1:
            raise ValueError(f"Join expression not supported: {expr}")
        if n_dot_in_left != 1:
            raise ValueError(f"Join expression not supported: {expr}")
        if n_dot_in_left + n_dot_in_right == 2 and op != "=":
            # it is a join but not equality join
            raise ValueError(f"Join type not supported: {expr}")
        if n_dot_in_left < n_dot_in_right:
            left, right = right, left
            n_dot_in_left, n_dot_in_right = n_dot_in_right, n_dot_in_left
        if n_dot_in_right == 1 and right.split(".")[0] < left.split(".")[0]:
            left, right = right, left
            n_dot_in_left, n_dot_in_right = n_dot_in_right, n_dot_in_left
        return (left, op, right)
    
    def _decode(self, cur, to_fetch, dbname):
        # Get the JoinPattern for each intermediate join stage
        #     for example, if it is a 5-table join, there should 
        #     be a 2-table JP, a 3-table JP, ..., a 5-table JP
        node_type = cur['Node Type']
        is_leaf = 'Plans' not in cur
        if is_leaf:
            assert node_type in [
                'Index Scan', 
                'Seq Scan', 
                'Bitmap Scan', 
                'Index Only Scan'
            ]
            if to_fetch and 'Index Cond' in cur:
                _cond = [self._expression_to_a_join_cond(
                    cur['Alias'] + "." + cur['Index Cond'].lstrip("(").rstrip(")")
                )]
                return tuple(_cond), [JoinPattern(_cond, dbname)]
            return (), []
        else:
            if node_type in [
                'Aggregate', 'Gather', 'Sort', 
                'Materialize', 'Sort', 'Hash', 
                'Gather Merge', 'Memoize'
            ]:
                assert len(cur['Plans']) == 1
                return self._decode(cur['Plans'][0], to_fetch, dbname)
            elif node_type in ['Merge Join', 'Hash Join', 'Nested Loop']:
                join_conds_at_cur = []
                all_jps_in_this_subtree = []
                assert len(cur['Plans']) == 2
                to_fetch = True
                for key in ["Hash Cond", "Merge Cond", "Join Filter"]:
                    if key in cur:
                        to_fetch = False
                        join_conds_at_cur.append(
                            self._expression_to_a_join_cond(
                                cur[key].lstrip("(").rstrip(")")
                            )
                        )
                for i in range(2):
                    (
                        join_conds_at_child, 
                        all_join_conds_in_child_subtree
                    ) = self._decode(cur['Plans'][i], to_fetch, dbname)
                    all_jps_in_this_subtree.extend(
                        all_join_conds_in_child_subtree
                    )
                    for join_cond in join_conds_at_child:
                        join_conds_at_cur.append(join_cond)
                if not to_fetch:
                    all_jps_in_this_subtree.append(
                        JoinPattern(join_conds_at_cur, dbname)
                    )
                return tuple(join_conds_at_cur), all_jps_in_this_subtree
            else:
                raise NotImplementedError(node_type)
    
    def _get_mcf(self, join_col, mcv):
        if self.col_to_mcv_to_mcf[join_col] is None:
            return 1
        if mcv not in self.col_to_mcv_to_mcf[join_col]:
            return self.col_to_freq_of_infreq[join_col]
        return self.col_to_mcv_to_mcf[join_col][mcv]
    
    def _get_n_distinct(self, join_col):
        _n_distinct = self.col_to_n_distinct[join_col]
        if _n_distinct >= 0:
            return _n_distinct
        else:
            alias = join_col.split(".")[0]
            return -_n_distinct * self._get_base_table_size(alias) \
                * (1 - self.col_to_null_frac[join_col])
    
    def _compute_agm(self, alias_sizes, cc_id_to_alias_i, alias_has_pk):
        n_alias = len(alias_sizes)
        lp = pulp.LpProblem('AGM_bound', pulp.LpMinimize)
        x = {} # one variable per alias/tab
        for i in range(n_alias):
            if alias_has_pk[i]:
                x[str(i)] = pulp.LpVariable(str(i), lowBound=0)
            else:
                x[str(i)] = pulp.LpVariable(str(i), lowBound=1)
        lp += pulp.lpSum([
            math.log(alias_sizes[i]) * x[str(i)] 
            for i in range(n_alias)
        ])
        for j in range(len(cc_id_to_alias_i)): # one constraint per cc
            lp += pulp.lpSum(
                x[str(alias_i)] for alias_i in cc_id_to_alias_i[j]
            ) >= 1, f"cover_cc_{j}"
        lp.solve(pulp.PULP_CBC_CMD(msg=False))

        agm_bound = round(math.prod(
            alias_sizes[i] ** x[str(i)].value() 
            for i in range(n_alias)
        ))
        return agm_bound
    
    # def _compute_pairwise(self, residual_sizes, residual_divisors):
    #     assert len(residual_sizes) == len(residual_divisors) + 1
    #     _size = 1.
    #     for residual_divisor in residual_divisors:
    #         if residual_divisor != 0:
    #             _size /= residual_divisor
    #     for residual_size in residual_sizes:
    #         _size *= residual_size
    #     return round(_size)   

    def _estimate_unfiltered_join_size_of_jp(self, jp):
        if self.load_accurate_unfiltered_join_size:
            # when we are willing to pay the cost to get the accurate
            #      unfiltered join size
            if jp.dbname in self.db_to_jp_to_unfiltered_join_size \
                and jp in self.db_to_jp_to_unfiltered_join_size[jp.dbname]:
                return self.db_to_jp_to_unfiltered_join_size[jp.dbname][jp]

        if jp.dbname not in self.db_to_jp_to_unfiltered_join_size \
            or jp not in self.db_to_jp_to_unfiltered_join_size[jp.dbname]:
            
            if jp.dbname not in self.db_to_jp_to_unfiltered_join_size:
                self.db_to_jp_to_unfiltered_join_size[jp.dbname] = {}
            self.db_to_jp_to_unfiltered_join_size[jp.dbname][jp] = 0 

            root_alias = jp.root_alias
            alias_enumeration_order = jp._tree_preorder()

            # get the col stats (if not computed and cached already)
            for alias in alias_enumeration_order:
                for col in jp.alias_to_join_col[alias]:
                    if col not in self.col_to_null_frac:
                        sql = "SELECT "
                        sql += "null_frac, n_distinct, most_common_vals, most_common_freqs"
                        sql += f" FROM pg_stats where tablename = '{self.alias_to_tab[alias]}'"
                        sql += f" and attname = '{col.split('.')[1]}';"
                        self.cursor.execute(sql)
                        (
                            self.col_to_null_frac[col], 
                            self.col_to_n_distinct[col], 
                            _col_to_mcv, 
                            _col_to_mcf
                        ) = self.cursor.fetchone()

                        # if _col_to_mcv is not None:
                        #     assert isinstance(_col_to_mcv, str) \
                        #         and _col_to_mcv.startswith("{") \
                        #         and _col_to_mcv.endswith("}")
                        #     _col_to_mcv = eval(
                        #         _col_to_mcv.replace("{", "[").replace("}", "]")
                        #     )
                        #     for i in range(len(_col_to_mcf)):
                        #         _col_to_mcf[i] = round(
                        #             self._get_base_table_size(alias) \
                        #                 * (1. - self.col_to_null_frac[col]) \
                        #                     * _col_to_mcf[i]
                        #         )
                        #     assert isinstance(_col_to_mcv, list)
                        #     if _col_to_mcf is None:
                        #         assert self.col_to_n_distinct[col] == -1
                        #         self.col_to_freq_of_infreq[col] = 1
                        #     else:
                        #         self.col_to_freq_of_infreq[col] = _col_to_mcf[-1]
                        #     self.col_to_mcv_to_mcf[col] = {}
                        #     for i in range(len(_col_to_mcv)):
                        #         self.col_to_mcv_to_mcf[col][_col_to_mcv[i]] = _col_to_mcf[i]
                        #     self.col_to_n_infreq_rows[col] = round(
                        #         self._get_base_table_size(alias) \
                        #             * (1. - self.col_to_null_frac[col]) \
                        #             - sum(_col_to_mcf)
                        #     )
                        # else:
                        #     # the case when it is a key column 
                        #     assert _col_to_mcf is None
                        #     self.col_to_mcv_to_mcf[col] = None
                        #     self.col_to_n_infreq_rows[col] = None
            
            n_cc = 0
            _join_col_to_cc_id = {} # join col -> id
            # _cc_id_to_mcv = {} # id -> a set of mcvs
                
            # jp.alias_to_child_join_col
            def find_cc(cur, join_col_from_parent): 
                nonlocal n_cc
                if join_col_from_parent is not None:
                    my_join_col = jp.alias_to_join_col[cur][0]
                    parent_cc_id = _join_col_to_cc_id[join_col_from_parent]
                    _join_col_to_cc_id[my_join_col] = parent_cc_id
                    # if self.col_to_mcv_to_mcf[my_join_col] is not None:
                    #     for elem in self.col_to_mcv_to_mcf[my_join_col]:
                    #         _cc_id_to_mcv[parent_cc_id].add(elem)

                for child in jp.alias_to_child_join_col[cur]:
                    join_col_to_child = jp.alias_to_child_join_col[cur][child]
                    if join_col_to_child not in _join_col_to_cc_id:
                        _join_col_to_cc_id[join_col_to_child] = n_cc
                        # if self.col_to_mcv_to_mcf[join_col_to_child] is None:
                        #     _cc_id_to_mcv[n_cc] = set()
                        # else:
                        #     _cc_id_to_mcv[n_cc] = set(
                        #         list(self.col_to_mcv_to_mcf[join_col_to_child].keys())
                        #     )
                        n_cc += 1
                    find_cc(child, join_col_to_child)
            
            find_cc(root_alias, None)
            
            # # Enumerate the skewed parts (i.e., mcvs)
            # # we assume the number of tuples with one mcv in each join 
            # #     attribute is min(mcf for those mcvs)
            # skewed_part_join_size = 0

            # def enumerate_join_combos(
            #     which_alias, 
            #     which_join_col, 
            #     prod, 
            #     this_alias_min_mcf, 
            #     cc_id_to_selected_mcv
            # ):
            #     nonlocal alias_enumeration_order
            #     nonlocal skewed_part_join_size
            #     nonlocal jp
            #     nonlocal _join_col_to_cc_id

            #     cur = alias_enumeration_order[which_alias]
            #     if which_join_col == len(jp.alias_to_join_col[cur]):
            #         which_join_col = 0
            #         prod *= this_alias_min_mcf
            #         which_alias += 1
            #         if which_alias == len(alias_enumeration_order):
            #             skewed_part_join_size += prod
            #             return
                
            #     join_col = jp.alias_to_join_col[cur][which_join_col]
            #     join_col_cc_id = _join_col_to_cc_id[join_col]
            #     if join_col_cc_id not in cc_id_to_selected_mcv:
            #         for _mcv in _cc_id_to_mcv[join_col_cc_id]:
            #             cc_id_to_selected_mcv[join_col_cc_id] = _mcv
            #             _mcf = self._get_mcf(join_col, _mcv)
            #             if which_join_col == 0:
            #                 new_this_alias_min_mcf = _mcf
            #             else:
            #                 new_this_alias_min_mcf = min(this_alias_min_mcf, _mcf)
            #             enumerate_join_combos(
            #                 which_alias,
            #                 which_join_col + 1,
            #                 prod,
            #                 new_this_alias_min_mcf,
            #                 cc_id_to_selected_mcv
            #             )
            #             del cc_id_to_selected_mcv[join_col_cc_id]
            #     else:
            #         _mcv = cc_id_to_selected_mcv[join_col_cc_id]
            #         _mcf = self._get_mcf(join_col, _mcv)
            #         if which_join_col == 0:
            #             new_this_alias_min_mcf = _mcf
            #         else:
            #             new_this_alias_min_mcf = min(this_alias_min_mcf, _mcf)
            #         enumerate_join_combos(
            #             which_alias,
            #             which_join_col + 1,
            #             prod,
            #             new_this_alias_min_mcf,
            #             cc_id_to_selected_mcv
            #         )
            
            # enumerate_join_combos(
            #     which_alias=0,
            #     which_join_col=0,
            #     prod=1,
            #     this_alias_min_mcf=None,
            #     cc_id_to_selected_mcv={}
            # )

            # we use AGM bound for the original tables
            alias_sizes = []
            alias_has_pk = []
            cc_id_to_alias_i = [set() for _ in range(n_cc)]
            for alias_i, alias in enumerate(alias_enumeration_order):
                alias_sizes.append(self.alias_to_base_table_size[alias])
                this_alias_has_pk = False
                for join_col in jp.alias_to_join_col[alias]:
                    cc_id = _join_col_to_cc_id[join_col]
                    cc_id_to_alias_i[cc_id].add(alias_i)
                    if (self.col_to_n_distinct[join_col] - (-1.0)) < 1e-6:
                        this_alias_has_pk = True
                alias_has_pk.append(this_alias_has_pk)
            
            original_agm_bound = self._compute_agm(
                alias_sizes, 
                cc_id_to_alias_i,
                alias_has_pk
            )
            self.db_to_jp_to_unfiltered_join_size[jp.dbname][jp] = original_agm_bound
            return original_agm_bound
            # if original_agm_bound <= skewed_part_join_size:
            #     self.db_to_jp_to_unfiltered_join_size[jp.dbname][jp] = original_agm_bound
            #     return original_agm_bound

            # let's check if original pairwise bound is tighter than skewed part bound
            alias_sizes = []
            divisors = []
            for alias in alias_enumeration_order:
                _max_nul_frac = 0.
                for join_col in jp.alias_to_join_col[alias]:
                    # as long as one join_col is NULL, that tuple won't contribute to 
                    #     the final join
                    _max_nul_frac = max(_max_nul_frac, self.col_to_null_frac[join_col])
                alias_sizes.append(self._get_base_table_size(alias) * (1 - _max_nul_frac))
            for left_col, _, right_col in jp.join_pattern:
                divisors.append(
                    min(
                        self._get_n_distinct(left_col),
                        self._get_n_distinct(right_col)
                    )
                )
            original_pairwise_bound = self._compute_pairwise(
                alias_sizes, 
                divisors
            )
            if original_pairwise_bound <= skewed_part_join_size:
                self.db_to_jp_to_unfiltered_join_size[jp.dbname][jp] = original_pairwise_bound
                return original_pairwise_bound

            # Otherwise, estimate the residual parts 
            # (tuples that have >= 1 join column owning an infrequent value)
            # in each table, the residual table size is 
            # base_table_size - sum(sum of mcfs of a join col J) for each J
            # or just base_table_size
            residual_sizes = []
            for i, alias in enumerate(alias_enumeration_order):
                _union_size = 0
                for join_col in jp.alias_to_join_col[alias]:
                    _sum_mcf = sum(
                        self._get_mcf(join_col, _mcv)
                        for _mcv in _cc_id_to_mcv[_join_col_to_cc_id[join_col]]
                    )
                    _residual_size_by_this_join_col = \
                        self._get_base_table_size(alias) * (1 - self.col_to_null_frac[join_col]) \
                            - _sum_mcf
                    _union_size += _residual_size_by_this_join_col
                residual_sizes.append(
                    min(
                        _union_size, 
                        alias_sizes[i]
                    )
                )
            
            # To compute the pairwise bound, we take the minimum of NDV for each 
            #     edge (i.e. join equality)
            residual_divisors = [] 
            for left_col, _, right_col in jp.join_pattern:
                residual_divisors.append(
                    min(
                        # remove MCVs that were considered
                        self._get_n_distinct(left_col) \
                            - len(_cc_id_to_mcv[_join_col_to_cc_id[left_col]]), 
                        # remove MCVs that were considered
                        self._get_n_distinct(right_col) \
                            - len(_cc_id_to_mcv[_join_col_to_cc_id[right_col]]) 
                    )
                )

            residual_part_join_size = self._compute_pairwise(
                residual_sizes,
                residual_divisors
            )
            # Deprecated after we decide not to use agm bound
            # min(
                # self._compute_agm(
                #     residual_sizes, 
                #     cc_id_to_alias_i,
                #     alias_has_pk
                # ),
            # )

            # At the end, we compute the min(pairwise bound, the sum of both parts)
            self.db_to_jp_to_unfiltered_join_size[jp.dbname][jp] = min(
                # original_agm_bound,
                original_pairwise_bound,
                skewed_part_join_size + residual_part_join_size
            )

        return self.db_to_jp_to_unfiltered_join_size[jp.dbname][jp]
    
    def _get_cost_of_sql(self, sql):
        self.cursor.execute(f"EXPLAIN {sql};")
        first_line_of_plan = self.cursor.fetchall()[0][0]
        match = re.search(
            r'cost=(\d+\.\d+)\.\.(\d+\.\d+)', 
            first_line_of_plan
        )
        if match:
            # total cost, not the startup cost
            return float(match.group(2)) 
        else:
            assert True == False
    
    def _get_list_of_intermediate_join_sizes(
        self,
        source_jp,
        alias_to_tab
    ):
        unfiltered_sql = "SELECT COUNT(*) FROM " + \
            source_jp.get_from_clause(self.alias_to_tab) + \
                " WHERE " + source_jp.get_join_clause() + ";"
        self.cursor.execute("EXPLAIN (FORMAT JSON)" + unfiltered_sql)
        plan = self.cursor.fetchone()[0][0]["Plan"]
        _, jp_in_each_stage = self._decode(plan, True, source_jp.dbname)
        assert(len(jp_in_each_stage) == source_jp.n_tables - 1)

        res = []
        for jp in jp_in_each_stage:
            jp._build_the_tree(alias_to_tab)
            res.append(self._estimate_unfiltered_join_size_of_jp(jp))
        return res

    def collect_stats(
        self,
        pattern_id,
        source_jp,
        participated_alias_list,
        alias_to_sel_col,
        workload_summary,
        alias_to_tab,
    ):
        training_data = {}
        training_data["n_tables"] = source_jp.n_tables
        if source_jp.n_tables == 1:
            assert len(participated_alias_list) == 1
            training_data["n_queries"] = len(
                workload_summary.id_to_alias_to_qid[
                    pattern_id
                ][participated_alias_list[0]]
            )
        else:
            training_data["n_queries"] = len(
                workload_summary.id_to_qid[pattern_id]
            )

        training_data["base_table_sizes"] = [
            self._get_base_table_size(alias)
            for alias in participated_alias_list
        ]

        if source_jp.n_tables == 1:
            training_data["intermediate_join_sizes"] = []
        else:
            training_data["intermediate_join_sizes"] = \
                self._get_list_of_intermediate_join_sizes(
                    source_jp,
                    alias_to_tab
                )
            
        training_data["costs_of_scanning"] = []
        training_data["numbers_of_selection_columns"] = []
        training_data["numbers_of_buckets"] = []
        for alias in participated_alias_list:
            training_data["numbers_of_selection_columns"].append(
                len(alias_to_sel_col[alias])
            )
            pure_join_cols = []
            if source_jp.n_tables > 1:
                for join_col in source_jp.alias_to_join_col[alias]:
                    # remove the prefix "{alias}."
                    pure_col = join_col.split(".")[1] 
                    pure_join_cols.append(pure_col)
            pure_sel_cols = []
            for sel_col in alias_to_sel_col[alias]:
                # remove the prefix "{alias}."
                pure_col = sel_col.split(".")[1] 
                pure_sel_cols.append(pure_col)
                training_data["numbers_of_buckets"].append(
                    len(
                        workload_summary
                            .id_to_cols_to_bucketization[pattern_id][sel_col]
                            .chunks
                    )
                )
            sql = "SELECT \n\t"
            if len(pure_join_cols) + len(pure_sel_cols) == 0:
                sql += "*"
            else:
                sql += "\n\t, ".join(pure_join_cols + pure_sel_cols)
            sql += f"\n FROM {self.alias_to_tab[alias]} {alias}"
            if len(pure_join_cols) > 0:
                sql += "\n ORDER BY " + ", ".join(pure_join_cols)
            sql += "\n;"
            training_data["costs_of_scanning"].append(
                self._get_cost_of_sql(sql)
            )
        
        if source_jp.n_tables == 1:
            assert len(participated_alias_list) == 1
            list_of_coverage_ratios = workload_summary\
                .get_query_coverage_of_a_pattern(
                    pattern_id
                )[participated_alias_list[0]]
        else:
            list_of_coverage_ratios = workload_summary\
                .get_query_coverage_of_a_pattern(
                    pattern_id
                )
            
        training_data["sum_query_coverage_ratio"] = _sum(
            list_of_coverage_ratios
        )
        training_data["max_query_coverage_ratio"] = _max(
            list_of_coverage_ratios
        )
        training_data["mean_query_coverage_ratio"] = _mean(
            list_of_coverage_ratios
        )

        return training_data
    
    # Depending on source_jp
    # if source_jp.n_tables == 1:
    #     return a {alg -> [alias1, alias2, ...]}
    # otherwise:
    #     return a {alg -> None}
    def select_algorithm(
        self,
        source_jp, 
        pattern_id,
        alias_to_sel_col,
        workload_summary,
        model_name,
        sqls,
        alias_to_tab,
    ):
        pg_limits_on_n_selections = 1664

        # --- preparation    
        if source_jp.n_tables == 1:
            participated_alias_list = list(
                workload_summary.id_to_alias_to_qid[pattern_id].keys()
            )
            # a dict, {alias : [list of selections]}
            each_sql_selection = {
                alias : [
                    workload_summary.qid_to_selection[qid] 
                    for qid in workload_summary\
                        .id_to_alias_to_qid[pattern_id][alias]
                ]
                for alias in workload_summary\
                    .id_to_alias_to_qid[pattern_id] 
            }
        else:
            participated_alias_list = list(
                source_jp.alias_to_join_col.keys()
            )
            # a list of selections
            each_sql_selection = [
                workload_summary.qid_to_selection[qid] 
                for qid in workload_summary.id_to_qid[pattern_id]
            ]
        
        for alias in alias_to_sel_col:
            for sel_col in alias_to_sel_col[alias]:
                workload_summary.process_a_selected_col(
                    pattern_id=pattern_id, 
                    col=sel_col, 
                    batch_size=50000
                )

        # Again, if source_jp.n_tables == 1, 
        #     we query the ML model for M times, each with an alias
        # Otherwise, we query the ML model once, 
        #     with the entire participated_alias_list

        query_against = []
        if source_jp.n_tables == 1:
            for alias in participated_alias_list:
                query_against.append([alias])
        else:
            query_against.append(participated_alias_list)

        choice_to_alias = {}
        for alias_list in query_against:
            choice = None
            # --- collect stats
            instance = self.collect_stats(
                pattern_id=pattern_id,
                source_jp=source_jp,
                participated_alias_list=alias_list,
                alias_to_sel_col=alias_to_sel_col,
                workload_summary=workload_summary,
                alias_to_tab=alias_to_tab,
            )

            # --- load model
            if model_name not in self.file_to_model:
                # if model is not loaded yet, load it
                with open(model_name, "rb") as fmodel: 
                    self.file_to_model[model_name] = pickle.load(
                        fmodel
                    )

            # --- prediction for algorithm selection
            features = get_features()
            data = [extract_one(instance)]
            X = pd.DataFrame(data, columns=features)
            pred = \
                self.file_to_model[model_name].predict(X)[0]

            # if should_go_with_baseline:
            #     # decide which baseline to use, "SeqProc" or "PostFilter"
            #     # by comparing their costs computed by pg

            #     # --- cost for SeqProc
            #     cost_for_seq_proc = 0.
            #     for qid in workload_summary.id_to_qid[pattern_id]:
            #         cost_for_seq_proc += self._get_cost_of_sql(sqls[qid])

            #     # --- cost for PostFilter
            #     if source_jp.n_tables == 1:
            #         cost_for_post_filter = 0.
            #         for alias in each_sql_selection:
            #             selection_chunk = []
            #             for selection in each_sql_selection[alias]:
            #                 if selection == "":
            #                     selection_chunk.append("COUNT(*)")
            #                 else:
            #                     selection_chunk.append(
            #                         f"COUNT(CASE WHEN {selection} "
            #                         "THEN 1 END)"
            #                     )
                        
            #             for k in range(
            #                 0, 
            #                 len(selection_chunk), 
            #                 pg_limits_on_n_selections
            #             ):
            #                 st = k
            #                 en = min(
            #                     len(selection_chunk), 
            #                     k + pg_limits_on_n_selections
            #                 )
            #                 sql_for_post_filter = "SELECT " \
            #                     + ", ".join(selection_chunk[st:en]) \
            #                     + f" FROM {self.alias_to_tab[alias]} {alias};"
            #                 cost_for_post_filter += self._get_cost_of_sql(
            #                     sql_for_post_filter
            #                 )
            #     else:
            #         cost_for_post_filter = 0.
            #         alias_enumerate_order = list(
            #             source_jp.alias_to_join_col.keys()
            #         )
            #         join_chunk = " AND ".join([
            #             f"{tmp[0]} {tmp[1]} {tmp[2]}" 
            #             for tmp in list(source_jp.join_pattern)
            #         ])
            #         selection_chunk = []
            #         for selection in each_sql_selection:
            #             if selection == "":
            #                 selection_chunk.append("COUNT(*)")
            #             else:
            #                 selection_chunk.append(
            #                     f"COUNT(CASE WHEN {selection} "
            #                     "THEN 1 END)"
            #                 )
                    
            #         for k in range(
            #             0, 
            #             len(selection_chunk), 
            #             pg_limits_on_n_selections
            #         ):
            #             st = k
            #             en = min(
            #                 len(selection_chunk), 
            #                 k + pg_limits_on_n_selections
            #             )
            #             where_chunk = ", ".join([
            #                 f"{self.alias_to_tab[alias]} {alias}" 
            #                 for alias in alias_enumerate_order
            #             ])
            #             sql_for_post_filter = "SELECT " \
            #                 + ", ".join(selection_chunk[st:en]) \
            #                 + " FROM " + where_chunk \
            #                     + " WHERE " + join_chunk + ";"
            #             cost_for_post_filter += self._get_cost_of_sql(
            #                 sql_for_post_filter
            #             )
                
            #     if cost_for_seq_proc < cost_for_post_filter:
            #         choice = Algorithm.SeqProc
            #     else:
            #         choice = Algorithm.PostFilter               
            # else:
            #     choice = Algorithm.TreeBatchCext

            if pred == 0:
                choice = Algorithm.TreeBatchCext
            elif pred == 1:
                choice = Algorithm.SeqProc
            else:
                choice = Algorithm.PostFilter

            if source_jp.n_tables == 1:
                if choice not in choice_to_alias:
                    choice_to_alias[choice] = []
                choice_to_alias[choice].append(alias_list[0])
            else:
                choice_to_alias[choice] = None
        return choice_to_alias

    
