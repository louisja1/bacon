import os
import json

class JoinPattern:
    def __init__(
        self, 
        join_conds, # a list of join conditions, i.e., [(left, op, right), ...]
        dbname,
    ):
        self.join_pattern = tuple(sorted(join_conds, key=lambda x: (x[0], x[2])))
        self.dbname = dbname

        if os.path.exists(f"../{self.dbname}_fk_pk_def.json"):
            with open(f"../{self.dbname}_fk_pk_def.json") as fkeydef:
                self.fk_pk_def = json.load(fkeydef)
        else:
            self.fk_pk_def = None

        # set in _build_the_tree(), used only in is_a_star()
        self.alias_to_tab = None 
        # set in is_a_star()
        self._is_a_star_result = None

        self.alias_to_join_col = {} # {alias : [join_col0, ...]}
        for left, _, right in join_conds:
            left_alias = left.split(".")[0]
            if left_alias not in self.alias_to_join_col:
                self.alias_to_join_col[left_alias] = []
            self.alias_to_join_col[left_alias].append(left)
            right_alias = right.split(".")[0]
            if right_alias not in self.alias_to_join_col:
                self.alias_to_join_col[right_alias] = []
            self.alias_to_join_col[right_alias].append(right)
        
        for alias in self.alias_to_join_col:
            self.alias_to_join_col[alias] = list(set(self.alias_to_join_col[alias]))

        if len(join_conds) == 0:
            self.n_tables = 1
        else:
            self.n_tables = len(self.alias_to_join_col.keys())
    def get_from_clause(self, alias_to_tab):
        tabs_involved = []
        for i in range(len(self.join_pattern)):
            tabs_involved.append(
                alias_to_tab[self.join_pattern[i][0].split(".")[0]] 
                + " " 
                + self.join_pattern[i][0].split(".")[0]
            )
            tabs_involved.append(
                alias_to_tab[self.join_pattern[i][2].split(".")[0]] 
                + " " 
                + self.join_pattern[i][2].split(".")[0]
            )
        tabs_involved = list(set(tabs_involved))
        return ", ".join(tabs_involved)
    def get_join_clause(self):
        return " AND ".join(
            [
                self.join_pattern[i][0] + self.join_pattern[i][1] + self.join_pattern[i][2] 
                for i in range(len(self.join_pattern))
            ]
        )

    # used only in/after _build_the_tree()
    # return (True/False, center_alias/None)
    def is_a_star(self):
        assert \
            self.alias_to_tab is not None, \
            "JoinPattern.is_a_star() called before ._build_the_tree()"
        
        if self._is_a_star_result is not None:
            return self._is_a_star_result
        if self.fk_pk_def is None:
            # we have no information to determine 
            #     whether it is a star
            return False, None

        center_alias = None
        for left, _, right in self.join_pattern:
            # convert to full column names
            left_alias = left.split(".")[0]
            right_alias = right.split(".")[0]
            _left = self.alias_to_tab[left_alias] \
                + "." + left.split(".")[1]
            _right = self.alias_to_tab[right_alias] \
                + "." + right.split(".")[1]
            
            cur_center_alias = None
            if _left in self.fk_pk_def:
                if (
                    isinstance(self.fk_pk_def[_left], list) \
                    and _right in self.fk_pk_def[_left]
                ) or _right == self.fk_pk_def[_left]:
                    cur_center_alias = left_alias
                else:
                    self._is_a_star_result = (False, None)
                    return self._is_a_star_result
            elif _right in self.fk_pk_def:
                if (
                    isinstance(self.fk_pk_def[_right], list) \
                    and _left in self.fk_pk_def[_right]
                ) or _left == self.fk_pk_def[_right]:
                    cur_center_alias = right_alias
                else:
                    self._is_a_star_result = (False, None)
                    return self._is_a_star_result
            else:
                self._is_a_star_result = (False, None)
                return self._is_a_star_result
            
            assert cur_center_alias is not None
            if center_alias is None or center_alias == cur_center_alias:
                center_alias = cur_center_alias
            else:
                self._is_a_star_result = (False, None)
                return self._is_a_star_result
        self._is_a_star_result = (True, center_alias)
        return self._is_a_star_result
            
    # utilized by tree_*.py
    def _build_the_tree(self, alias_to_tab):
        if self.n_tables != len(self.join_pattern) + 1:
            raise ValueError(f"The JoinPattern {self.join_pattern} is not a tree")
        self.alias_to_tab = alias_to_tab
        # determine a root 
        alias_to_alias = {}
        for left, _, right in self.join_pattern:
            left_alias = left.split(".")[0]
            right_alias = right.split(".")[0]
            if left_alias not in alias_to_alias:
                alias_to_alias[left_alias] = []
            alias_to_alias[left_alias].append(right_alias)
            if right_alias not in alias_to_alias:
                alias_to_alias[right_alias] = []
            alias_to_alias[right_alias].append(left_alias)

        is_a_star, center_alias = self.is_a_star()
        if is_a_star:
            # root must be the center
            # the next branch should work for most cases;
            #     only when it is a 2-table join, it can be 
            #     the other table in the star schema
            self.root_alias = center_alias
        else:
            # set the one with largest degree as the root
            self.root_alias = None
            for alias in alias_to_alias:
                if self.root_alias is None or len(alias_to_alias[alias]) > len(alias_to_alias[self.root_alias]):
                    self.root_alias = alias

        # get the tree structure
        self.alias_to_child = {}
        self.alias_to_depth = {self.root_alias : 0}
        def dfs(node, par):
            self.alias_to_child[node] = []
            for alias in alias_to_alias[node]:
                if alias != par:
                    self.alias_to_depth[alias] = self.alias_to_depth[node] + 1
                    self.alias_to_child[node].append(alias)
                    dfs(alias, node)
        dfs(self.root_alias, None)
        # reorder alias_to_join_col: the first join column is always the one connected to the parent node
        self.alias_to_child_join_col = {} # alias : {child alias : join col on alias}
        for left, _, right in self.join_pattern:
            left_alias = left.split(".")[0]
            right_alias = right.split(".")[0]
            if self.alias_to_depth[right_alias] < self.alias_to_depth[left_alias]:
                left, right = right, left
                left_alias, right_alias = right_alias, left_alias
            if left_alias not in self.alias_to_child_join_col:
                self.alias_to_child_join_col[left_alias] = {}
            self.alias_to_child_join_col[left_alias][right_alias] = left
            for i, col in enumerate(self.alias_to_join_col[right_alias]):
                if col == right:
                    self.alias_to_join_col[right_alias][0], self.alias_to_join_col[right_alias][i] = self.alias_to_join_col[right_alias][i], self.alias_to_join_col[right_alias][0]
                    break
        for alias in self.alias_to_depth:
            if alias not in self.alias_to_child:
                self.alias_to_child[alias] = []
            if alias not in self.alias_to_child_join_col:
                self.alias_to_child_join_col[alias] = {}
    def _tree_preorder(self):
        def dfs(node):
            order = [node]
            for child in self.alias_to_child[node]:
                order.extend(dfs(child))
            return order
        return dfs(self.root_alias)
    
    def __str__(self):
        return str(self.join_pattern)
    def __hash__(self):
        return hash(self.join_pattern)
    def __eq__(self, other):
        if isinstance(other, JoinPattern):
            return self.join_pattern == other.join_pattern
        return False

class QueryPattern: # JoinPattern + Columns involved for selections
    def __init__(self, join_pattern, list_of_sel_columns):
        self.join_pattern = join_pattern
        self.sel_columns = tuple(set(list_of_sel_columns))
    def __str__(self):
        return str(self.join_pattern) + " with selections on " + str(self.sel_columns)
    def __hash__(self):
        return hash((self.join_pattern, self.sel_columns))
    def __eq__(self, other):
        if isinstance(other, QueryPattern):
            return self.join_pattern == other.join_pattern and self.sel_columns == other.sel_columns
        return False