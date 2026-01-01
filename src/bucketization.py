# Bucketization of a column
from collections import OrderedDict

class Bucketization:
    def __init__(
        self, 
        list_of_two_sels, 
        flag_for_queries_wo_sel_on_this_col, 
        batch_size
    ):
        # list_of_two_sels : [(lb0, ub0), ...] where ub0 is open-ended 
        #     and lb0 is close-ended, i.e., lb0 <= . < ub0
        ends = []
        for lb, ub in list_of_two_sels:
            ends.append((lb, 1))
            ends.append((ub, -1))
        # upper bounds are prior to lower bounds when they share the same value
        ends.sort() 

        self.chunks = [] 
        # a list of (lb, ub), denoting lb <= . < ub, and guarantees that 
        #     these chunks are sorted in ascending order
        i = 0
        acc = 0
        prev = None
        while i < len(ends):
            this_chunk_is_interesting = acc > 0
            j = i
            acc += ends[i][1]
            while j + 1 < len(ends) and ends[j + 1][0] == ends[i][0]:
                j += 1
                acc += ends[j][1]
            if this_chunk_is_interesting:
                if prev is None:
                    raise ValueError("Something wrong with the bucketization")
                self.chunks.append((prev, ends[i][0]))
            prev = ends[i][0]
            i = j + 1
    
        self.flag_for_queries_wo_sel_on_this_col \
            = flag_for_queries_wo_sel_on_this_col
        self.cache = OrderedDict()
        self.cache_budget = batch_size

    def size(self):
        return len(self.chunks)
    
    def map_a_lb(self, lb): # map lb <= . by a chunk[x]
        le = 0
        ri = len(self.chunks) - 1
        ans = None
        while le <= ri:
            mid = (le + ri) >> 1
            if self.chunks[mid][0] <= lb:
                ans = mid
                le = mid + 1
            else:
                ri = mid - 1
        if ans is None or self.chunks[ans][0] != lb:
            raise ValueError("Location on a chunk failed")
        return ans
    
    def map_an_ub(self, ub, searching_le=0): # map . < ub by a chunk[x]
        le = searching_le
        ri = len(self.chunks) - 1
        ans = None
        while le <= ri:
            mid = (le + ri) >> 1
            if self.chunks[mid][1] >= ub:
                ans = mid
                ri = mid - 1
            else:
                le = mid + 1
        if ans is None or self.chunks[ans][1] != ub:
            raise ValueError("Location on a chunk failed")
        return ans
    
    def map_a_range(self, lb, ub): 
        # map lb <= . < ub by chunks[x] ... chunks[y]
        x = self.map_a_lb(lb)
        y = self.map_an_ub(ub, searching_le = x)
        return x, y
    
    def map_a_val(self, val): 
        # map a value by chunk[x][0] <= val < chunk[x][1]
        # NOTE 
        # (1) if val is None, we treat it as col.min - 1, which should 
        #     fall into the first buckect (0-th bucket if exists)
        # (2) if val is out-of-interests, we return None
        if val is None:
            if self.flag_for_queries_wo_sel_on_this_col:
                # missing value is queried by some queries without selection
                return 0 
            else:
                return None # missing value is out-of-interest
        if val in self.cache:
            return self.cache[val]
        le = 0
        ri = len(self.chunks) - 1
        ans = None
        while le <= ri:
            mid = (le + ri) >> 1
            if self.chunks[mid][0] <= val:
                ans = mid 
                le = mid + 1
            else:
                ri = mid - 1
        if ans is not None and val >= self.chunks[ans][1]:
            ans = None
        while len(self.cache) >= self.cache_budget:
            self.cache.popitem(last=False)
        self.cache[val] = ans
        return ans
    
    def get_merged_chunks(self):
        merged_chunks = []
        for i in range(self.size()):
            if len(merged_chunks) == 0 \
                or merged_chunks[-1][1] != self.chunks[i][0]:
                merged_chunks.append(self.chunks[i])
            else:
                merged_chunks[-1] = (merged_chunks[-1][0], self.chunks[i][1])
        return merged_chunks
    
    def __str__(self):
        return str(self.chunks)