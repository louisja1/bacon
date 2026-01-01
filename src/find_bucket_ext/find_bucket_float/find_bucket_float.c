#include "postgres.h"
#include "fmgr.h"
#include "utils/array.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(find_bucket_float);

Datum find_bucket_float(PG_FUNCTION_ARGS)
{
    if (PG_ARGISNULL(0) && PG_GETARG_BOOL(2))
    {
        PG_RETURN_INT32(0);
    }
    else if (PG_ARGISNULL(0))
    {
        PG_RETURN_NULL();
    }

    float8 val = PG_GETARG_FLOAT8(0);
    ArrayType *buckets = PG_GETARG_ARRAYTYPE_P(1);

    typedef struct
    {
        float8 *data;
        int n_pairs;
    } Buckets;

    Buckets *cache = (Buckets *)fcinfo->flinfo->fn_extra;
    if (cache == NULL)
    {
        // first time to parse the buckets

        cache = MemoryContextAlloc(fcinfo->flinfo->fn_mcxt, sizeof(Buckets));

        if (ARR_NDIM(buckets) != 2)
        {
            ereport(ERROR, (errmsg("Expected a 2D array")));
        }

        int *dims = ARR_DIMS(buckets);
        cache->n_pairs = dims[0];
        cache->data = (float8 *)MemoryContextAlloc(
            fcinfo->flinfo->fn_mcxt,
            sizeof(float8) * dims[0] * dims[1]);
        memcpy(cache->data, ARR_DATA_PTR(buckets), sizeof(float8) * dims[0] * dims[1]);
        fcinfo->flinfo->fn_extra = cache;
    }

    float8 *data = cache->data;
    int n_pairs = cache->n_pairs;

    int le = 0;
    int ri = n_pairs - 1;

    if (val < data[0] || val >= data[ri * 2 + 1]) {
        if (PG_GETARG_BOOL(2)) {
            PG_RETURN_INT32(0);
        } else {
            PG_RETURN_NULL();
        }
    }

    while (le <= ri)
    {
        int mid = (le + ri) >> 1;
        if (val >= data[mid * 2] && val < data[mid * 2 + 1])
        {
            PG_RETURN_INT32(mid + 1);
        }
        else if (val < data[mid * 2])
        {
            ri = mid - 1;
        }
        else
        {
            le = mid + 1;
        }
    }
    if (PG_GETARG_BOOL(2)) {
        PG_RETURN_INT32(0);
    } else {
        PG_RETURN_NULL();
    }
}