#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"
#include "executor/spi.h"
#include "utils/builtins.h"
#include "utils/array.h"
#include "utils/jsonb.h"
#include "utils/numeric.h"
#include "catalog/pg_type.h"
#include "utils/timestamp.h"

PG_MODULE_MAGIC;
PG_FUNCTION_INFO_V1(find_bucket_table);

#define BATCH_SIZE 50000

/* Helper function: binary search for int */
static inline int find_bucket_int(int32 val, int32 *data, int len)
{
    int le = 0, ri = (len >> 1) - 1;
    if (val < data[0] || val >= data[(ri << 1) + 1])
        return -1;

    while (le <= ri)
    {
        int mid = (le + ri) >> 1;
        int low = data[mid << 1];
        int high = data[(mid << 1) + 1];

        if (val >= low && val < high)
        {
            return mid;
        }
        else if (val < low)
        {
            ri = mid - 1;
        }
        else
        {
            le = mid + 1;
        }
    }

    return -1;
}

/* Helper function: binary search for timestamp */
static inline int find_bucket_ts(TimestampTz val, TimestampTz *data, int len)
{
    int le = 0, ri = (len >> 1) - 1;
    if (val < data[0] || val >= data[(ri << 1) + 1])
        return -1;

    while (le <= ri)
    {
        int mid = (le + ri) >> 1;
        TimestampTz low = data[mid << 1];
        TimestampTz high = data[(mid << 1) + 1];

        if (val >= low && val < high)
        {
            return mid;
        }
        else if (val < low)
        {
            ri = mid - 1;
        }
        else
        {
            le = mid + 1;
        }
    }

    return -1;
}

/* Parse jsonb array of ints into int* */
static int *parse_jsonb_int_array(Jsonb *jsonb, int expected_len, MemoryContext mcxt)
{
    JsonbIterator *it = JsonbIteratorInit(&jsonb->root);
    JsonbValue v;
    int tok;
    int *arr = NULL;
    int count = 0;

    MemoryContext oldctx = MemoryContextSwitchTo(mcxt);
    arr = palloc(sizeof(int) * expected_len);

    while ((tok = JsonbIteratorNext(&it, &v, false)) != WJB_DONE)
    {
        if (tok == WJB_ELEM && v.type == jbvNumeric)
        {
            arr[count++] = DatumGetInt32(
                DirectFunctionCall1(numeric_int4, NumericGetDatum(v.val.numeric)));
        }
    }

    MemoryContextSwitchTo(oldctx);

    if (count != expected_len)
        ereport(ERROR, (errmsg("Number of bounds (both left and right) does not match the specifications")));

    return arr;
}

/* Parse jsonb array of timestamps into TimestampTz* */
static TimestampTz *parse_jsonb_ts_array(Jsonb *jsonb, int expected_len, MemoryContext mcxt)
{
    JsonbIterator *it = JsonbIteratorInit(&jsonb->root);
    JsonbValue v;
    int tok;
    TimestampTz *arr = NULL;
    int count = 0;

    MemoryContext oldctx = MemoryContextSwitchTo(mcxt);
    arr = palloc(sizeof(TimestampTz) * expected_len);

    while ((tok = JsonbIteratorNext(&it, &v, false)) != WJB_DONE)
    {
        if (tok == WJB_ELEM && v.type == jbvString)
        {
            char *str = pnstrdup(v.val.string.val, v.val.string.len);
            TimestampTz ts = DatumGetTimestampTz(
                DirectFunctionCall3(timestamptz_in, CStringGetDatum(str), ObjectIdGetDatum(InvalidOid), Int32GetDatum(-1))
            );
            arr[count++] = ts;
            pfree(str);
        }
    }

    MemoryContextSwitchTo(oldctx);

    if (count != expected_len)
        ereport(ERROR, (errmsg("Number of bounds (both left and right) does not match the specifications")));

    return arr;
}

typedef struct
{
    Portal portal;
    SPITupleTable *tuptable;
    TupleDesc tupdesc;
    int current_row;
    bool done;

    void **buckets;
    int *bucket_lens;
    int n_sel;
    int n_join;
    int *type_names;
    bool *need_nulls;
    Datum *values;
    bool *isnull;
    Datum *bucket_id_datums;
    TupleDesc outdesc;
} BucketCtx;

/* Main function */
Datum find_bucket_table(PG_FUNCTION_ARGS)
{
    text *tab = PG_GETARG_TEXT_P(0);
    ArrayType *join_cols_arr = PG_GETARG_ARRAYTYPE_P(1);
    ArrayType *sel_cols_arr = PG_GETARG_ARRAYTYPE_P(2);
    ArrayType *bucket_lens_arr = PG_GETARG_ARRAYTYPE_P(3);
    ArrayType *buckets_arr = PG_GETARG_ARRAYTYPE_P(4);
    ArrayType *type_names_arr = PG_GETARG_ARRAYTYPE_P(5);
    ArrayType *need_nulls_arr = PG_GETARG_ARRAYTYPE_P(6);
    text *where_clause = PG_GETARG_TEXT_P(7);

    int n_join, n_sel, n_bucket_len, n_bucket, n_type, n_flag;
    Datum *join_cols, *sel_cols, *bucket_len_elems, *bucket_elems, *type_names, *need_nulls;
    bool *nulls;

    deconstruct_array(join_cols_arr, TEXTOID, -1, false, 'i', &join_cols, &nulls, &n_join);
    deconstruct_array(sel_cols_arr, TEXTOID, -1, false, 'i', &sel_cols, &nulls, &n_sel);
    deconstruct_array(bucket_lens_arr, INT4OID, 4, true, 'i', &bucket_len_elems, &nulls, &n_bucket_len);
    deconstruct_array(buckets_arr, JSONBOID, -1, false, 'i', &bucket_elems, &nulls, &n_bucket);
    deconstruct_array(type_names_arr, TEXTOID, -1, false, 'i', &type_names, &nulls, &n_type);
    deconstruct_array(need_nulls_arr, BOOLOID, 1, true, 'c', &need_nulls, &nulls, &n_flag);

    if ((n_sel != n_bucket_len) || (n_sel != n_bucket) || (n_sel != n_type) || (n_sel != n_flag))
        ereport(ERROR, (errmsg("Input array sizes unmatch")));

    FuncCallContext *funcctx;

    if (SRF_IS_FIRSTCALL())
    {
        funcctx = SRF_FIRSTCALL_INIT();
        MemoryContext oldctx = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        BucketCtx *ctx = palloc(sizeof(BucketCtx));

        /* parse the buckets */
        void **buckets = palloc(sizeof(void *) * n_sel);
        int *bucket_lens = palloc(sizeof(int) * n_bucket_len);

        for (int i = 0; i < n_bucket_len; i++)
            bucket_lens[i] = DatumGetInt32(bucket_len_elems[i]);

        ctx->type_names = palloc(sizeof(int) * n_sel);
        for (int i = 0; i < n_sel; i++)
        {
            char *type_name = TextDatumGetCString(type_names[i]);
            if (strcmp(type_name, "int") == 0)
                ctx->type_names[i] = 0;
            else if (strcmp(type_name, "ts") == 0)
                ctx->type_names[i] = 1;
            else
                ctx->type_names[i] = -1;

            Jsonb *jb = DatumGetJsonbP(bucket_elems[i]);
            if (ctx->type_names[i] == 0)
                buckets[i] = parse_jsonb_int_array(jb, bucket_lens[i], funcctx->multi_call_memory_ctx);
            else if (ctx->type_names[i] == 1)
                buckets[i] = parse_jsonb_ts_array(jb, bucket_lens[i], funcctx->multi_call_memory_ctx);
            else
                ereport(ERROR, (errmsg("Unsupported type: %s", type_name)));
        }

        /** construct sql */
        StringInfoData sql;
        initStringInfo(&sql);

        appendStringInfo(&sql, "SELECT ");
        for (int i = 0; i < n_join; i++)
        {
            if (i > 0) appendStringInfoString(&sql, ", ");
            appendStringInfoString(&sql, TextDatumGetCString(join_cols[i]));
        }
        for (int i = 0; i < n_sel; i++)
        {
            appendStringInfo(&sql, ", %s", TextDatumGetCString(sel_cols[i]));
        }

        appendStringInfo(&sql, " FROM %s", text_to_cstring(tab));
        char *where = text_to_cstring(where_clause);
        if (where && strlen(where) > 0)
            appendStringInfo(&sql, " WHERE %s", where);

        appendStringInfoString(&sql, " ORDER BY ");
        for (int i = 0; i < n_join; i++)
        {
            if (i > 0) appendStringInfoString(&sql, ", ");
            appendStringInfoString(&sql, TextDatumGetCString(join_cols[i]));
        }

        /* execute the sql */
        SPI_connect();
        SPIPlanPtr plan = SPI_prepare(sql.data, 0, NULL);
        if (!plan)
            ereport(ERROR, (errmsg("SPI_prepare failed")));
        ctx->portal = SPI_cursor_open(NULL, plan, NULL, NULL, true);
        if (!ctx->portal)
            ereport(ERROR, (errmsg("Failed to open SPI cursor: %s", sql.data)));

        ctx->done = false;
        ctx->current_row = 0;
        ctx->tuptable = NULL;
        ctx->tupdesc = NULL;

        ctx->buckets = buckets;
        ctx->bucket_lens = bucket_lens;
        ctx->n_join = n_join;
        ctx->n_sel = n_sel;
        ctx->need_nulls = palloc(sizeof(bool) * n_sel);
        for (int i = 0; i < n_sel; i++)
            ctx->need_nulls[i] = DatumGetBool(need_nulls[i]);

        ctx->values = palloc(sizeof(Datum) * (ctx->n_join + ctx->n_sel));
        ctx->isnull = palloc(sizeof(bool) * (ctx->n_join + ctx->n_sel));
        ctx->bucket_id_datums = palloc(sizeof(Datum) * ctx->n_sel);

        /* output tuple descriptor */
        ctx->outdesc = CreateTemplateTupleDesc(2);
        TupleDescInitEntry(ctx->outdesc, 1, "join_vals", INT4ARRAYOID, -1, 0);
        TupleDescInitEntry(ctx->outdesc, 2, "bucket_ids", INT4ARRAYOID, -1, 0);
        BlessTupleDesc(ctx->outdesc);

        funcctx->user_fctx = ctx;
        MemoryContextSwitchTo(oldctx);
    }

    funcctx = SRF_PERCALL_SETUP();
    BucketCtx *ctx = (BucketCtx *)funcctx->user_fctx;

    if (ctx->done) SRF_RETURN_DONE(funcctx);

    while (true) {
        if (!ctx->tuptable || ctx->current_row >= SPI_processed) {
            if (ctx->tuptable) SPI_freetuptable(ctx->tuptable);
            SPI_cursor_fetch(ctx->portal, true, BATCH_SIZE);
            if (SPI_processed == 0) {
                SPI_cursor_close(ctx->portal);
                SPI_finish();
                ctx->done = true;
                SRF_RETURN_DONE(funcctx);
            }
            ctx->tuptable = SPI_tuptable;
            ctx->tupdesc = SPI_tuptable->tupdesc;
            ctx->current_row = 0;
        }

        HeapTuple tup = SPI_tuptable->vals[ctx->current_row++];
        heap_deform_tuple(tup, SPI_tuptable->tupdesc, ctx->values, ctx->isnull);

        MemoryContext oldctx = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        bool skip = false;
        for (int i = 0; i < ctx->n_sel; i++)
        {
            int idx = ctx->n_join + i;
            int32 bucket_id;

            if (ctx->isnull[idx])
            {
                bucket_id = ctx->need_nulls[i] ? 0 : -1;
            }
            else
            {
                if (ctx->type_names[i] == 0) // int
                    bucket_id = find_bucket_int(DatumGetInt32(ctx->values[idx]), ctx->buckets[i], ctx->bucket_lens[i]);
                else if (ctx->type_names[i] == 1) // ts
                    bucket_id = find_bucket_ts(DatumGetTimestampTz(ctx->values[idx]), ctx->buckets[i], ctx->bucket_lens[i]);
            }

            if (bucket_id == -1)
            {
                skip = true;
                break;
            }

            ctx->bucket_id_datums[i] = Int32GetDatum(bucket_id);
        }

        if (skip)
        {
            MemoryContextSwitchTo(oldctx);
            continue;
        }

        Datum out_data[2];
        bool out_nulls[2] = {false, false};

        out_data[0] = construct_array(ctx->values, ctx->n_join, INT4OID, 4, true, 'i');
        out_data[1] = construct_array(ctx->bucket_id_datums, ctx->n_sel, INT4OID, 4, true, 'i');

        HeapTuple out_tup = heap_form_tuple(ctx->outdesc, out_data, out_nulls);

        MemoryContextSwitchTo(oldctx);
        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(out_tup));
    }
}
