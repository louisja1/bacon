CREATE FUNCTION find_bucket_table(
    tab_name text,
    join_cols text[],
    sel_cols text[],
    bucket_lens int[],
    bucket_json jsonb[],
    type_names text[],
    need_nulls boolean[],
    where_clause text
) 
RETURNS TABLE (
   join_vals int[],
   bucket_ids int[]
)
AS 'MODULE_PATHNAME', 'find_bucket_table'
LANGUAGE C VOLATILE;