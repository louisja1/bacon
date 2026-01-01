CREATE FUNCTION find_bucket_float(
    val double precision,
    buckets double precision[][],
    need_null boolean
) RETURNS int
AS 'MODULE_PATHNAME', 'find_bucket_float'
LANGUAGE C;