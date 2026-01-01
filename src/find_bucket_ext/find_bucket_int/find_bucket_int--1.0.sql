CREATE FUNCTION find_bucket_int(
    val int,
    buckets int[][],
    need_null boolean
) RETURNS int
AS 'MODULE_PATHNAME', 'find_bucket_int'
LANGUAGE C;