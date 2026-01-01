CREATE FUNCTION find_bucket_ts(
    val timestamp without time zone,
    buckets timestamp without time zone[][],
    need_null boolean
) RETURNS int
AS 'MODULE_PATHNAME', 'find_bucket_ts'
LANGUAGE C;