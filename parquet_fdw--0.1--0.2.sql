CREATE FUNCTION import_parquet(
    tablename  text,
    schemaname text,
    servername text,
    func       regproc,
    arg        jsonb,
    options    jsonb default NULL)
RETURNS VOID
AS 'MODULE_PATHNAME'
LANGUAGE C;

