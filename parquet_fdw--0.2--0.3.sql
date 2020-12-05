CREATE FUNCTION convert_csv_to_parquet(
  src_filepath     TEXT,
  target_filepath  TEXT,
  compression_type TEXT DEFAULT 'zstd'
) RETURNS VOID
AS 'MODULE_PATHNAME', 'convert_csv_to_parquet'
LANGUAGE C;
