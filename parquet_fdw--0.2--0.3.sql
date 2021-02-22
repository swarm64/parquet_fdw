CREATE FUNCTION convert_csv_to_parquet(
  src_filepath     TEXT,
  target_filepath  TEXT,
  field_names      TEXT[],
  compression_type TEXT DEFAULT 'zstd'
) RETURNS BIGINT
AS 'MODULE_PATHNAME', 'convert_csv_to_parquet'
LANGUAGE C;
