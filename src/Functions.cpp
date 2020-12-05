
#if __cplusplus > 199711L
#define register      // Deprecated in C++11.
#endif  // #if __cplusplus > 199711L

extern "C" {
#include "postgres.h"
#include "fmgr.h"
#include "utils/builtins.h"
}

#include <filesystem>

#include "arrow/io/file.h"
#include "arrow/csv/api.h"
#include "parquet/arrow/writer.h"

static void convertCsvToParquet(
      const char* src_filepath
    , const char* target_filepath
    , const char* compression_type
) {
    std::filesystem::path src(src_filepath);
    if (!std::filesystem::exists(src))
        elog(ERROR, "Source file does not exist");

    std::filesystem::path dest(target_filepath);
    if (std::filesystem::exists(dest))
        elog(ERROR, "Target file does exist already");

    const auto inputSrcFileResult = arrow::io::ReadableFile::Open(src.native());
    if (!inputSrcFileResult.ok())
        elog(ERROR, "Could not open CSV source file: %s", inputSrcFileResult.status().ToString().c_str());

    const auto inputSrcFile = inputSrcFileResult.ValueOrDie();

    const auto readOptions = arrow::csv::ReadOptions::Defaults();
    const auto parseOptions = arrow::csv::ParseOptions::Defaults();
    const auto convertOptions = arrow::csv::ConvertOptions::Defaults();

    auto* pool = arrow::default_memory_pool();
    const auto csvReaderResult = arrow::csv::TableReader::Make(
        pool, inputSrcFile, readOptions, parseOptions, convertOptions);

    if (!csvReaderResult.ok())
        elog(ERROR, "Could not allocate CSV reader: %s", csvReaderResult.status().ToString().c_str());

    const auto csvReader = csvReaderResult.ValueOrDie();
    const auto csvTableResult = csvReader->Read();
    if (!csvTableResult.ok())
        elog(ERROR, "Could not read CSV file: %s", csvTableResult.status().ToString().c_str());

    const auto csvTable = csvTableResult.ValueOrDie();

    const auto outputDestFileResult = arrow::io::FileOutputStream::Open(dest.native());
    if (!outputDestFileResult.ok())
        elog(ERROR, "Could not open target parquet file: %s", outputDestFileResult.status().ToString().c_str());

    const auto outputDestFile = outputDestFileResult.ValueOrDie();
    const auto writeParquetResult = parquet::arrow::WriteTable(*csvTable, arrow::default_memory_pool(), outputDestFile, 3);
    if (!writeParquetResult.ok())
        elog(ERROR, "Could not write target parquet file: %s", writeParquetResult.ToString().c_str());
}

extern "C" {
PG_FUNCTION_INFO_V1(convert_csv_to_parquet);
Datum convert_csv_to_parquet(PG_FUNCTION_ARGS)
{
    char *src_filepath;
    char *target_filepath;
    char *compression_type;

    src_filepath = PG_ARGISNULL(0) ? NULL : text_to_cstring(PG_GETARG_TEXT_P(0));
    target_filepath = PG_ARGISNULL(1) ? NULL : text_to_cstring(PG_GETARG_TEXT_P(1));
    compression_type = PG_ARGISNULL(2) ? NULL : text_to_cstring(PG_GETARG_TEXT_P(2));

    convertCsvToParquet(src_filepath, target_filepath, compression_type);

    PG_RETURN_VOID();
}
}
