
#if __cplusplus > 199711L
#define register      // Deprecated in C++11.
#endif  // #if __cplusplus > 199711L

#include <filesystem>

#include "arrow/api.h"
#include "arrow/io/file.h"
#include "arrow/csv/api.h"
#include "parquet/arrow/writer.h"

extern "C" {
#include "postgres.h"
#include "fmgr.h"
#include "catalog/pg_type_d.h"
#include "utils/array.h"
#include "utils/builtins.h"
}

static std::vector<std::string> textArrayToVector(ArrayType* array) {
    Datum* fieldsArray;
    bool* nulls;
    int fieldsCount;
    deconstruct_array(array, TEXTOID, -1, false, 'i', &fieldsArray, &nulls, &fieldsCount);

    std::vector<std::string> fields;
    for (int i = 0; i < fieldsCount; i++) {
        fields.emplace_back(text_to_cstring(DatumGetTextP(fieldsArray[i])));
    }
    return fields;
}

static std::shared_ptr<arrow::csv::TableReader> getCsvTableReader(const char* src_filepath) {
    std::filesystem::path src(src_filepath);
    if (!std::filesystem::exists(src))
        elog(ERROR, "Source file does not exist");

    const auto inputSrcFileResult = arrow::io::ReadableFile::Open(src.native());
    if (!inputSrcFileResult.ok())
        elog(ERROR, "Could not open CSV source file: %s", inputSrcFileResult.status().ToString().c_str());

    const auto inputSrcFile = inputSrcFileResult.ValueOrDie();

    auto readOptions = arrow::csv::ReadOptions::Defaults();
    readOptions.autogenerate_column_names = true;

    const auto parseOptions = arrow::csv::ParseOptions::Defaults();
    const auto convertOptions = arrow::csv::ConvertOptions::Defaults();

    auto* pool = arrow::default_memory_pool();
    const auto csvReaderResult = arrow::csv::TableReader::Make(
        pool, inputSrcFile, readOptions, parseOptions, convertOptions);

    if (csvReaderResult.ok())
        return csvReaderResult.ValueOrDie();
    elog(ERROR, "Could not allocate CSV reader: %s", csvReaderResult.status().ToString().c_str());
}

static std::shared_ptr<arrow::Table> assignFieldNames(
      ArrayType* field_names
    , const std::shared_ptr<arrow::Table> targetTable
) {
    if (!field_names || ARR_HASNULL(field_names))
        elog(ERROR, "Field names not provided or incomplete");

    const auto fieldNames = textArrayToVector(field_names);
    const uint64_t numColumns = targetTable->num_columns();
    if (numColumns != fieldNames.size())
        elog(ERROR, "Column count does not match field name count (%lu vs. %lu)", numColumns, fieldNames.size());

    const auto renamedColumnsTableResult = targetTable->RenameColumns(fieldNames);
    if (renamedColumnsTableResult.ok())
        return renamedColumnsTableResult.ValueOrDie();

    elog(ERROR, "Could not assign provided column names: %s", renamedColumnsTableResult.status().ToString().c_str());
}

static void writeParquetFile(const char* target_filepath, std::shared_ptr<arrow::Table> srcTable) {
    std::filesystem::path dest(target_filepath);
    if (std::filesystem::exists(dest))
        elog(ERROR, "Target file does exist already");

    const auto outputDestFileResult = arrow::io::FileOutputStream::Open(dest.native());
    if (!outputDestFileResult.ok())
        elog(ERROR, "Could not open target parquet file: %s", outputDestFileResult.status().ToString().c_str());

    const auto outputDestFile = outputDestFileResult.ValueOrDie();
    const auto writeParquetResult = parquet::arrow::WriteTable(*srcTable, arrow::default_memory_pool(), outputDestFile, 1000000);
    if (!writeParquetResult.ok())
        elog(ERROR, "Could not write target parquet file: %s", writeParquetResult.ToString().c_str());
}

static int64_t convertCsvToParquet(
      const char* src_filepath
    , const char* target_filepath
    , const char* compression_type
    , ArrayType* field_names
) {
    const auto csvTableResult = getCsvTableReader(src_filepath)->Read();
    if (!csvTableResult.ok())
        elog(ERROR, "Could not read CSV file: %s", csvTableResult.status().ToString().c_str());

    const auto csvTable = assignFieldNames(field_names, csvTableResult.ValueOrDie());
    writeParquetFile(target_filepath, csvTable);

    return csvTable->num_rows();
}

extern "C" {
PG_FUNCTION_INFO_V1(convert_csv_to_parquet);
Datum convert_csv_to_parquet(PG_FUNCTION_ARGS)
{
    char *src_filepath;
    char *target_filepath;
    ArrayType *field_names;
    char *compression_type;

    src_filepath =
        PG_ARGISNULL(0) ? nullptr : text_to_cstring(PG_GETARG_TEXT_P(0));
    target_filepath =
        PG_ARGISNULL(1) ? nullptr : text_to_cstring(PG_GETARG_TEXT_P(1));
    field_names = PG_ARGISNULL(2) ? nullptr : PG_GETARG_ARRAYTYPE_P(2);
    compression_type =
        PG_ARGISNULL(3) ? nullptr : text_to_cstring(PG_GETARG_TEXT_P(3));

    const int64_t numRows = convertCsvToParquet(src_filepath, target_filepath, compression_type, field_names);

    PG_RETURN_INT64(numRows);
}
}
