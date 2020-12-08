
#include "arrow/api.h"
#include "arrow/csv/reader.h"
#include "arrow/io/api.h"
#include "parquet/arrow/writer.h"

extern "C" {
#include "catalog/pg_type_d.h"
#include "postgres.h"
#include "utils/builtins.h"
}

#include "ConvertCsvToParquet.hpp"

#include <filesystem>

int64_t ConvertCsvToParquet::convert(const char *srcFilePath,
                                     const char *targetFilePath,
                                     const char *compressionType,
                                     ArrayType * fieldNames)
{
    const auto csvTableResult = getCsvTableReader(srcFilePath)->Read();
    if (!csvTableResult.ok())
        elog(ERROR, "Could not read CSV file: %s", csvTableResult.status().ToString().c_str());

    const auto csvTable = assignFieldNames(fieldNames, csvTableResult.ValueOrDie());
    writeParquetFile(targetFilePath, csvTable);

    return csvTable->num_rows();
}

std::vector<std::string> ConvertCsvToParquet::textArrayToVector(ArrayType *array)
{
    Datum *fieldsArray;
    bool * nulls;
    int    fieldsCount;
    deconstruct_array(array, TEXTOID, -1, false, 'i', &fieldsArray, &nulls, &fieldsCount);

    std::vector<std::string> fields(fieldsCount);
    for (int i = 0; i < fieldsCount; i++)
    {
        fields[i] = std::string(text_to_cstring(DatumGetTextP(fieldsArray[i])));
    }
    return fields;
}

std::shared_ptr<arrow::csv::TableReader>
        ConvertCsvToParquet::getCsvTableReader(const char *src_filepath)
{
    std::filesystem::path src(src_filepath);
    if (!std::filesystem::exists(src))
        elog(ERROR, "Source file does not exist");

    const auto inputSrcFileResult = arrow::io::ReadableFile::Open(src.native());
    if (!inputSrcFileResult.ok())
        elog(ERROR, "Could not open CSV source file: %s",
             inputSrcFileResult.status().ToString().c_str());

    const auto inputSrcFile = inputSrcFileResult.ValueOrDie();

    auto readOptions                      = arrow::csv::ReadOptions::Defaults();
    readOptions.autogenerate_column_names = true;

    const auto parseOptions   = arrow::csv::ParseOptions::Defaults();
    const auto convertOptions = arrow::csv::ConvertOptions::Defaults();

    auto *     pool            = arrow::default_memory_pool();
    const auto csvReaderResult = arrow::csv::TableReader::Make(pool, inputSrcFile, readOptions,
                                                               parseOptions, convertOptions);

    if (csvReaderResult.ok())
        return csvReaderResult.ValueOrDie();
    elog(ERROR, "Could not allocate CSV reader: %s", csvReaderResult.status().ToString().c_str());
}

ConvertCsvToParquet::tArrowTablePtr
        ConvertCsvToParquet::assignFieldNames(ArrayType *          field_names,
                                              const tArrowTablePtr targetTable)
{
    if (!field_names || ARR_HASNULL(field_names))
        elog(ERROR, "Field names not provided or incomplete");

    const auto     fieldNames = textArrayToVector(field_names);
    const uint64_t numColumns = targetTable->num_columns();
    if (numColumns != fieldNames.size())
        elog(ERROR, "Column count does not match field name count (%lu vs. %lu)", numColumns,
             fieldNames.size());

    const auto renamedColumnsTableResult = targetTable->RenameColumns(fieldNames);
    if (renamedColumnsTableResult.ok())
        return renamedColumnsTableResult.ValueOrDie();

    elog(ERROR, "Could not assign provided column names: %s",
         renamedColumnsTableResult.status().ToString().c_str());
}

void ConvertCsvToParquet::writeParquetFile(const char *target_filepath, tArrowTablePtr srcTable)
{
    std::filesystem::path dest(target_filepath);
    if (std::filesystem::exists(dest))
        elog(ERROR, "Target file does exist already");

    const auto outputDestFileResult = arrow::io::FileOutputStream::Open(dest.native());
    if (!outputDestFileResult.ok())
        elog(ERROR, "Could not open target parquet file: %s",
             outputDestFileResult.status().ToString().c_str());

    const auto outputDestFile     = outputDestFileResult.ValueOrDie();
    const auto writeParquetResult = parquet::arrow::WriteTable(
            *srcTable, arrow::default_memory_pool(), outputDestFile, 1000000);
    if (!writeParquetResult.ok())
        elog(ERROR, "Could not write target parquet file: %s",
             writeParquetResult.ToString().c_str());
}
