
#pragma once

extern "C" {
#include "utils/array.h"
}

#include <memory>
#include <string>
#include <vector>

namespace arrow
{
class Table;
}

namespace arrow::csv
{
class TableReader;
}

class ConvertCsvToParquet
{
    using tArrowTablePtr = std::shared_ptr<arrow::Table>;

    std::vector<std::string>                 textArrayToVector(ArrayType *array);
    std::shared_ptr<arrow::csv::TableReader> getCsvTableReader(const char *src_filepath);
    tArrowTablePtr assignFieldNames(ArrayType *field_names, const tArrowTablePtr targetTable);
    void           writeParquetFile(const char *target_filepath, tArrowTablePtr srcTable);

public:
    ConvertCsvToParquet() = default;

    int64_t convert(const char *srcFilePath,
                    const char *targetFilePath,
                    const char *compressionType,
                    ArrayType * fieldNames);
};
