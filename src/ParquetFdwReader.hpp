#pragma once

#include "arrow/api.h"
#include "arrow/array.h"
#include "parquet/arrow/reader.h"
#include "parquet/arrow/schema.h"
#include "parquet/file_reader.h"
#include "parquet/statistics.h"

#include <atomic>
#include <filesystem>
#include <set>

#include "FastAllocator.hpp"
#include "Misc.hpp"
#include "ReadCoordinator.hpp"
#include "FilterPushdown.hpp"
#include "utils/palloc.h"

extern "C" {
#include "access/attnum.h"
#include "access/tupdesc.h"
#include "executor/tuptable.h"
#include "nodes/primnodes.h"
}

class ParquetFdwReader
{
private:
    struct PgTypeInfo
    {
        Oid oid;

        /* For array types. elem_type == InvalidOid means type is not an array */
        Oid   elem_type;
        int16 elem_len;
        bool  elem_byval;
        char  elem_align;
    };

    struct ChunkInfo {
        const std::shared_ptr<arrow::Array> sharedArray;
        const arrow::Array* array;
        const bool hasNulls;

        ChunkInfo(std::shared_ptr<arrow::Array> _sharedArray = nullptr)
        : sharedArray(_sharedArray)
        , array(_sharedArray.get())
        , hasNulls(_sharedArray && _sharedArray->null_count() > 0)
        { }
    };

    std::unique_ptr<FastAllocator> allocator;

    std::shared_ptr<parquet::FileMetaData> metadata;
    std::shared_ptr<arrow::Schema> schema;

    /* Arrow column indices that are used in query */
    std::vector<int> indices;

    std::vector<arrow::Type::type> columnTypes;

    std::vector<ChunkInfo> columnChunks;

    std::vector<PgTypeInfo> pg_types;

    int                    row_group;  /* current row group index */
    uint32_t               row;        /* current row within row group */
    uint32_t               num_rows;   /* total rows in row group */

    /* Wether object is properly initialized */
    // bool initialized;

    size_t numRowGroups;

    parquet::ArrowReaderProperties props;

    const std::string parquetFilePath;

    std::unique_ptr<parquet::arrow::FileReader> getFileReader() const;
    std::unique_ptr<parquet::arrow::FileReader> fileReader;

public:

    ParquetFdwReader(const char* parquetFilePath);

    ~ParquetFdwReader()
    {
        if (allocator)
            allocator.release();
    }

    void bufferRowGroup(const int32_t rowGroupId, TupleDesc tupleDesc,
        const std::vector<bool>& attrUseList);

    bool  next(TupleTableSlot *slot, bool fake = false);
    void  populate_slot(TupleTableSlot *slot, bool fake = false);
    void  rescan();
    Datum read_primitive_type(const arrow::Array *array, int type_id, int64_t i);

    std::shared_ptr<arrow::Schema> GetSchema() const {
        if (!schema)
            throw Error("Schema not set");

        return schema;
    }

    /*
     * copy_to_c_array
     *      memcpy plain values from Arrow array to a C array.
     */
    template <typename T>
    static inline void copy_to_c_array(T *values, const arrow::Array *array, int elem_size)
    {
        const T *in = GetPrimitiveValues<T>(*array);

        memcpy(values, in, elem_size * array->length());
    }

    /*
     * GetPrimitiveValues
     *      Get plain C value array. Copy-pasted from Arrow.
     */
    template <typename T>
    static inline const T *GetPrimitiveValues(const arrow::Array &arr)
    {
        if (arr.length() == 0)
        {
            return nullptr;
        }
        const auto &prim_arr   = arrow::internal::checked_cast<const arrow::PrimitiveArray &>(arr);
        const T *   raw_values = reinterpret_cast<const T *>(prim_arr.values()->data());
        return raw_values + arr.offset();
    }

    bool finishedReadingRowGroup() const {
        return row >= num_rows;
    }

    bool finishedReadingTable() const {
        return finishedReadingRowGroup();
    }

    size_t getNumRowGroups() const {
        return numRowGroups;
    }

    auto getRowGroup(const size_t rowGroupId) const {
        return metadata->RowGroup(rowGroupId);
    }

    int64_t getNumTotalRows() const {
        if (!metadata)
            throw Error("Metadata not set.");
        return metadata->num_rows();
    }

    void setMemoryContext(MemoryContext cxt);
    void validateSchema(TupleDesc tupleDesc) const;
    void schemaMustBeEqual(const std::shared_ptr<arrow::Schema> otherSchema) const;

    void finishReadingFile() {
        if (fileReader)
            fileReader.reset();
    }
};
