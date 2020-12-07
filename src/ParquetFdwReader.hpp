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

extern "C" {
#include "access/attnum.h"
#include "access/tupdesc.h"
#include "executor/tuptable.h"
#include "nodes/primnodes.h"
}

/*
 * Restriction
 */
struct RowGroupFilter
{
    AttrNumber  attnum;
    Const      *value;
    int         strategy;
};

struct ChunkInfo
{
    int     chunk;      /* current chunk number */
    int64   pos;        /* current pos within chunk */
    int64   len;        /* current chunk length */
};

class ParquetFdwReader
{
private:
    struct PgTypeInfo
    {
        Oid     oid;

        /* For array types. elem_type == InvalidOid means type is not an array */
        Oid     elem_type;
        int16   elem_len;
        bool    elem_byval;
        char    elem_align;
    };
public:
    const std::filesystem::path filePath;
    /* id needed for parallel execution */
    int32                           reader_id;

    std::unique_ptr<parquet::arrow::FileReader> reader;

    std::shared_ptr<arrow::Schema>  schema;

    /* Arrow column indices that are used in query */
    std::vector<int>                indices;

    /*
     * Mapping between slot attributes and arrow result set columns.
     * Corresponds to 'indices' vector.
     */
    std::vector<int>                map;

    /*
     * Cast functions from dafult postgres type defined in `to_postgres_type`
     * to actual table column type.
     */
    std::vector<FmgrInfo *>         castfuncs;

    /* Current row group */
    std::shared_ptr<arrow::Table>   table;

    /*
     * Plain pointers to inner the structures of row group. It's needed to
     * prevent excessive shared_ptr management.
     */
    std::vector<arrow::Array *>     chunks;
    std::vector<arrow::DataType *>  types;

    std::vector<PgTypeInfo>         pg_types;

    bool           *has_nulls;          /* per-column info on nulls */

    int             row_group;          /* current row group index */
    uint32_t        row;                /* current row within row group */
    uint32_t        num_rows;           /* total rows in row group */
    std::vector<ChunkInfo> chunk_info;  /* current chunk and position per-column */

    /*
     * Filters built from query restrictions that help to filter out row
     * groups.
     */
    std::list<RowGroupFilter>       filters;

    /*
     * List of row group indexes to scan
     */
    std::vector<int>                rowgroups;

    FastAllocator                  *allocator;

    /* Wether object is properly initialized */
    bool     initialized;

    ParquetFdwReader(const char* file_path, const int reader_id)
        : filePath(file_path), reader_id(reader_id), row_group(-1), row(0), num_rows(0),
          initialized(false)
    { }

    ~ParquetFdwReader()
    {
        if (allocator)
            delete allocator;
    }

    void open(const char *filename,
              MemoryContext cxt,
              TupleDesc tupleDesc,
              std::set<int> &attrs_used,
              bool use_threads,
              bool use_mmap);

    void prepareToReadRowGroup(const int32_t rowGroupId, TupleDesc tupleDesc);
    bool next(TupleTableSlot *slot, bool fake=false);
    void populate_slot(TupleTableSlot *slot, bool fake=false);
    void rescan(void);
    Datum
    read_primitive_type(arrow::Array *array,
                        int type_id, int64_t i,
                        FmgrInfo *castfunc);
    Datum
    nested_list_get_datum(arrow::Array *array, int arrow_type,
                          PgTypeInfo *pg_type, FmgrInfo *castfunc);

    void initialize_castfuncs(TupleDesc tupleDesc);

    /*
     * copy_to_c_array
     *      memcpy plain values from Arrow array to a C array.
     */
    template<typename T> static inline void
    copy_to_c_array(T *values, const arrow::Array *array, int elem_size)
    {
        const T *in = GetPrimitiveValues<T>(*array);

        memcpy(values, in, elem_size * array->length());
    }

    /*
     * GetPrimitiveValues
     *      Get plain C value array. Copy-pasted from Arrow.
     */
    template <typename T>
    static inline const T* GetPrimitiveValues(const arrow::Array& arr) {
        if (arr.length() == 0) {
            return nullptr;
        }
        const auto& prim_arr = arrow::internal::checked_cast<const arrow::PrimitiveArray&>(arr);
        const T* raw_values = reinterpret_cast<const T*>(prim_arr.values()->data());
        return raw_values + arr.offset();
    }

    void set_rowgroups_list(const std::vector<int> &rowgroups);

    bool finishedReadingRowGroup() const {
        return row >= num_rows;
    }
};
