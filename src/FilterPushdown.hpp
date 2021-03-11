
#pragma once

#if __cplusplus > 199711L
#    define register // Deprecated in C++11.
#endif               // #if __cplusplus > 199711L

#include <arrow/api.h>
#include <parquet/statistics.h>

#include "ParquetFdwReader.hpp"
#include "Misc.hpp"

extern "C" {
#include "postgres.h"
#include "postgres_ext.h"
#include "catalog/pg_type.h"
#include "access/tupdesc.h"
#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/timestamp.h"
}

#include <map>
#include <set>

struct FmgrInfo;
struct List;

class FilterPushdown {

private:
    struct RowGroupFilter
    {
        AttrNumber attnum;
        Const *    value;
        int        strategy;
    };

    std::vector<bool> rowGroupSkipList;
    std::vector<RowGroupFilter> filters;

    void find_cmp_func(FmgrInfo *finfo, Oid type1, Oid type2);

    /*
     * bytes_to_postgres_type
     *      Convert min/max values from column statistics stored in parquet file as
     *      plain bytes to postgres Datum.
     */
    static Datum bytes_to_postgres_type(const char *bytes, arrow::DataType *arrow_type)
    {
        switch (arrow_type->id())
        {
        case arrow::Type::BOOL:
            return BoolGetDatum(*(bool *)bytes);
        case arrow::Type::INT32:
            return Int32GetDatum(*(int32 *)bytes);
        case arrow::Type::INT64:
            return Int64GetDatum(*(int64 *)bytes);
        case arrow::Type::FLOAT:
            return Float4GetDatum(*(float *)bytes);
        case arrow::Type::DOUBLE:
            return Float8GetDatum(*(double *)bytes);
        case arrow::Type::STRING:
        case arrow::Type::BINARY:
            return CStringGetTextDatum(bytes);
        case arrow::Type::TIMESTAMP:
        {
            TimestampTz ts;
            auto        tstype = (arrow::TimestampType *)arrow_type;

            to_postgres_timestamp(tstype, *(int64 *)bytes, ts);
            return TimestampGetDatum(ts);
        }
        case arrow::Type::DATE32:
            return DateADTGetDatum(*(int32 *)bytes + (UNIX_EPOCH_JDATE - POSTGRES_EPOCH_JDATE));
        default:
            return PointerGetDatum(NULL);
        }
    }

    bool row_group_matches_filter(parquet::Statistics *stats,
                                         arrow::DataType *    arrow_type,
                                         const RowGroupFilter& filter);

public:

    FilterPushdown(const int64_t numRowGroups)
    : rowGroupSkipList(numRowGroups, false)
    { }

    static Oid arrowTypeToPostgresType(int arrow_type)
    {
        switch (arrow_type)
        {
        case arrow::Type::BOOL:
            return BOOLOID;
        case arrow::Type::INT32:
            return INT4OID;
        case arrow::Type::INT64:
            return INT8OID;
        case arrow::Type::FLOAT:
            return FLOAT4OID;
        case arrow::Type::DOUBLE:
            return FLOAT8OID;
        case arrow::Type::STRING:
            return TEXTOID;
        case arrow::Type::BINARY:
            return BYTEAOID;
        case arrow::Type::TIMESTAMP:
            return TIMESTAMPOID;
        case arrow::Type::DATE32:
            return DATEOID;
        default:
            return InvalidOid;
        }
    }

    void extract_rowgroup_filters(List *scan_clauses);
    List* getRowGroupSkipListAndUpdateTupleCount(
        const ParquetFdwReader& reader,
        TupleDesc tupleDesc,
        const std::vector<bool>& attrUseList,
        uint64_t *numTotalRows,
        uint64_t *numRowsToRead,
        size_t *numPagesToRead) noexcept;
};
