#pragma once

#if __cplusplus > 199711L
#define register      // Deprecated in C++11.
#endif  // #if __cplusplus > 199711L

extern "C" {
#include "access/attnum.h"
#include "catalog/pg_type_d.h"
#include "nodes/makefuncs.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/timestamp.h"
}

#include <list>

#define SEGMENT_SIZE (1024 * 1024)

#define ERROR_STR_LEN 512

#define to_postgres_timestamp(tstype, i, ts)                    \
    switch ((tstype)->unit()) {                                 \
        case arrow::TimeUnit::SECOND:                           \
            ts = time_t_to_timestamptz((i)); break;             \
        case arrow::TimeUnit::MILLI:                            \
            ts = time_t_to_timestamptz((i) / 1000); break;      \
        case arrow::TimeUnit::MICRO:                            \
            ts = time_t_to_timestamptz((i) / 1000000); break;   \
        case arrow::TimeUnit::NANO:                             \
            ts = time_t_to_timestamptz((i) / 1000000000); break;\
        default:                                                \
            elog(ERROR, "Timestamp of unknown precision: %d",   \
                 (tstype)->unit());                             \
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

void* exc_palloc(Size size);

static Oid
to_postgres_type(int arrow_type)
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
