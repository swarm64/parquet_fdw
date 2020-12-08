#pragma once

using int16 = short;
using int32 = int;
using int64 = long;
using uint32 = unsigned int;

#if __cplusplus > 199711L
#define register      // Deprecated in C++11.
#endif  // #if __cplusplus > 199711L

extern "C" {
#include "postgres_ext.h"
#include "access/attnum.h"
#include "catalog/pg_type_d.h"
}

struct FmgrInfo;

#include <cstddef>
using Size = size_t;

#include "arrow/api.h"
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

void * exc_palloc(Size size);


