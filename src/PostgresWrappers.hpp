#pragma once

extern "C" {
#include "postgres.h"
#include "utils/array.h"
}

#include "PostgresErrors.hpp"

inline void DeconstructArray(ArrayType *array,
                             Oid        elmtype,
                             int        elmlen,
                             bool       elmbyval,
                             char       elmalign,
                             Datum **   elemsp,
                             bool **    nullsp,
                             int *      nelemsp)
{
    return CatchAndRethrow([&]() {
        deconstruct_array(array, elmtype, elmlen, elmbyval, elmalign, elemsp, nullsp, nelemsp);
    });
}

inline char *TextToCString(const text *t)
{
    return CatchAndRethrow([&]() { return text_to_cstring(t); });
}
