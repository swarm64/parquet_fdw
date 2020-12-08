#pragma once

#include "arrow/api.h"

extern "C" {
#include "postgres.h"
}

static int get_arrow_list_elem_type(arrow::DataType *type)
{
    auto children = type->fields();

    Assert(children.size() == 1);
    return children[0]->type()->id();
}
