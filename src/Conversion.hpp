#pragma once

static Oid to_postgres_type(int arrow_type, const bool isArrayType = false)
{
    if (isArrayType)
    {
        switch (arrow_type)
        {
        case arrow::Type::BOOL:
            return BOOLARRAYOID;
        case arrow::Type::INT32:
            return INT4ARRAYOID;
        case arrow::Type::INT64:
            return INT8ARRAYOID;
        case arrow::Type::FLOAT:
            return FLOAT4ARRAYOID;
        case arrow::Type::DOUBLE:
            return FLOAT8ARRAYOID;
        case arrow::Type::STRING:
            return TEXTARRAYOID;
        case arrow::Type::BINARY:
            return BYTEAARRAYOID;
        case arrow::Type::TIMESTAMP:
            return TIMESTAMPARRAYOID;
        case arrow::Type::DATE32:
            return DATEARRAYOID;
        default:
            return InvalidOid;
        }
    }
    else
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
}
