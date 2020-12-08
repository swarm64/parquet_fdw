#pragma once

static Oid to_postgres_type(int arrow_type) {
    switch (arrow_type) {
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

static Oid to_postgres_arraytype(int arrow_type) {
    switch (arrow_type) {
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

/*
static int to_arrow_type(Oid postgres_type) {
    switch (postgres_type) {
    case BOOLOID:
        return arrow::Type::BOOL;
    case INT4OID:
        return arrow::Type::INT32;
    case INT8OID:
        return arrow::Type::INT64;
    case FLOAT4OID:
        return arrow::Type::FLOAT;
    case FLOAT8OID:
        return arrow::Type::DOUBLE;
    case TEXTOID:
        return arrow::Type::STRING;
    case BYTEAOID:
        return arrow::Type::BINARY;
    case TIMESTAMPOID:
        return arrow::Type::TIMESTAMP;
    case DATEOID:
        return arrow::Type::DATE32;
    default:
        return arrow::Type::NA;
    }
}
*/
