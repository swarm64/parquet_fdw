#include "ParquetFdwReader.hpp"
#include "Error.hpp"
#include "PostgresWrappers.hpp"

extern "C" {
#include "postgres.h"

#include "utils/date.h"
#include "utils/timestamp.h"
}

ParquetFdwReader::ParquetFdwReader(const char* parquetFilePath)
: parquetFilePath(parquetFilePath)
{
    props.set_use_threads(false);

    const auto reader = getFileReader();

    numRowGroups = reader->num_row_groups();
    metadata = reader->parquet_reader()->metadata();

    if (!parquet::arrow::FromParquetSchema(metadata->schema(), props, &schema).ok())
        throw Error("Error reading parquet schema.");

    for (const auto& field : schema->fields()) {
        columnTypes.push_back(field->type()->id());
    }
}

std::unique_ptr<parquet::arrow::FileReader> ParquetFdwReader::getFileReader() const {
    std::unique_ptr<parquet::arrow::FileReader> reader;

    const bool mmap = true;
    const auto status = parquet::arrow::FileReader::Make(
            arrow::default_memory_pool(),
            parquet::ParquetFileReader::OpenFile(parquetFilePath, mmap),
            props,
            &reader);
    if (!status.ok())
        throw Error("Failed to open parquet file: %s", status.message().c_str());

    reader->set_use_threads(false);
    return reader;
}

void ParquetFdwReader::setMemoryContext(MemoryContext cxt) {
    allocator = std::make_unique<FastAllocator>(FastAllocator(cxt));
}

void ParquetFdwReader::bufferRowGroup(
    const int32_t rowGroupId, TupleDesc tupleDesc, const std::vector<bool>& attrUseList)
{
    arrow::Status status;

    const auto reader = getFileReader();
    auto rowgroup_meta = reader->parquet_reader()->metadata()->RowGroup(rowGroupId);

    columnChunks.clear();
    for (int numAttr = 0; numAttr < tupleDesc->natts; ++numAttr)
    {
        if (attrUseList[numAttr])
        {
            std::shared_ptr<arrow::ChunkedArray> columnChunk;
            const auto columnReader = reader->RowGroup(rowGroupId)->Column(numAttr);
            const auto status       = columnReader->Read(&columnChunk);
            if (!status.ok())
                throw Error("Could not read column %d in row group %d: %s", numAttr, rowGroupId,
                            status.message().c_str());

            // Not sure on this one, possibly needs to support multiple chunks
            if (columnChunk->num_chunks() > 1)
                throw Error("More than one chunk found.");

            columnChunks.emplace_back(ChunkInfo(columnChunk->chunk(0)));
        }
        else
            columnChunks.emplace_back(ChunkInfo());
    }

    if ((int)columnChunks.size() != tupleDesc->natts)
        throw Error("Amount of column chunks does not match number of attributes");

    this->row_group = rowGroupId;
    this->row       = 0;
    num_rows        = rowgroup_meta->num_rows();
}

bool ParquetFdwReader::next(TupleTableSlot *slot, bool fake)
{
    if (!allocator)
        throw Error("Allocator not set.");

    allocator->recycle();

    this->populate_slot(slot, fake);
    this->row++;

    return true;
}

/*
 * populate_slot
 *      Fill slot with the values from parquet row.
 *
 * If `fake` set to true the actual reading and populating the slot is skipped.
 * The purpose of this feature is to correctly skip rows to collect sparse
 * samples.
 */
void ParquetFdwReader::populate_slot(TupleTableSlot *slot, bool fake)
{
    std::memset(slot->tts_isnull, 1, sizeof(bool) * slot->tts_tupleDescriptor->natts);
    /* Fill slot values */
    for (int attr = 0; attr < slot->tts_tupleDescriptor->natts; attr++)
    {
        const auto& columnChunk = columnChunks[attr];
        if (columnChunk.array == nullptr || (columnChunk.hasNulls && columnChunk.array->IsNull(row)))
            continue;

        slot->tts_values[attr] = this->read_primitive_type(columnChunk.array, columnTypes[attr], row);
        slot->tts_isnull[attr] = false;
    }
}

void ParquetFdwReader::rescan()
{
    this->row_group = 0;
    this->row       = 0;
    this->num_rows  = 0;
}

/*
 * read_primitive_type
 *      Returns primitive type value from arrow array
 */
Datum ParquetFdwReader::read_primitive_type(const arrow::Array *array, int type_id, int64_t i)
{
    if (!allocator)
        throw Error("Allocator not set");

    Datum res;

    /* Get datum depending on the column type */
    switch (type_id)
    {
    case arrow::Type::BOOL:
    {
        arrow::BooleanArray *boolarray = (arrow::BooleanArray *)array;

        res = BoolGetDatum(boolarray->Value(i));
        break;
    }
    case arrow::Type::INT32:
    {
        arrow::Int32Array *intarray = (arrow::Int32Array *)array;
        int                value    = intarray->Value(i);

        res = Int32GetDatum(value);
        break;
    }
    case arrow::Type::INT64:
    {
        arrow::Int64Array *intarray = (arrow::Int64Array *)array;
        int64              value    = intarray->Value(i);

        res = Int64GetDatum(value);
        break;
    }
    case arrow::Type::FLOAT:
    {
        arrow::FloatArray *farray = (arrow::FloatArray *)array;
        float              value  = farray->Value(i);

        res = Float4GetDatum(value);
        break;
    }
    case arrow::Type::DOUBLE:
    {
        arrow::DoubleArray *darray = (arrow::DoubleArray *)array;
        double              value  = darray->Value(i);

        res = Float8GetDatum(value);
        break;
    }
    case arrow::Type::STRING:
    case arrow::Type::BINARY:
    {
        arrow::BinaryArray *binarray = (arrow::BinaryArray *)array;

        int32_t     vallen = 0;
        const char *value  = reinterpret_cast<const char *>(binarray->GetValue(i, &vallen));

        /* Build bytea */
        int64  bytea_len = vallen + VARHDRSZ;
        bytea *b         = (bytea *)this->allocator->fast_alloc(bytea_len);
        SET_VARSIZE(b, bytea_len);
        memcpy(VARDATA(b), value, vallen);

        res = PointerGetDatum(b);
        break;
    }
    case arrow::Type::TIMESTAMP:
    {
        /* TODO: deal with timezones */
        TimestampTz            ts;
        arrow::TimestampArray *tsarray = (arrow::TimestampArray *)array;
        auto                   tstype  = (arrow::TimestampType *)array->type().get();

        to_postgres_timestamp(tstype, tsarray->Value(i), ts);
        res = TimestampGetDatum(ts);
        break;
    }
    case arrow::Type::DATE32:
    {
        arrow::Date32Array *tsarray = (arrow::Date32Array *)array;
        int32               d       = tsarray->Value(i);

        /*
         * Postgres date starts with 2000-01-01 while unix date (which
         * Parquet is using) starts with 1970-01-01. So we need to do
         * simple calculations here.
         */
        res = DateADTGetDatum(d + (UNIX_EPOCH_JDATE - POSTGRES_EPOCH_JDATE));
        break;
    }
    /* TODO: add other types */
    default:
        throw Error("Unsupported column type: %d", type_id);
    }

    return res;
}

void ParquetFdwReader::validateSchema(TupleDesc tupleDesc) const
{
    // Validate column types
    for (int numAttr = 0; numAttr < tupleDesc->natts; ++numAttr)
    {
        const auto attr             = tupleDesc->attrs[numAttr];
        const auto expectedPgTypeId = attr.atttypid;

        const auto field       = schema->field(numAttr);
        const auto arrowTypeId = field->type()->id();
        const auto pgTypeId    = FilterPushdown::arrowTypeToPostgresType(arrowTypeId);

        if (pgTypeId == InvalidOid)
            throw Error("Type on column '%s' currently not supported.", NameStr(attr.attname));

        if (pgTypeId != expectedPgTypeId)
            throw Error("Type mismatch on column '%s'.", NameStr(attr.attname));
    }
}

void ParquetFdwReader::schemaMustBeEqual(const std::shared_ptr<arrow::Schema> otherSchema) const {
    if (!schema || !otherSchema)
        throw Error("Schemas not available for comparison");

    if (!schema->Equals(otherSchema))
        throw Error("Parquet schemas do not match.");
}
