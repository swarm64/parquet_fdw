#include "ParquetFdwReader.hpp"
#include "Error.hpp"
#include "PostgresWrappers.hpp"

extern "C" {
#include "postgres.h"

#include "utils/date.h"
#include "utils/timestamp.h"
}

bool parquet_fdw_use_threads = true;

void ParquetFdwReader::open(const char *             filename,
                            MemoryContext            cxt,
                            TupleDesc                tupleDesc,
                            const std::vector<bool> &attrUseList,
                            // std::set<int> &attrs_used,
                            bool use_threads,
                            bool use_mmap)
{
    parquet::ArrowReaderProperties              props;
    arrow::Status                               status;
    std::unique_ptr<parquet::arrow::FileReader> reader;

    status = parquet::arrow::FileReader::Make(
            arrow::default_memory_pool(), parquet::ParquetFileReader::OpenFile(filename, use_mmap),
            &reader);
    if (!status.ok())
        throw Error("Failed to open parquet file: %s", status.message().c_str());

    this->reader = std::move(reader);

    auto schema = this->reader->parquet_reader()->metadata()->schema();
    if (!parquet::arrow::FromParquetSchema(schema, props, &this->schema).ok())
        throw Error("Error reading parquet schema.");

    /* Enable parallel columns decoding/decompression if needed */
    this->reader->set_use_threads(use_threads && parquet_fdw_use_threads);

    for (int i = 0; i < tupleDesc->natts; i++)
    {
        columnTypes.push_back(this->schema->field(i)->type()->id());
    }

    std::copy(attrUseList.cbegin(), attrUseList.cend(), std::back_inserter(columnUseList));
    this->allocator = new FastAllocator(cxt);
}

void ParquetFdwReader::prepareToReadRowGroup(const int32_t rowGroupId, TupleDesc tupleDesc)
{
    arrow::Status status;

    auto rowgroup_meta = this->reader->parquet_reader()->metadata()->RowGroup(rowGroupId);

    columnChunks.clear();
    for (int columnIdx = 0; columnIdx < this->columnUseList.size(); columnIdx++)
    {
        if (columnUseList[columnIdx])
        {
            std::shared_ptr<arrow::ChunkedArray> columnChunk;
            const auto columnReader = reader->RowGroup(rowGroupId)->Column(columnIdx);
            const auto status       = columnReader->Read(&columnChunk);
            if (!status.ok())
                throw Error("Could not read column %d in row group %d: %s", columnIdx, rowGroupId,
                            status.message().c_str());

            // Not sure on this one, possibly needs to support multiple chunks
            if (columnChunk->num_chunks() > 1)
                throw Error("More than one chunk found.");

            const auto array = columnChunk->chunk(0);
            columnChunks.push_back(array);
        }
        else
        {
            columnChunks.push_back(nullptr);
        }
    }

    this->row_group = rowGroupId;
    this->row       = 0;
    num_rows        = rowgroup_meta->num_rows();
}

bool ParquetFdwReader::next(TupleTableSlot *slot, bool fake)
{
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
    /* Fill slot values */
    for (int attr = 0; attr < slot->tts_tupleDescriptor->natts; attr++)
    {
        const auto columnChunk = columnChunks[attr];
        if (columnChunk == nullptr)
            slot->tts_isnull[attr] = true;
        else
        {
            if (columnChunk->IsNull(row))
                slot->tts_isnull[attr] = true;
            else
            {
                slot->tts_values[attr] =
                        this->read_primitive_type(columnChunk.get(), columnTypes[attr], row);
                slot->tts_isnull[attr] = false;
            }
        }
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
Datum ParquetFdwReader::read_primitive_type(arrow::Array *array, int type_id, int64_t i)
{
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

void ParquetFdwReader::set_rowgroups_list(const std::vector<int> &rowgroups)
{
    this->rowgroups = rowgroups;
}
