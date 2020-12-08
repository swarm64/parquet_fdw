
#include "ParquetFdwReader.hpp"
#include "Conversion.hpp"
#include "Helpers.hpp"

extern "C" {
#include "access/tupdesc.h"
#include "executor/tuptable.h"
#include "miscadmin.h"
#include "parser/parse_coerce.h"
#include "postgres.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/timestamp.h"
}

bool parquet_fdw_use_threads = true;

static char *tolowercase(const char *input, char *output)
{
    int i = 0;

    Assert(strlen(input) < 254);

    do
    {
        output[i] = tolower(input[i]);
    } while (input[i++]);

    return output;
}

void ParquetFdwReader::open(const char *   filename,
                            MemoryContext  cxt,
                            TupleDesc      tupleDesc,
                            std::set<int> &attrs_used,
                            bool           use_threads,
                            bool           use_mmap)
{
    parquet::ArrowReaderProperties              props;
    arrow::Status                               status;
    std::unique_ptr<parquet::arrow::FileReader> reader;

    status = parquet::arrow::FileReader::Make(
            arrow::default_memory_pool(), parquet::ParquetFileReader::OpenFile(filename, use_mmap),
            &reader);
    if (!status.ok())
        elog(ERROR, "Failed to open parquet file: %s", status.message().c_str());

    this->reader = std::move(reader);

    auto schema = this->reader->parquet_reader()->metadata()->schema();
    if (!parquet::arrow::FromParquetSchema(schema, props, &this->schema).ok())
        elog(ERROR, "Error reading parquet schema.");

    /* Enable parallel columns decoding/decompression if needed */
    this->reader->set_use_threads(use_threads && parquet_fdw_use_threads);

    /* Create mapping between tuple descriptor and parquet columns. */
    this->map.resize(tupleDesc->natts);
    for (int i = 0; i < tupleDesc->natts; i++)
    {
        AttrNumber attnum = i + 1 - FirstLowInvalidHeapAttributeNumber;
        char       pg_colname[255];

        this->map[i] = -1;

        /* Skip columns we don't intend to use in query */
        if (attrs_used.find(attnum) == attrs_used.end())
            continue;

        tolowercase(NameStr(TupleDescAttr(tupleDesc, i)->attname), pg_colname);

        for (int k = 0; k < schema->num_columns(); k++)
        {
            parquet::schema::NodePtr node = schema->Column(k)->schema_node();
            std::vector<std::string> path = node->path()->ToDotVector();
            char                     parquet_colname[255];

            tolowercase(path[0].c_str(), parquet_colname);

            /*
             * Compare postgres attribute name to the top level column name in
             * parquet.
             *
             * XXX If we will ever want to support structs then this should be
             * changed.
             */
            if (strcmp(pg_colname, parquet_colname) == 0)
            {
                PgTypeInfo typinfo;
                bool       error = false;

                /* Found mapping! */
                this->indices.push_back(k);

                /* index of last element */
                this->map[i] = this->indices.size() - 1;

                this->types.push_back(this->schema->field(k)->type().get());

                /* Find the element type in case the column type is array */
                PG_TRY();
                {
                    typinfo.oid       = TupleDescAttr(tupleDesc, i)->atttypid;
                    typinfo.elem_type = get_element_type(typinfo.oid);

                    if (OidIsValid(typinfo.elem_type))
                    {
                        get_typlenbyvalalign(typinfo.elem_type, &typinfo.elem_len,
                                             &typinfo.elem_byval, &typinfo.elem_align);
                    }
                }
                PG_CATCH();
                {
                    error = true;
                }
                PG_END_TRY();

                if (error)
                    elog(ERROR, "Failed to get the element type of column %s", pg_colname);

                this->pg_types.push_back(typinfo);

                break;
            }
        }
    }

    this->has_nulls = (bool *)exc_palloc(sizeof(bool) * this->map.size());
    this->allocator = new FastAllocator(cxt);
}

void ParquetFdwReader::prepareToReadRowGroup(const int32_t rowGroupId, TupleDesc tupleDesc)
{
    arrow::Status status;

    auto rowgroup_meta = this->reader->parquet_reader()->metadata()->RowGroup(rowGroupId);

    /* Determine which columns has null values */
    for (int arrow_col : this->map)
    {
        std::shared_ptr<parquet::Statistics> stats;
        if (arrow_col < 0)
            continue;

        stats = rowgroup_meta->ColumnChunk(this->indices[arrow_col])->statistics();

        if (stats)
            this->has_nulls[arrow_col] = (stats->null_count() > 0);
        else
            this->has_nulls[arrow_col] = true;
    }

    status = this->reader->RowGroup(rowGroupId)->ReadTable(this->indices, &this->table);

    if (!status.ok())
        elog(ERROR, "Error reading table: %s", status.message().c_str());

    if (!this->table)
        elog(ERROR, "Table is empty");

    /* TODO: don't clear each time */
    this->chunk_info.clear();
    this->chunks.clear();
    for (int i = 0; i < tupleDesc->natts; i++)
    {
        if (this->map[i] >= 0)
        {
            ChunkInfo chunkInfo = {.chunk = 0, .pos = 0, .len = 0};
            auto      column    = this->table->column(this->map[i]);

            this->chunk_info.push_back(chunkInfo);
            this->chunks.push_back(column->chunk(0).get());
        }
    }

    this->row_group = rowGroupId;
    this->row       = 0;
    this->num_rows  = this->table->num_rows();
}

bool ParquetFdwReader::next(TupleTableSlot *slot, bool fake)
{
    allocator->recycle();

    if (this->row == 0)
    {
        /* Lookup cast funcs */
        if (!this->initialized)
            this->initialize_castfuncs(slot->tts_tupleDescriptor);
    }
    /*
    if (this->row >= this->num_rows)
    {
        // Read next row group
        if (!this->read_next_rowgroup(slot->tts_tupleDescriptor))
            return false;

        // Lookup cast funcs
        if (!this->initialized)
            this->initialize_castfuncs(slot->tts_tupleDescriptor);
    }
    */

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
        int arrow_col = this->map[attr];
        /*
         * We only fill slot attributes if column was referred in targetlist
         * or clauses. In other cases mark attribute as NULL.
         * */
        if (arrow_col >= 0)
        {
            ChunkInfo &      chunkInfo     = this->chunk_info[arrow_col];
            arrow::Array *   array         = this->chunks[arrow_col];
            arrow::DataType *arrow_type    = this->types[arrow_col];
            int              arrow_type_id = arrow_type->id();
            PgTypeInfo *     pg_type       = &this->pg_types[arrow_col];

            chunkInfo.len = array->length();

            if (chunkInfo.pos >= chunkInfo.len)
            {
                auto column = this->table->column(arrow_col);

                /* There are no more chunks */
                if (++chunkInfo.chunk >= column->num_chunks())
                    break;

                array                   = column->chunk(chunkInfo.chunk).get();
                this->chunks[arrow_col] = array;
                chunkInfo.pos           = 0;
                chunkInfo.len           = array->length();
            }

            /* Don't do actual reading data into slot in fake mode */
            if (fake)
                continue;

            /* Currently only primitive types and lists are supported */
            if (arrow_type_id != arrow::Type::LIST)
            {
                const auto expectedPostgresType = to_postgres_type(arrow_type_id);
                if (expectedPostgresType != pg_type->oid)
                    elog(ERROR, "Type mismatch on column '%s'",
                         NameStr(slot->tts_tupleDescriptor->attrs[attr].attname));

                if (this->has_nulls[arrow_col] && array->IsNull(chunkInfo.pos))
                {
                    slot->tts_isnull[attr] = true;
                }
                else
                {
                    slot->tts_values[attr] = this->read_primitive_type(
                            array, arrow_type_id, chunkInfo.pos, this->castfuncs[attr]);
                    slot->tts_isnull[attr] = false;
                }
            }
            else
            {
                if (!OidIsValid(pg_type->elem_type))
                    elog(ERROR, "Could not convert parquet column of type LIST to scalar PG type");

                arrow_type_id = get_arrow_list_elem_type(arrow_type);

                const auto expectedPostgresType = to_postgres_arraytype(arrow_type_id);
                if (expectedPostgresType != pg_type->oid)
                    elog(ERROR, "Type mismatch on column '%s'",
                         NameStr(slot->tts_tupleDescriptor->attrs[attr].attname));

                int64             pos    = chunkInfo.pos;
                arrow::ListArray *larray = (arrow::ListArray *)array;

                if (this->has_nulls[arrow_col] && array->IsNull(pos))
                {
                    slot->tts_isnull[attr] = true;
                }
                else
                {
                    std::shared_ptr<arrow::Array> slice = larray->values()->Slice(
                            larray->value_offset(pos), larray->value_length(pos));

                    slot->tts_values[attr] = this->nested_list_get_datum(
                            slice.get(), arrow_type_id, pg_type, this->castfuncs[attr]);
                    slot->tts_isnull[attr] = false;
                }
            }

            chunkInfo.pos++;
        }
        else
        {
            slot->tts_isnull[attr] = true;
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
Datum ParquetFdwReader::read_primitive_type(arrow::Array *array,
                                            int           type_id,
                                            int64_t       i,
                                            FmgrInfo *    castfunc)
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
        elog(ERROR, "Unsupported column type: %d", type_id);
    }

    /* Call cast function if needed */
    if (castfunc != nullptr)
    {
        MemoryContext ccxt  = CurrentMemoryContext;
        bool          error = false;
        Datum         res;
        char          errstr[ERROR_STR_LEN];

        PG_TRY();
        {
            res = FunctionCall1(castfunc, res);
        }
        PG_CATCH();
        {
            ErrorData *errdata;

            MemoryContextSwitchTo(ccxt);
            error   = true;
            errdata = CopyErrorData();
            FlushErrorState();

            strncpy(errstr, errdata->message, ERROR_STR_LEN - 1);
            FreeErrorData(errdata);
        }
        PG_END_TRY();
        if (error)
            elog(ERROR, "%s", errstr);
    }

    return res;
}

/*
 * nested_list_get_datum
 *      Returns postgres array build from elements of array. Only one
 *      dimensional arrays are supported.
 */
Datum ParquetFdwReader::nested_list_get_datum(arrow::Array *array,
                                              int           arrow_type,
                                              PgTypeInfo *  pg_type,
                                              FmgrInfo *    castfunc)
{
    MemoryContext oldcxt;
    ArrayType *   res;
    Datum *       values;
    bool *        nulls = nullptr;
    int           dims[1];
    int           lbs[1];
    bool          error = false;

    values = (Datum *)this->allocator->fast_alloc(sizeof(Datum) * array->length());

#if SIZEOF_DATUM == 8
    /* Fill values and nulls arrays */
    if (array->null_count() == 0 && arrow_type == arrow::Type::INT64)
    {
        /*
         * Ok, there are no nulls, so probably we could just memcpy the
         * entire array.
         *
         * Warning: the code below is based on the assumption that Datum is
         * 8 bytes long, which is true for most contemporary systems but this
         * will not work on some exotic or really old systems.
         */
        copy_to_c_array<int64_t>((int64_t *)values, array, pg_type->elem_len);
        goto construct_array;
    }
#endif
    for (int64_t i = 0; i < array->length(); ++i)
    {
        if (!array->IsNull(i))
            values[i] = this->read_primitive_type(array, arrow_type, i, castfunc);
        else
        {
            if (!nulls)
            {
                Size size = sizeof(bool) * array->length();

                nulls = (bool *)this->allocator->fast_alloc(size);
                memset(nulls, 0, size);
            }
            nulls[i] = true;
        }
    }

construct_array:
    /*
     * Construct one dimensional array. We have to use PG_TRY / PG_CATCH
     * to prevent any kind leaks of resources allocated by c++ in case of
     * errors.
     */
    dims[0] = array->length();
    lbs[0]  = 1;
    PG_TRY();
    {
        oldcxt = MemoryContextSwitchTo(allocator->context());
        res = construct_md_array(values, nulls, 1, dims, lbs, pg_type->elem_type, pg_type->elem_len,
                                 pg_type->elem_byval, pg_type->elem_align);
        MemoryContextSwitchTo(oldcxt);
    }
    PG_CATCH();
    {
        error = true;
    }
    PG_END_TRY();
    if (error)
        elog(ERROR, "Failed to construct array");

    return PointerGetDatum(res);
}

/*
 * initialize_castfuncs
 *      Check wether implicit cast will be required and prepare cast function
 *      call. For arrays find cast functions for its elements.
 */
void ParquetFdwReader::initialize_castfuncs(TupleDesc tupleDesc)
{
    this->castfuncs.resize(this->map.size());

    for (uint i = 0; i < this->map.size(); ++i)
    {
        MemoryContext ccxt      = CurrentMemoryContext;
        int           arrow_col = this->map[i];
        bool          error     = false;
        char          errstr[ERROR_STR_LEN];

        if (this->map[i] < 0)
        {
            /* Null column */
            this->castfuncs[i] = nullptr;
            continue;
        }

        arrow::DataType *type    = this->types[arrow_col];
        int              type_id = type->id();
        int              src_type, dst_type;
        bool             src_is_list, dst_is_array;
        Oid              funcid;
        CoercionPathType ct;

        /* Find underlying type of list */
        src_is_list = (type_id == arrow::Type::LIST);
        if (src_is_list)
            type_id = get_arrow_list_elem_type(type);

        src_type = to_postgres_type(type_id);
        dst_type = TupleDescAttr(tupleDesc, i)->atttypid;

        if (!OidIsValid(src_type))
            elog(ERROR, "Unsupported column type: %s", type->name().c_str());

        /* Find underlying type of array */
        dst_is_array = type_is_array(dst_type);
        if (dst_is_array)
            dst_type = get_element_type(dst_type);

        /* Make sure both types are compatible */
        if (src_is_list != dst_is_array)
        {
            const auto column = this->table->field(arrow_col)->name().c_str();
            if (src_is_list)
                elog(ERROR, "Incompatible types in column %s: list vs scalar", column);
            else
                elog(ERROR, "Incompatible types in column %s: scalar vs array", column);
        }

        PG_TRY();
        {
            if (IsBinaryCoercible(src_type, dst_type))
            {
                this->castfuncs[i] = nullptr;
            }
            else
            {
                ct = find_coercion_pathway(dst_type, src_type, COERCION_EXPLICIT, &funcid);
                switch (ct)
                {
                case COERCION_PATH_FUNC:
                {
                    MemoryContext oldctx;

                    oldctx             = MemoryContextSwitchTo(CurTransactionContext);
                    this->castfuncs[i] = (FmgrInfo *)palloc0(sizeof(FmgrInfo));
                    fmgr_info(funcid, this->castfuncs[i]);
                    MemoryContextSwitchTo(oldctx);
                    break;
                }
                case COERCION_PATH_RELABELTYPE:
                case COERCION_PATH_COERCEVIAIO: /* TODO: double check that we
                                                 * shouldn't do anything here*/
                    /* Cast is not needed */
                    this->castfuncs[i] = nullptr;
                    break;
                default:
                    elog(ERROR, "cast function to %s ('%s' column) is not found",
                         format_type_be(dst_type), NameStr(TupleDescAttr(tupleDesc, i)->attname));
                }
            }
        }
        PG_CATCH();
        {
            ErrorData *errdata;

            MemoryContextSwitchTo(ccxt);
            error   = true;
            errdata = CopyErrorData();
            FlushErrorState();

            strncpy(errstr, errdata->message, ERROR_STR_LEN - 1);
            FreeErrorData(errdata);
        }
        PG_END_TRY();
        if (error)
            elog(ERROR, "%s", errstr);
    }
    this->initialized = true;
}

void ParquetFdwReader::set_rowgroups_list(const std::vector<int> &rowgroups)
{
    this->rowgroups = rowgroups;
}
