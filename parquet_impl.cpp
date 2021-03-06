/*
 * Parquet processing implementation
 */
// basename comes from string.h on Linux,
// but from libgen.h on other POSIX systems (see man basename)
#ifndef GNU_SOURCE
#    include <libgen.h>
#endif

#if __cplusplus > 199711L
#    define register // Deprecated in C++11.
#endif               // #if __cplusplus > 199711L

#include <sys/stat.h>

#include <filesystem>
#include <list>
#include <mutex>
#include <set>

#include "arrow/api.h"
#include "arrow/array.h"
#include "arrow/io/api.h"
#include "parquet/arrow/reader.h"
#include "parquet/arrow/schema.h"
#include "parquet/exception.h"
#include "parquet/file_reader.h"
#include "parquet/statistics.h"

extern "C" {
#include "postgres.h"

#include "access/htup_details.h"
#include "access/nbtree.h"
#include "access/parallel.h"
#include "access/reloptions.h"
#include "access/sysattr.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "executor/spi.h"
#include "executor/tuptable.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "miscadmin.h"
#include "nodes/execnodes.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/cost.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "parser/parse_coerce.h"
#include "parser/parse_func.h"
#include "parser/parse_oper.h"
#include "parser/parse_type.h"
#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/jsonb.h"
#include "utils/lsyscache.h"
#include "utils/memdebug.h"
#include "utils/memutils.h"
#include "utils/regproc.h"
#include "utils/rel.h"
#include "utils/timestamp.h"
#include "utils/typcache.h"

#if PG_VERSION_NUM < 120000
#    include "nodes/relation.h"
#    include "optimizer/var.h"
#else
#    include "access/relation.h"
#    include "access/table.h"
#    include "optimizer/optimizer.h"
#endif
}

#include "src/FilterPushdown.hpp"
#include "src/ParquetFdwExecutionState.hpp"
#include "src/ParquetFdwReader.hpp"
#include "src/functions/ConvertCsvToParquet.hpp"
#include "src/functions/Filesystem.hpp"

/* from costsize.c */
#define LOG2(x) (log(x) / 0.693147180559945)

#if PG_VERSION_NUM < 110000
#    define PG_GETARG_JSONB_P PG_GETARG_JSONB
#endif

static void  destroy_parquet_state(void *arg);

/*
 * Plain C struct for fdw_state
 */
struct ParquetFdwPlanState
{
    List *     filenames;
    List *     attrs_sorted;
    Bitmapset *attrs_used; // attributes actually used in query
    bool       use_mmap;
    uint64_t numTotalRows;
    uint64_t numRowsToRead;
    size_t numPagesToRead;
    List * rowGroupsToSkip;
};

typedef enum {
    FDW_PLAN_STATE_FILENAMES = 0,
    FDW_PLAN_STATE_ATTRS_USED,
    FDW_PLAN_STATE_ATTRS_SORTED,
    FDW_PLAN_STATE_USE_MMAP,
    FDW_PLAN_STATE_ROW_GROUPS_TO_SKIP,
    FDW_PLAN_STATE_END__
} FdwPlanStatePack;

typedef enum
{
    PS_START = 0,
    PS_IDENT,
    PS_QUOTE
} ParserState;

/*
 * parse_filenames_list
 *      Parse space separated list of filenames.
 */
static List *parse_filenames_list(const char *str)
{
    char *      cur       = pstrdup(str);
    char *      f         = cur;
    ParserState state     = PS_START;
    List *      filenames = NIL;

    while (*cur)
    {
        switch (state)
        {
        case PS_START:
            switch (*cur)
            {
            case ' ':
                /* just skip */
                break;
            case '"':
                f     = cur + 1;
                state = PS_QUOTE;
                break;
            default:
                /* XXX we should check that *cur is a valid path symbol
                 * but let's skip it for now */
                state = PS_IDENT;
                f     = cur;
                break;
            }
            break;
        case PS_IDENT:
            switch (*cur)
            {
            case ' ':
                *cur      = '\0';
                filenames = lappend(filenames, makeString(f));
                state     = PS_START;
                break;
            default:
                break;
            }
            break;
        case PS_QUOTE:
            switch (*cur)
            {
            case '"':
                *cur      = '\0';
                filenames = lappend(filenames, makeString(f));
                state     = PS_START;
                break;
            default:
                break;
            }
            break;
        default:
            elog(ERROR, "parquet_fdw: unknown parse state");
        }
        cur++;
    }
    filenames = lappend(filenames, makeString(f));

    return filenames;
}

static List* getFilesToRead(const char* filenamesListString) {
    ListCell* lc;
    List* fileNamesList = parse_filenames_list(filenamesListString);
    List* fileNames = NIL;

    try {
        foreach (lc, fileNamesList)
        {
            char *filename = strVal((Value *)lfirst(lc));
            const auto path = std::filesystem::path(filename);

            if (!std::filesystem::exists(path))
            {
                elog(ERROR, "parquet_fdw: No such file or directory");
            }
            else if (std::filesystem::is_directory(path))
            {
                for (auto &entry : std::filesystem::recursive_directory_iterator(path))
                {
                    const auto currentPath = entry.path();
                    if (std::filesystem::is_regular_file(currentPath))
                    {
                        elog(DEBUG1, "Appending file %s", currentPath.c_str());
                        char* thisFile = pstrdup(currentPath.c_str());
                        fileNames = lappend(fileNames, makeString(thisFile));
                    }
                }
            }
            else if (std::filesystem::is_regular_file(path))
            {
                elog(DEBUG1, "Appending file %s", path.c_str());
                char* thisFile = pstrdup(path.c_str());
                fileNames = lappend(fileNames, makeString(thisFile));
            }
            else
            {
                elog(ERROR, "File %s is neither regular file nor a directory. Skipping.", filename);
            }
        }
    }
    catch (std::exception &e)
    {
        elog(ERROR, "parquet_fdw exception: %s", e.what());
    }

    return fileNames;
}


struct FieldInfo
{
    char name[NAMEDATALEN];
    Oid  oid;
};

/*
 * extract_parquet_fields
 *      Read parquet file and return a list of its fields
 */
static List *extract_parquet_fields(const char *path) noexcept
{
    List *                                      res = NIL;

    try
    {
        auto reader = std::make_unique<ParquetFdwReader>(path);
        const auto schema = reader->GetSchema();
        reader.reset();
        const auto numFields = schema->num_fields();

        FieldInfo *fields = (FieldInfo *)exc_palloc(sizeof(FieldInfo) * numFields);
        for (int k = 0; k < numFields; ++k)
        {
            std::shared_ptr<arrow::Field>    field;
            std::shared_ptr<arrow::DataType> type;
            Oid                              pg_type;

            /* Convert to arrow field to get appropriate type */
            field = schema->field(k);
            type  = field->type();

            pg_type = FilterPushdown::arrowTypeToPostgresType(type->id());
            if (pg_type == InvalidOid)
                elog(ERROR, "Cannot convert field %s of type %s in %s", field->name().c_str(),
                     type->name().c_str(), path);

            if (field->name().length() > 63)
                elog(ERROR, "Field name %s in %s is too long", field->name().c_str(), path);

            memcpy(fields->name, field->name().c_str(), field->name().length() + 1);
            fields->oid = pg_type;
            res         = lappend(res, fields++);
        }
    }
    catch (std::exception &e)
    {
        elog(ERROR, "parquet_fdw: %s", e.what());
    }

    return res;
}

/*
 * create_foreign_table_query
 *      Produce a query text for creating a new foreign table.
 */
char *create_foreign_table_query(const char *tablename,
                                 const char *schemaname,
                                 const char *servername,
                                 char **     paths,
                                 int         npaths,
                                 List *      fields,
                                 List *      options)
{
    StringInfoData str;
    ListCell *     lc;

    initStringInfo(&str);
    appendStringInfo(&str, "CREATE FOREIGN TABLE ");

    /* append table name */
    if (schemaname)
        appendStringInfo(&str, "%s.%s (", schemaname, quote_identifier(tablename));
    else
        appendStringInfo(&str, "%s (", quote_identifier(tablename));

    /* append columns */
    bool is_first = true;
    foreach (lc, fields)
    {
        FieldInfo * field     = (FieldInfo *)lfirst(lc);
        char *      name      = field->name;
        Oid         pg_type   = field->oid;
        const char *type_name = format_type_be(pg_type);

        if (!is_first)
            appendStringInfo(&str, ", %s %s", name, type_name);
        else
        {
            appendStringInfo(&str, "%s %s", name, type_name);
            is_first = false;
        }
    }
    appendStringInfo(&str, ") SERVER %s ", servername);
    appendStringInfo(&str, "OPTIONS (filename '");

    /* list paths */
    is_first = true;
    for (int i = 0; i < npaths; ++i)
    {
        if (!is_first)
            appendStringInfoChar(&str, ' ');
        else
            is_first = false;

        appendStringInfoString(&str, paths[i]);
    }
    appendStringInfoChar(&str, '\'');

    /* list options */
    foreach (lc, options)
    {
        DefElem *def = (DefElem *)lfirst(lc);

        appendStringInfo(&str, ", %s '%s'", def->defname, defGetString(def));
    }

    appendStringInfo(&str, ")");

    return str.data;
}

static void destroy_parquet_state(void *arg)
{
    ParquetFdwExecutionState *festate = (ParquetFdwExecutionState *)arg;

    if (festate)
        delete festate;
}

/*
 * OidFunctionCall1NullableArg
 *      Practically a copy-paste from FunctionCall1Coll with added capability
 *      of passing a NULL argument.
 */
static Datum OidFunctionCall1NullableArg(Oid functionId, Datum arg, bool argisnull)
{
#if PG_VERSION_NUM < 120000
    FunctionCallInfoData  _fcinfo;
    FunctionCallInfoData *fcinfo = &_fcinfo;
#else
    LOCAL_FCINFO(fcinfo, 1);
#endif
    FmgrInfo flinfo;
    Datum    result;

    fmgr_info(functionId, &flinfo);
    InitFunctionCallInfoData(*fcinfo, &flinfo, 1, InvalidOid, NULL, NULL);

#if PG_VERSION_NUM < 120000
    fcinfo->arg[0]     = arg;
    fcinfo->argnull[0] = false;
#else
    fcinfo->args[0].value  = arg;
    fcinfo->args[0].isnull = argisnull;
#endif

    result = FunctionCallInvoke(fcinfo);

    /* Check for null result, since caller is clearly not expecting one */
    if (fcinfo->isnull)
        elog(ERROR, "function %u returned NULL", flinfo.fn_oid);

    return result;
}

static List *get_filenames_from_userfunc(const char *funcname, const char *funcarg)
{
    Jsonb *    j = nullptr;
    Oid        funcid;
    List *     f = stringToQualifiedNameList(funcname);
    Datum      filenames;
    Oid        jsonboid = JSONBOID;
    Datum *    values;
    bool *     nulls;
    int        num;
    List *     res = NIL;
    ArrayType *arr;

    if (funcarg)
    {
#if PG_VERSION_NUM < 110000
        j = DatumGetJsonb(DirectFunctionCall1(jsonb_in, CStringGetDatum(funcarg)));
#else
        j = DatumGetJsonbP(DirectFunctionCall1(jsonb_in, CStringGetDatum(funcarg)));
#endif
    }

    funcid    = LookupFuncName(f, 1, &jsonboid, false);
    filenames = OidFunctionCall1NullableArg(funcid, (Datum)j, funcarg == nullptr);

    arr = DatumGetArrayTypeP(filenames);
    if (ARR_ELEMTYPE(arr) != TEXTOID)
        elog(ERROR, "function returned an array with non-TEXT element type");

    deconstruct_array(arr, TEXTOID, -1, false, 'i', &values, &nulls, &num);

    if (num == 0)
    {
        elog(WARNING, "'%s' function returned an empty array; foreign table wasn't created",
             get_func_name(funcid));
        return NIL;
    }

    for (int i = 0; i < num; ++i)
    {
        if (nulls[i])
            elog(ERROR, "user function returned an array containing NULL value(s)");
        res = lappend(res, makeString(TextDatumGetCString(values[i])));
    }

    return res;
}

static void get_table_options(Oid relid, ParquetFdwPlanState *fdw_private)
{
    ForeignTable *table;
    ListCell *    lc;
    char *        funcname = nullptr;
    char *        funcarg  = nullptr;

    if (!fdw_private)
        elog(ERROR, "FDW plan state not provided.");

    fdw_private->use_mmap    = false;
    table                    = GetForeignTable(relid);

    foreach (lc, table->options)
    {
        DefElem *def = (DefElem *)lfirst(lc);

        if (strcmp(def->defname, "filename") == 0)
        {
            fdw_private->filenames = getFilesToRead(defGetString(def));
        }
        else if (strcmp(def->defname, "files_func") == 0)
        {
            funcname = defGetString(def);
        }
        else if (strcmp(def->defname, "files_func_arg") == 0)
        {
            funcarg = defGetString(def);
        }
        else if (strcmp(def->defname, "use_mmap") == 0)
        {
            if (!parse_bool(defGetString(def), &fdw_private->use_mmap))
                ereport(ERROR,
                        (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                         errmsg("invalid value for boolean option \"%s\": %s", def->defname,
                                defGetString(def))));
        }
        else
            elog(ERROR, "unknown option '%s'", def->defname);
    }

    if (funcname)
        fdw_private->filenames = get_filenames_from_userfunc(funcname, funcarg);
}

static void extract_used_attributes(RelOptInfo *baserel)
{
    ParquetFdwPlanState *fdw_private = (ParquetFdwPlanState *)baserel->fdw_private;
    ListCell *           lc;

    pull_varattnos((Node *)baserel->reltarget->exprs, baserel->relid, &fdw_private->attrs_used);

    foreach (lc, baserel->baserestrictinfo)
    {
        RestrictInfo *rinfo = (RestrictInfo *)lfirst(lc);

        pull_varattnos((Node *)rinfo->clause, baserel->relid, &fdw_private->attrs_used);
    }

    if (bms_is_empty(fdw_private->attrs_used))
    {
        bms_free(fdw_private->attrs_used);
        fdw_private->attrs_used = bms_make_singleton(1 - FirstLowInvalidHeapAttributeNumber);
    }
}

extern "C" void parquetGetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid)
{
    ParquetFdwPlanState *fdw_private = (ParquetFdwPlanState *)palloc0(sizeof(ParquetFdwPlanState));
    fdw_private->rowGroupsToSkip = NIL;

    get_table_options(foreigntableid, fdw_private);
    baserel->fdw_private = fdw_private;

    /* Collect used attributes to reduce number of read columns during scan */
    extract_used_attributes(baserel);

    RangeTblEntry *rte = root->simple_rte_array[baserel->relid];
#if PG_VERSION_NUM < 120000
    Relation rel = heap_open(rte->relid, AccessShareLock);
#else
    Relation rel = table_open(rte->relid, AccessShareLock);
#endif

    TupleDesc tupleDesc = RelationGetDescr(rel);

#if PG_VERSION_NUM < 120000
    heap_close(rel, AccessShareLock);
#else
    table_close(rel, AccessShareLock);
#endif

    try
    {
        ListCell *lc, *lc2;

        auto attrUseList = std::vector<bool>(tupleDesc->natts, false);
        int attIdx = -1;
        while ((attIdx = bms_next_member(fdw_private->attrs_used, attIdx)) >= 0)
        {
            const auto attNum   = attIdx - 1 + FirstLowInvalidHeapAttributeNumber;
            attrUseList[attNum] = true;
        }

        std::shared_ptr<arrow::Schema> previousSchema;
        List* allFiles = list_copy(fdw_private->filenames);
        foreach (lc, allFiles)
        {
            char *     filename = strVal((Value *)lfirst(lc));
            auto reader = std::make_unique<ParquetFdwReader>(filename);

            if (previousSchema)
                reader->schemaMustBeEqual(previousSchema);
            else
                reader->validateSchema(tupleDesc);

            FilterPushdown filterPushdown(reader->getNumRowGroups());
            filterPushdown.extract_rowgroup_filters(baserel->baserestrictinfo);

            /*
             * Extract list of row groups that match query clauses. Also calculate
             * approximate number of rows in result set based on total number of tuples
             * in those row groups. It isn't very precise but it is best we got.
             */
            List *thisFileSkipList = filterPushdown.getRowGroupSkipListAndUpdateTupleCount(
                filename,
                tupleDesc,
                attrUseList,
                &(fdw_private->numTotalRows),
                &(fdw_private->numRowsToRead),
                &(fdw_private->numPagesToRead));

            if (thisFileSkipList != NIL && reader->getNumRowGroups() == (size_t)thisFileSkipList->length)
            {
                elog(DEBUG1, "parquet_fdw: skipping file %s", filename);
                fdw_private->filenames = list_delete_ptr(fdw_private->filenames, lfirst(lc));
            }
            else
            {
                foreach(lc2, thisFileSkipList) {
                    elog(DEBUG1, "parquet_fdw: skipping rowgroup %d of file %s", lfirst_int(lc2), filename);
                }
                fdw_private->rowGroupsToSkip = lappend(fdw_private->rowGroupsToSkip, thisFileSkipList);
            }

            previousSchema = reader->GetSchema();
            reader.reset();
        }
    }
    catch (std::exception &e)
    {
        elog(ERROR, "parquet_fdw: error validating schema: %s", e.what());
    }

    baserel->rows = fdw_private->numRowsToRead;
    baserel->tuples = fdw_private->numTotalRows;
}

static Path* constructPath(PlannerInfo *root, RelOptInfo *baserel, const double parallelDivisor)
{
    auto fdw_private = (ParquetFdwPlanState *)baserel->fdw_private;


    const Cost startupCost = 0.0;
    const Cost cpuRunCost = (cpu_tuple_cost * fdw_private->numRowsToRead) / parallelDivisor;
    const Cost diskRunCost = seq_page_cost * fdw_private->numPagesToRead;

    const Cost parallelCostScaler = parallelDivisor > 1.0 ? 1.0 : 2.0;
    const Cost totalCost = startupCost + cpuRunCost * parallelCostScaler + diskRunCost;

    return reinterpret_cast<Path*>(
        create_foreignscan_path(
            root,
            baserel,
            nullptr, // default pathtarget
            fdw_private->numRowsToRead / parallelDivisor,
            startupCost,
            totalCost,
            nullptr, // no pathkeys
            nullptr, // no outer rel either
            nullptr, // no extra plan
            (List *)fdw_private));
}

extern "C" void parquetGetForeignPaths(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid)
{
    ParquetFdwPlanState *fdw_private;
    List *                    pathkeys = NIL;

    fdw_private = (ParquetFdwPlanState *)baserel->fdw_private;

    /* Build pathkeys based on attrs_sorted */
    ListCell *lc;
    foreach (lc, fdw_private->attrs_sorted)
    {
        Oid   relid  = root->simple_rte_array[baserel->relid]->relid;
        int   attnum = lfirst_int(lc);
        Oid   typid, collid;
        int32 typmod;
        Oid   sort_op;
        Var * var;
        List *attr_pathkey;

        /* Build an expression (simple var) */
        get_atttypetypmodcoll(relid, attnum, &typid, &typmod, &collid);
        var = makeVar(baserel->relid, attnum, typid, typmod, collid, 0);

        /* Lookup sorting operator for the attribute type */
        get_sort_group_operators(typid, true, false, false, &sort_op, nullptr, nullptr, nullptr);

        attr_pathkey = build_expression_pathkey(root, (Expr *)var, nullptr, sort_op,
                                                baserel->relids, true);
        pathkeys     = list_concat(pathkeys, attr_pathkey);
    }

    Path *foreignPath = constructPath(root, baserel, 1.0);
    add_path(baserel, foreignPath);

    if (baserel->consider_parallel > 0)
    {
        const int numWorkers = parallel_leader_participation
                             ? max_parallel_workers_per_gather + 1
                             : max_parallel_workers_per_gather;

        const double parallelDivisor = static_cast<double>(numWorkers);

        Path* parallelPath = constructPath(root, baserel, parallelDivisor);

        parallelPath->parallel_workers = numWorkers;
        parallelPath->parallel_aware = true;
        parallelPath->parallel_safe = true;

        add_partial_path(baserel, parallelPath);
    }
}

extern "C" ForeignScan *parquetGetForeignPlan(PlannerInfo *root,
                                              RelOptInfo * baserel,
                                              Oid          foreigntableid,
                                              ForeignPath *best_path,
                                              List *       tlist,
                                              List *       scan_clauses,
                                              Plan *       outer_plan)
{
    ParquetFdwPlanState *fdw_private  = (ParquetFdwPlanState *)best_path->fdw_private;
    Index                scan_relid   = baserel->relid;
    List *               attrs_used   = NIL;
    List *               attrs_sorted = NIL;
    AttrNumber           attr;
    List *               params = NIL;
    ListCell *           lc;

    /*
     * We have no native ability to evaluate restriction clauses, so we just
     * put all the scan_clauses into the plan node's qual list for the
     * executor to check.  So all we have to do here is strip RestrictInfo
     * nodes from the clauses and ignore pseudoconstants (which will be
     * handled elsewhere).
     */
    scan_clauses = extract_actual_clauses(scan_clauses, false);

    /*
     * We can't just pass arbitrary structure into make_foreignscan() because
     * in some cases (i.e. plan caching) postgres may want to make a copy of
     * the plan and it can only make copy of something it knows of, namely
     * Nodes. So we need to convert everything in nodes and store it in a List.
     */
    attr = -1;
    while ((attr = bms_next_member(fdw_private->attrs_used, attr)) >= 0)
        attrs_used = lappend_int(attrs_used, attr);

    foreach (lc, fdw_private->attrs_sorted)
        attrs_sorted = lappend_int(attrs_sorted, lfirst_int(lc));

    /* Packing all the data needed by executor into the list */
    for (int item = 0; item < FDW_PLAN_STATE_END__; ++item) {
        switch (item) {
            case FDW_PLAN_STATE_FILENAMES:
                params = lappend(params, fdw_private->filenames);
                break;

            case FDW_PLAN_STATE_ATTRS_USED:
                params = lappend(params, attrs_used);
                break;

            case FDW_PLAN_STATE_ATTRS_SORTED:
                params = lappend(params, attrs_sorted);
                break;

            case FDW_PLAN_STATE_USE_MMAP:
                params = lappend(params, makeInteger(fdw_private->use_mmap));
                break;

            case FDW_PLAN_STATE_ROW_GROUPS_TO_SKIP:
                params = lappend(params, fdw_private->rowGroupsToSkip);
                break;

            default:
                elog(ERROR, "FDW plan state item missing: %d", item);
        }
    }

    /* Create the ForeignScan node */
    return make_foreignscan(tlist, scan_clauses, scan_relid, NIL, /* no expressions to evaluate */
                            params, NIL,                          /* no custom tlist */
                            NIL,                                  /* no remote quals */
                            outer_plan);
}

extern "C" void parquetBeginForeignScan(ForeignScanState *node, int eflags)
{
    ParquetFdwExecutionState *festate;
    MemoryContextCallback *   callback;
    MemoryContext             reader_cxt;
    ForeignScan *             plan        = (ForeignScan *)node->ss.ps.plan;
    EState *                  estate      = node->ss.ps.state;
    List *                    fdw_private = plan->fdw_private;
    List *                    attrs_list;
    ListCell *                lc, *lc2;
    List *                    filenames    = NIL;
    List *                    attrs_sorted = NIL;
    bool                      use_mmap     = false;
    int                       i            = 0;
    List* rowGroupsToSkip = NIL;

    TupleTableSlot *slot        = node->ss.ss_ScanTupleSlot;
    TupleDesc       tupleDesc   = slot->tts_tupleDescriptor;
    auto            attrUseList = std::vector<bool>(tupleDesc->natts, false);

    /* Unwrap fdw_private */
    foreach (lc, fdw_private)
    {
        switch (i)
        {
        case FDW_PLAN_STATE_FILENAMES:
            filenames = (List *)lfirst(lc);
            break;

        case FDW_PLAN_STATE_ATTRS_USED:
            attrs_list = (List *)lfirst(lc);
            foreach (lc2, attrs_list)
            {
                const auto idx      = lfirst_int(lc2);
                const auto attNum   = idx - 1 + FirstLowInvalidHeapAttributeNumber;
                attrUseList[attNum] = true;
            }
            break;

        case FDW_PLAN_STATE_ATTRS_SORTED:
            attrs_sorted = (List *)lfirst(lc);
            break;

        case FDW_PLAN_STATE_USE_MMAP:
            use_mmap = (bool)intVal((Value *)lfirst(lc));
            break;

        case FDW_PLAN_STATE_ROW_GROUPS_TO_SKIP:
            rowGroupsToSkip = (List*)lfirst(lc);
            break;

        case FDW_PLAN_STATE_END__:
            break;

        default:
            elog(ERROR, "Unknown FDW plan state item number: %d", i);
        }
        ++i;
    }

    MemoryContext cxt = estate->es_query_cxt;
    reader_cxt = AllocSetContextCreate(cxt, "parquet_fdw tuple data", ALLOCSET_DEFAULT_SIZES);

    std::list<SortSupportData> sort_keys;
    foreach (lc, attrs_sorted)
    {
        SortSupportData sort_key;
        int             attr = lfirst_int(lc);
        Oid             typid;
        int             typmod;
        Oid             collid;
        Oid             relid = RelationGetRelid(node->ss.ss_currentRelation);
        Oid             sort_op;

        memset(&sort_key, 0, sizeof(SortSupportData));

        get_atttypetypmodcoll(relid, attr, &typid, &typmod, &collid);

        sort_key.ssup_cxt         = reader_cxt;
        sort_key.ssup_collation   = collid;
        sort_key.ssup_nulls_first = true;
        sort_key.ssup_attno       = attr;
        sort_key.abbreviate       = false;

        get_sort_group_operators(typid, true, false, false, &sort_op, nullptr, nullptr, nullptr);

        PrepareSortSupportFromOrderingOp(sort_op, &sort_key);

        try
        {
            sort_keys.push_back(sort_key);
        }
        catch (std::exception &e)
        {
            elog(ERROR, "parquet_fdw: scan initialization failed: %s", e.what());
        }
    }

    festate = new ParquetFdwExecutionState(reader_cxt, tupleDesc, attrUseList, use_mmap);

    if (filenames) {
        if (!rowGroupsToSkip)
            elog(ERROR, "parquet_fdw: got a null skiplist");

        if (filenames->length != rowGroupsToSkip->length)
            elog(ERROR, "Filenames count does not match skiplist count.");

        forboth(lc, filenames, lc2, rowGroupsToSkip)
        {
            char *filename  = strVal((Value *)lfirst(lc));
            List* skipList = (List*)lfirst(lc2);

            try
            {
                festate->addFileToRead(filename, reader_cxt, skipList);
            }
            catch (std::exception &e)
            {
                elog(ERROR, "parquet_fdw: %s", e.what());
            }
        }
    }

    /*
     * Enable automatic execution state destruction by using memory context
     * callback
     */
    callback       = (MemoryContextCallback *)palloc(sizeof(MemoryContextCallback));
    callback->func = destroy_parquet_state;
    callback->arg  = (void *)festate;
    MemoryContextRegisterResetCallback(reader_cxt, callback);

    node->fdw_state = festate;
}

extern "C" TupleTableSlot *parquetIterateForeignScan(ForeignScanState *node)
{
    ParquetFdwExecutionState *festate = (ParquetFdwExecutionState *)node->fdw_state;
    TupleTableSlot *          slot    = node->ss.ss_ScanTupleSlot;

    ExecClearTuple(slot);
    try
    {
        festate->next(slot);
    }
    catch (std::exception &e)
    {
        elog(ERROR, "parquet_fdw: %s", e.what());
    }

    return slot;
}

extern "C" void parquetEndForeignScan(ForeignScanState *node)
{
    /*
     * Destruction of execution state is done by memory context callback. See
     * destroy_parquet_state()
     */
}

extern "C" void parquetReScanForeignScan(ForeignScanState *node)
{
    ParquetFdwExecutionState *festate = (ParquetFdwExecutionState *)node->fdw_state;

    try
    {
        festate->rescan();
    }
    catch (std::exception &e)
    {
        elog(ERROR, "parquet_fdw: %s", e.what());
    }
}

static int parquetAcquireSampleRowsFunc(Relation   relation,
                                        int        elevel,
                                        HeapTuple *rows,
                                        int        targrows,
                                        double *   totalrows,
                                        double *   totaldeadrows)
{
    MemoryContext             reader_cxt;
    TupleDesc                 tupleDesc = RelationGetDescr(relation);
    TupleTableSlot *          slot;
    int       cnt      = 0;
    uint64    num_rows = 0;
    ListCell *lc;

    ParquetFdwPlanState newPlanState;
    get_table_options(RelationGetRelid(relation), &newPlanState);
    ParquetFdwPlanState *fdw_private = &newPlanState;

    const List* filenames = fdw_private->filenames;
    if (!filenames)
        elog(ERROR, "List of filenames is empty");

    const auto attrUseList = std::vector<bool>(tupleDesc->natts, true);

    reader_cxt = AllocSetContextCreate(CurrentMemoryContext, "parquet_fdw tuple data",
                                       ALLOCSET_DEFAULT_SIZES);
    const auto festate = new ParquetFdwExecutionState(reader_cxt, tupleDesc, attrUseList, false);

    try
    {
        std::shared_ptr<arrow::Schema> previousSchema;
        foreach (lc, filenames)
        {
            char *filename = strVal((Value *)lfirst(lc));
            auto reader = std::make_unique<ParquetFdwReader>(filename);
            num_rows += reader->getNumTotalRows();

            if (previousSchema)
                reader->schemaMustBeEqual(previousSchema);
            else
                reader->validateSchema(tupleDesc);

            previousSchema = reader->GetSchema();
            festate->addFileToRead(filename, reader_cxt, nullptr);
            reader.reset();
        }
    }
    catch (const std::exception &e)
    {
        elog(ERROR, "parquet_fdw: %s", e.what());
    }

    PG_TRY();
    {
        int    ratio = num_rows / targrows;

        /* Set ratio to at least 1 to avoid devision by zero issue */
        ratio = ratio < 1 ? 1 : ratio;

#if PG_VERSION_NUM < 120000
        slot = MakeSingleTupleTableSlot(tupleDesc);
#else
        slot = MakeSingleTupleTableSlot(tupleDesc, &TTSOpsHeapTuple);
#endif

        while (true)
        {
            CHECK_FOR_INTERRUPTS();

            if (cnt >= targrows)
                break;

            ExecClearTuple(slot);
            if (!festate->next(slot, false))
                break;

            rows[cnt++] = heap_form_tuple(tupleDesc, slot->tts_values, slot->tts_isnull);
        }

        *totalrows     = num_rows;
        *totaldeadrows = 0;

        ExecDropSingleTupleTableSlot(slot);
    }
    PG_CATCH();
    {
        elog(LOG, "Cancelled");
        delete festate;
        PG_RE_THROW();
    }
    PG_END_TRY();

    delete festate;

    return cnt - 1;
}

extern "C" bool parquetAnalyzeForeignTable(Relation               relation,
                                           AcquireSampleRowsFunc *func,
                                           BlockNumber *          totalpages)
{
    ParquetFdwPlanState newPlanState;
    get_table_options(RelationGetRelid(relation), &newPlanState);
    ParquetFdwPlanState *fdw_private = &newPlanState;

    const List* filenames = fdw_private->filenames;
    if (!filenames)
        elog(ERROR, "List of filenames is empty");

    ListCell *lc;
    size_t totalByteSize = 0;
    try
    {
        foreach (lc, filenames)
        {
            char *filename = strVal((Value *)lfirst(lc));
            auto reader = std::make_unique<ParquetFdwReader>(filename);
            totalByteSize += reader->getTotalByteSize();
            reader.reset();
        }
    }
    catch (const std::exception &e)
    {
        elog(ERROR, "parquet_fdw: %s", e.what());
    }

    *totalpages = totalByteSize / BLCKSZ;
    *func = parquetAcquireSampleRowsFunc;
    return true;
}

/*
 * parquetExplainForeignScan
 *      Additional explain information, namely row groups list.
 */
extern "C" void parquetExplainForeignScan(ForeignScanState *node, ExplainState *es)
{
    List *         fdw_private;
    ListCell *     lc, *lc2, *lc3;
    StringInfoData str;
    List *         filenames;
    List *         rowgroups_list;

    initStringInfo(&str);

    fdw_private    = ((ForeignScan *)node->ss.ps.plan)->fdw_private;
    filenames      = (List *)linitial(fdw_private);
    rowgroups_list = (List *)llast(fdw_private);

    ExplainPropertyText("Reader", "Multifile", es);

    forboth(lc, filenames, lc2, rowgroups_list)
    {
        char *filename  = strVal((Value *)lfirst(lc));
        List *rowgroups = (List *)lfirst(lc2);
        bool  is_first  = true;

        /* Only print filename if there're more than one file */
        if (list_length(filenames) > 1)
        {
            appendStringInfoChar(&str, '\n');
            appendStringInfoSpaces(&str, (es->indent + 1) * 2);

#ifdef _GNU_SOURCE
            appendStringInfo(&str, "%s: ", basename(filename));
#else
            appendStringInfo(&str, "%s: ", basename(pstrdup(filename)));
#endif
        }

        if (rowgroups) {
            foreach (lc3, rowgroups)
            {
                /*
                 * As parquet-tools use 1 based indexing for row groups it's probably
                 * a good idea to output row groups numbers in the same way.
                 */
                int rowgroup = lfirst_int(lc3) + 1;

                if (is_first)
                {
                    appendStringInfo(&str, "%i", rowgroup);
                    is_first = false;
                }
                else
                    appendStringInfo(&str, ", %i", rowgroup);
            }
        } else {
            appendStringInfo(&str, "none");
        }
    }

    ExplainPropertyText("Skipped row groups", str.data, es);
}

/* Parallel query execution */

extern "C" bool parquetIsForeignScanParallelSafe(PlannerInfo *root, RelOptInfo *rel, RangeTblEntry *rte)
{
    return true;
}

extern "C" Size parquetEstimateDSMForeignScan(ForeignScanState *node, ParallelContext *pcxt)
{
    return sizeof(ReadCoordinator);
}

extern "C" void parquetInitializeDSMForeignScan(ForeignScanState *node,
                                                ParallelContext * pcxt,
                                                void *            coordinate)
{
    ReadCoordinator *         coord = (ReadCoordinator *)coordinate;
    ParquetFdwExecutionState *festate;

    festate = (ParquetFdwExecutionState *)node->fdw_state;
    festate->set_coordinator(coord);
}

extern "C" void parquetReInitializeDSMForeignScan(ForeignScanState *node,
                                                  ParallelContext * pcxt,
                                                  void *            coordinate)
{
    ReadCoordinator *coord = (ReadCoordinator *)coordinate;
    coord->reset();
}

extern "C" void
        parquetInitializeWorkerForeignScan(ForeignScanState *node, shm_toc *toc, void *coordinate)
{
    ReadCoordinator *         coord = (ReadCoordinator *)coordinate;
    ParquetFdwExecutionState *festate;

    festate = (ParquetFdwExecutionState *)node->fdw_state;
    festate->set_coordinator(coord);
}

extern "C" void parquetShutdownForeignScan(ForeignScanState *node)
{
}

extern "C" List *parquetImportForeignSchema(ImportForeignSchemaStmt *stmt, Oid serverOid)
{
    List *cmds = NIL;

    try {
        const auto parquetFiles = SchemaBuilder::GetParquetFiles(stmt->remote_schema);
        for (const auto& parquetFile : parquetFiles) {
            char* fullPath = const_cast<char*>(parquetFile.native().c_str());
            const std::string tableName = parquetFile.filename().stem();

            ListCell *lc;
            bool      skip = false;
            List *    fields;
            char *    query;
            foreach (lc, stmt->table_list)
            {
                RangeVar *rv = (RangeVar *)lfirst(lc);

                switch (stmt->list_type)
                {
                case FDW_IMPORT_SCHEMA_LIMIT_TO:
                    if (strcmp(tableName.c_str(), rv->relname) != 0)
                    {
                        skip = true;
                        break;
                    }
                    break;
                case FDW_IMPORT_SCHEMA_EXCEPT:
                    if (strcmp(tableName.c_str(), rv->relname) == 0)
                    {
                        skip = true;
                        break;
                    }
                    break;
                default:;
                }
            }

            if (skip)
                continue;

            fields = extract_parquet_fields(fullPath);
            query  = create_foreign_table_query(tableName.c_str(), stmt->local_schema, stmt->server_name,
                                               &fullPath, 1, fields, stmt->options);
            cmds   = lappend(cmds, query);
        }
    } catch (std::exception& exc) {
        elog(ERROR, "parquet_fdw: %s", exc.what());
    }

    return cmds;
}

extern "C" Datum parquet_fdw_validator_impl(PG_FUNCTION_ARGS)
{
    List *    options_list = untransformRelOptions(PG_GETARG_DATUM(0));
    Oid       catalog      = PG_GETARG_OID(1);
    ListCell *lc;
    bool      filename_provided = false;
    bool      func_provided     = false;

    /* Only check table options */
    if (catalog != ForeignTableRelationId)
        PG_RETURN_VOID();

    foreach (lc, options_list)
    {
        DefElem *def = (DefElem *)lfirst(lc);
        if (!def)
            elog(ERROR, "Option list element not defined");

        if (strcmp(def->defname, "filename") == 0)
        {
            char *    filename = pstrdup(defGetString(def));
            List *    filenames;
            ListCell *lc;

            filenames = getFilesToRead(filename);

            foreach (lc, filenames)
            {
                struct stat stat_buf;
                char *      fn = strVal((Value *)lfirst(lc));

                if (stat(fn, &stat_buf) != 0)
                {
                    int e = errno;

                    ereport(ERROR,
                            (errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
                             errmsg("parquet_fdw: %s", strerror(e))));
                }
            }
            pfree(filenames);
            pfree(filename);
            filename_provided = true;
        }
        else if (strcmp(def->defname, "files_func") == 0)
        {
            Oid   jsonboid = JSONBOID;
            List *funcname = stringToQualifiedNameList(defGetString(def));
            Oid   funcoid;
            Oid   rettype;

            /*
             * Lookup the function with a single JSONB argument and fail
             * if there isn't one.
             */
            funcoid = LookupFuncName(funcname, 1, &jsonboid, false);
            if ((rettype = get_func_rettype(funcoid)) != TEXTARRAYOID)
            {
                elog(ERROR, "return type of '%s' is %s; expected text[]", defGetString(def),
                     format_type_be(rettype));
            }
            func_provided = true;
        }
        else if (strcmp(def->defname, "files_func_arg") == 0)
        {
            /*
             * Try to convert the string value into JSONB to validate it is
             * properly formatted.
             */
            DirectFunctionCall1(jsonb_in, CStringGetDatum(defGetString(def)));
        }
        else if (strcmp(def->defname, "sorted") == 0)
            ; /* do nothing */
        else if (strcmp(def->defname, "batch_size") == 0)
            /* check that int value is valid */
            strtol(defGetString(def), nullptr, 10);
        else if (strcmp(def->defname, "use_mmap") == 0)
        {
            /* Check that bool value is valid */
            bool use_mmap;

            if (!parse_bool(defGetString(def), &use_mmap))
                ereport(ERROR,
                        (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                         errmsg("invalid value for boolean option \"%s\": %s", def->defname,
                                defGetString(def))));
        }
        else
        {
            ereport(ERROR,
                    (errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
                     errmsg("parquet_fdw: invalid option \"%s\"", def->defname)));
        }
    }

    if (!filename_provided && !func_provided)
        elog(ERROR, "parquet_fdw: filename or function is required");

    PG_RETURN_VOID();
}

static List *jsonb_to_options_list(Jsonb *options)
{
    List *             res = NIL;
    JsonbIterator *    it;
    JsonbValue         v;
    JsonbIteratorToken type = WJB_DONE;

    if (!options)
        return NIL;

    if (!JsonContainerIsObject(&options->root))
        elog(ERROR, "options must be represented by a jsonb object");

    it = JsonbIteratorInit(&options->root);
    while ((type = JsonbIteratorNext(&it, &v, false)) != WJB_DONE)
    {
        switch (type)
        {
        case WJB_BEGIN_OBJECT:
        case WJB_END_OBJECT:
            break;
        case WJB_KEY:
        {
            DefElem *elem;
            char *   key;
            char *   val;

            if (v.type != jbvString)
                elog(ERROR, "expected a string key");
            key = pnstrdup(v.val.string.val, v.val.string.len);

            /* read value directly after key */
            type = JsonbIteratorNext(&it, &v, false);
            if (type != WJB_VALUE || v.type != jbvString)
                elog(ERROR, "expected a string value");
            val = pnstrdup(v.val.string.val, v.val.string.len);

            elem = makeDefElem(key, (Node *)makeString(val), 0);
            res  = lappend(res, elem);

            break;
        }
        default:
            elog(ERROR, "wrong options format");
        }
    }

    return res;
}

static void validate_import_args(const char *tablename, const char *servername, Oid funcoid)
{
    if (!tablename)
        elog(ERROR, "foreign table name is mandatory");

    if (!servername)
        elog(ERROR, "foreign server name is mandatory");

    if (!OidIsValid(funcoid))
        elog(ERROR, "function must be specified");
}

static void import_parquet_internal(const char *tablename,
                                    const char *schemaname,
                                    const char *servername,
                                    List *      fields,
                                    Oid         funcid,
                                    Jsonb *     arg,
                                    Jsonb *     options) noexcept
{
    Datum      res;
    FmgrInfo   finfo;
    ArrayType *arr;
    Oid        ret_type;
    List *     optlist;
    char *     query;

    validate_import_args(tablename, servername, funcid);

    if ((ret_type = get_func_rettype(funcid)) != TEXTARRAYOID)
    {
        elog(ERROR, "return type of '%s' function is %s; expected text[]", get_func_name(funcid),
             format_type_be(ret_type));
    }

    optlist = jsonb_to_options_list(options);

    /* Call the user provided function */
    fmgr_info(funcid, &finfo);
    res = FunctionCall1(&finfo, (Datum)arg);

    /*
     * In case function returns NULL the ERROR is thrown. So it's safe to
     * assume function returned something. Just for the sake of readability
     * I leave this condition
     */
    if (res != (Datum)0)
    {
        Datum *values;
        bool * nulls;
        int    num;
        int    ret;

        arr = DatumGetArrayTypeP(res);
        deconstruct_array(arr, TEXTOID, -1, false, 'i', &values, &nulls, &num);

        if (num == 0)
        {
            elog(WARNING, "'%s' function returned an empty array; foreign table wasn't created",
                 get_func_name(funcid));
            return;
        }

        /* Convert values to cstring array */
        char **paths = (char **)palloc(num * sizeof(char *));
        for (int i = 0; i < num; ++i)
        {
            if (nulls[i])
                elog(ERROR, "user function returned an array containing NULL value(s)");
            paths[i] = text_to_cstring(DatumGetTextP(values[i]));
        }

        /*
         * If attributes list is provided then use it. Otherwise get the list
         * from the first file provided by the user function. We trust the user
         * to provide a list of files with the same structure.
         */
        fields = fields ? fields : extract_parquet_fields(paths[0]);

        query = create_foreign_table_query(tablename, schemaname, servername, paths, num, fields,
                                           optlist);

        /* Execute query */
        if (SPI_connect() < 0)
            elog(ERROR, "parquet_fdw: SPI_connect failed");

        if ((ret = SPI_exec(query, 0)) != SPI_OK_UTILITY)
            elog(ERROR, "parquet_fdw: failed to create table '%s': %s", tablename,
                 SPI_result_code_string(ret));

        SPI_finish();
    }
}

extern "C" {

PG_FUNCTION_INFO_V1(import_parquet);
Datum import_parquet(PG_FUNCTION_ARGS)
{
    char * tablename;
    char * schemaname;
    char * servername;
    Oid    funcid;
    Jsonb *arg;
    Jsonb *options;

    tablename  = PG_ARGISNULL(0) ? nullptr : text_to_cstring(PG_GETARG_TEXT_P(0));
    schemaname = PG_ARGISNULL(1) ? nullptr : text_to_cstring(PG_GETARG_TEXT_P(1));
    servername = PG_ARGISNULL(2) ? nullptr : text_to_cstring(PG_GETARG_TEXT_P(2));
    funcid     = PG_ARGISNULL(3) ? InvalidOid : PG_GETARG_OID(3);
    arg        = PG_ARGISNULL(4) ? nullptr : PG_GETARG_JSONB_P(4);
    options    = PG_ARGISNULL(5) ? nullptr : PG_GETARG_JSONB_P(5);

    import_parquet_internal(tablename, schemaname, servername, nullptr, funcid, arg, options);

    PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(convert_csv_to_parquet);
Datum convert_csv_to_parquet(PG_FUNCTION_ARGS)
{
    char *     src_filepath;
    char *     target_filepath;
    ArrayType *field_names;
    char *     compression_type;

    src_filepath     = PG_ARGISNULL(0) ? nullptr : text_to_cstring(PG_GETARG_TEXT_P(0));
    target_filepath  = PG_ARGISNULL(1) ? nullptr : text_to_cstring(PG_GETARG_TEXT_P(1));
    field_names      = PG_ARGISNULL(2) ? nullptr : PG_GETARG_ARRAYTYPE_P(2);
    compression_type = PG_ARGISNULL(3) ? nullptr : text_to_cstring(PG_GETARG_TEXT_P(3));

    try
    {
        const int64_t numRows = ConvertCsvToParquet().convert(src_filepath, target_filepath,
                                                              compression_type, field_names);
        PG_RETURN_INT64(numRows);
    }
    catch (std::exception &e)
    {
        elog(ERROR, "converting csv to parquet failed: %s", e.what());
    }

    PG_RETURN_VOID();
}
}
