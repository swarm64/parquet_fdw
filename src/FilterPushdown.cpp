
#include "FilterPushdown.hpp"

#include "parquet/arrow/reader.h"
#include "parquet/arrow/schema.h"

extern "C" {
#include "access/nbtree.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/pathnodes.h"
#include "utils/lsyscache.h"
#include "utils/typcache.h"
}

/*
 * find_cmp_func
 *      Find comparison function for two given types.
 */
void FilterPushdown::find_cmp_func(FmgrInfo *finfo, Oid type1, Oid type2)
{
    Oid             cmp_proc_oid;
    TypeCacheEntry *tce_1, *tce_2;

    tce_1 = lookup_type_cache(type1, TYPECACHE_BTREE_OPFAMILY);
    tce_2 = lookup_type_cache(type2, TYPECACHE_BTREE_OPFAMILY);

    cmp_proc_oid = get_opfamily_proc(tce_1->btree_opf, tce_1->btree_opintype, tce_2->btree_opintype,
                                     BTORDER_PROC);
    fmgr_info(cmp_proc_oid, finfo);
}

/*
 * row_group_matches_filter
 *      Check if min/max values of the column of the row group match filter.
 */
bool FilterPushdown::row_group_matches_filter(parquet::Statistics *stats, arrow::DataType *arrow_type,
    const RowGroupFilter& filter)
{
    FmgrInfo finfo;
    Datum    val      = filter.value->constvalue;
    int      collid   = filter.value->constcollid;
    int      strategy = filter.strategy;

    const auto postgresType = arrowTypeToPostgresType(arrow_type->id());
    find_cmp_func(&finfo, filter.value->consttype, postgresType);

    switch (filter.strategy)
    {
    case BTLessStrategyNumber:
    case BTLessEqualStrategyNumber:
    {
        Datum lower;
        int   cmpres;
        bool  satisfies;

        lower  = bytes_to_postgres_type(stats->EncodeMin().c_str(), arrow_type);
        cmpres = FunctionCall2Coll(&finfo, collid, val, lower);

        satisfies = (strategy == BTLessStrategyNumber && cmpres > 0)
                || (strategy == BTLessEqualStrategyNumber && cmpres >= 0);

        if (!satisfies)
            return false;
        break;
    }

    case BTGreaterStrategyNumber:
    case BTGreaterEqualStrategyNumber:
    {
        Datum upper;
        int   cmpres;
        bool  satisfies;

        upper  = bytes_to_postgres_type(stats->EncodeMax().c_str(), arrow_type);
        cmpres = FunctionCall2Coll(&finfo, collid, val, upper);

        satisfies = (strategy == BTGreaterStrategyNumber && cmpres < 0)
                || (strategy == BTGreaterEqualStrategyNumber && cmpres <= 0);

        if (!satisfies)
            return false;
        break;
    }

    case BTEqualStrategyNumber:
    {
        Datum lower, upper;

        lower = bytes_to_postgres_type(stats->EncodeMin().c_str(), arrow_type);
        upper = bytes_to_postgres_type(stats->EncodeMax().c_str(), arrow_type);

        int l = FunctionCall2Coll(&finfo, collid, val, lower);
        int u = FunctionCall2Coll(&finfo, collid, val, upper);

        if (l < 0 || u > 0)
            return false;
        break;
    }

    default:
        return true;
    }

    return true;
}

/*
 * extract_rowgroup_filters
 *      Build a list of expressions we can use to filter out row groups.
 */
void FilterPushdown::extract_rowgroup_filters(List *scan_clauses)
{
    ListCell *lc;

    foreach (lc, scan_clauses)
    {
        TypeCacheEntry *tce;
        Expr *          clause = (Expr *)lfirst(lc);
        OpExpr *        expr;
        Expr *          left, *right;
        int             strategy;
        Const *         c;
        Var *           v;
        Oid             opno;

        if (IsA(clause, RestrictInfo))
            clause = ((RestrictInfo *)clause)->clause;

        if (IsA(clause, OpExpr))
        {
            expr = (OpExpr *)clause;

            /* Only interested in binary opexprs */
            if (list_length(expr->args) != 2)
                continue;

            left  = (Expr *)linitial(expr->args);
            right = (Expr *)lsecond(expr->args);

            /*
             * Looking for expressions like "EXPR OP CONST" or "CONST OP EXPR"
             *
             * XXX Currently only Var as expression is supported. Will be
             * extended in future.
             */
            if (IsA(right, Const))
            {
                if (!IsA(left, Var))
                    continue;
                v    = (Var *)left;
                c    = (Const *)right;
                opno = expr->opno;
            }
            else if (IsA(left, Const))
            {
                /* reverse order (CONST OP VAR) */
                if (!IsA(right, Var))
                    continue;
                v    = (Var *)right;
                c    = (Const *)left;
                opno = get_commutator(expr->opno);
            }
            else
                continue;

            /* TODO */
            tce      = lookup_type_cache(exprType((Node *)left), TYPECACHE_BTREE_OPFAMILY);
            strategy = get_op_opfamily_strategy(opno, tce->btree_opf);

            /* Not a btree family operator? */
            if (strategy == 0)
                continue;
        }
        else if (IsA(clause, Var))
        {
            /*
             * Trivial expression containing only a single boolean Var. This
             * also covers cases "BOOL_VAR = true"
             * */
            v        = (Var *)clause;
            strategy = BTEqualStrategyNumber;
            c        = (Const *)makeBoolConst(true, false);
        }
        else if (IsA(clause, BoolExpr))
        {
            /*
             * Similar to previous case but for expressions like "!BOOL_VAR" or
             * "BOOL_VAR = false"
             */
            BoolExpr *boolExpr = (BoolExpr *)clause;

            if (boolExpr->args && list_length(boolExpr->args) != 1)
                continue;

            if (!IsA(linitial(boolExpr->args), Var))
                continue;

            v        = (Var *)linitial(boolExpr->args);
            strategy = BTEqualStrategyNumber;
            c        = (Const *)makeBoolConst(false, false);
        }
        else
            continue;

        filters.push_back({ v->varattno, c, strategy });
    }
}

/*
 * extract_rowgroups_list
 *      Analyze query predicates and using min/max statistics determine which
 *      row groups satisfy clauses. Store resulting row group list to
 *      fdw_private.
 */
List* FilterPushdown::getRowGroupSkipListAndUpdateTupleCount(const ParquetFdwReader& reader,
                             TupleDesc                  tupleDesc,
                             uint64_t *ntuples
                             ) noexcept
{
    List* rowGroupSkipList = NIL;

    /* Check each row group whether it matches the filters */
    const int32_t numRowGroups = reader.getNumRowGroups();
    for (int r = 0; r < numRowGroups; r++)
    {
        bool skipRowGroup = false;
        const auto rowgroup = reader.getRowGroup(r);
        for (auto &filter : filters)
        {
            AttrNumber  attnum;
            const char *attname;

            attnum  = filter.attnum - 1;
            attname = NameStr(TupleDescAttr(tupleDesc, attnum)->attname);

            // Attnum == column name since schema is identical
            const auto column = rowgroup->ColumnChunk(attnum);
            const auto columnName = column->path_in_schema()->ToDotString();
            if (strcmp(attname, columnName.c_str()))
                Error("Attribute name does not match column name");

            const auto stats = column->statistics();
            if (!stats)
                continue;

            const auto type = reader.GetSchema()->field(attnum)->type();
            skipRowGroup = !row_group_matches_filter(stats.get(), type.get(), filter);
            if (skipRowGroup) {
                rowGroupSkipList = lappend_int(rowGroupSkipList, r);
                break;
            }
        }

        if (!skipRowGroup)
            *ntuples += rowgroup->num_rows();
    }

    return rowGroupSkipList;
}
