/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

#include <postgres.h>

#include <catalog/pg_type.h>
#include <fmgr.h>
#include <nodes/makefuncs.h>

#include "compat/compat.h"

#include "alter_table_with_clause.h"
#include "create_materialized_view_with_clause.h"
#include "cross_module_fn.h"
#include "ts_catalog/continuous_agg.h"
#include "with_clause_parser.h"

static const WithClauseDefinition continuous_aggregate_with_clause_def[] = {
    [CreateMaterializedViewFlagContinuous] = {
        .arg_names = {"continuous", NULL},
        .type_id = BOOLOID,
        .default_val = (Datum)false,
    },
    [CreateMaterializedViewFlagCreateGroupIndexes] = {
        .arg_names = {"create_group_indexes", NULL},
        .type_id = BOOLOID,
        .default_val = (Datum)true,
    },
    [CreateMaterializedViewFlagMaterializedOnly] = {
        .arg_names = {"materialized_only", NULL},
        .type_id = BOOLOID,
        .default_val = (Datum)true,
    },
    [CreateMaterializedViewFlagCompress] = {
        .arg_names = {"columnstore", "enable_columnstore", "compress", NULL},
        .type_id = BOOLOID,
    },
    [CreateMaterializedViewFlagFinalized] = {
        .arg_names = {"finalized", NULL},
        .type_id = BOOLOID,
        .default_val = (Datum)true,
    },
    [CreateMaterializedViewFlagChunkTimeInterval] = {
        .arg_names = {"chunk_time_interval", NULL},
         .type_id = INTERVALOID,
    },
    [CreateMaterializedViewFlagCompressSegmentBy] = {
        .arg_names = {"segmentby", "compress_segmentby", NULL},
        .type_id = TEXTOID,
    },
    [CreateMaterializedViewFlagCompressOrderBy] = {
        .arg_names = {"orderby", "compress_orderby", NULL},
         .type_id = TEXTOID,
    },
    [CreateMaterializedViewFlagCompressChunkTimeInterval] = {
        .arg_names = {"compress_chunk_time_interval", NULL},
         .type_id = INTERVALOID,
    },
};

WithClauseResult *
ts_create_materialized_view_with_clause_parse(const List *defelems)
{
	return ts_with_clauses_parse(defelems,
								 continuous_aggregate_with_clause_def,
								 TS_ARRAY_LEN(continuous_aggregate_with_clause_def));
}

List *
ts_continuous_agg_get_compression_defelems(const WithClauseResult *with_clauses)
{
	List *ret = NIL;

	for (int i = 0; i < AlterTableFlagsMax; i++)
	{
		int option_index = 0;
		switch (i)
		{
			case AlterTableFlagChunkTimeInterval:
				continue;
				break;
			case AlterTableFlagCompressEnabled:
				option_index = CreateMaterializedViewFlagCompress;
				break;
			case AlterTableFlagCompressSegmentBy:
				option_index = CreateMaterializedViewFlagCompressSegmentBy;
				break;
			case AlterTableFlagCompressOrderBy:
				option_index = CreateMaterializedViewFlagCompressOrderBy;
				break;
			case AlterTableFlagCompressChunkTimeInterval:
				option_index = CreateMaterializedViewFlagCompressChunkTimeInterval;
				break;
			default:
				elog(ERROR, "Unhandled compression option");
				break;
		}

		const WithClauseResult *input = &with_clauses[option_index];
		WithClauseDefinition def = continuous_aggregate_with_clause_def[option_index];

		if (!input->is_default)
		{
			Node *value = (Node *) makeString(ts_with_clause_result_deparse_value(input));
			DefElem *elem = makeDefElemExtended(EXTENSION_NAMESPACE,
												(char *) def.arg_names[0],
												value,
												DEFELEM_UNSPEC,
												-1);
			ret = lappend(ret, elem);
		}
	}
	return ret;
}
