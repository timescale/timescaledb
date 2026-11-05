/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

#include <postgres.h>
#include <catalog/pg_type.h>
#include <fmgr.h>

#include "compat/compat.h"
#include "create_table_with_clause.h"
#include "with_clause_parser.h"

static const WithClauseDefinition create_table_with_clauses_def[] = {
	[CreateTableFlagHypertable] = {.arg_names = {"hypertable", NULL}, .type_id = BOOLOID,},
	[CreateTableFlagColumnstore] = {.arg_names = {"columnstore", "enable_columnstore", "compress", NULL}, .type_id = BOOLOID, .default_val = (Datum)true,},
	[CreateTableFlagTimeColumn] = {.arg_names = {"partition_column", "partitioning_column", NULL}, .type_id = TEXTOID,},
	[CreateTableFlagChunkTimeInterval] = {.arg_names = {"chunk_interval", NULL}, .type_id = TEXTOID,},
	[CreateTableFlagCreateDefaultIndexes] = {.arg_names = {"create_default_indexes", NULL}, .type_id = BOOLOID, .default_val = (Datum) true,},
	[CreateTableFlagAssociatedSchema] = {.arg_names = {"associated_schema", NULL}, .type_id = TEXTOID,},
	[CreateTableFlagAssociatedTablePrefix] = {.arg_names = {"associated_table_prefix", NULL}, .type_id = TEXTOID,},
	[CreateTableFlagSegmentBy] = { .arg_names = {"segmentby", "segment_by", "compress_segmentby", NULL}, .type_id = TEXTOID,},
	[CreateTableFlagOrderBy] = { .arg_names = {"orderby", "order_by", "compress_orderby", NULL}, .type_id = TEXTOID,},
	[CreateTableFlagIndex] = { .arg_names = {"compress_index", "compress_sparse_index", "index", "sparse_index", NULL}, .type_id = TEXTOID,},
	[CreateTableFlagDirectCompress] = { .arg_names = {"direct_compress", NULL}, .type_id = BOOLOID, .default_val = (Datum)false,},
	[CreateTableFlagDirectCompressScheduleInterval] = { .arg_names = {"direct_compress_schedule_interval", NULL}, .type_id = INTERVALOID,},
	[CreateTableFlagCompressAfter] = { .arg_names = {"compress_after", NULL}, .type_id = TEXTOID,},
};

WithClauseResult *
ts_create_table_with_clause_parse(const List *defelems)
{
	return ts_with_clauses_parse(defelems,
								 create_table_with_clauses_def,
								 TS_ARRAY_LEN(create_table_with_clauses_def));
}

WithClauseResult *
ts_create_table_with_clause_overlay(const WithClauseResult *explicit_clauses,
									const WithClauseResult *policy_clauses)
{
	Size nargs = TS_ARRAY_LEN(create_table_with_clauses_def);
	WithClauseResult *result = palloc(sizeof(*result) * nargs);

	for (Size i = 0; i < nargs; i++)
		result[i] = (explicit_clauses != NULL && !explicit_clauses[i].is_default) ?
						explicit_clauses[i] :
						policy_clauses[i];

	return result;
}
