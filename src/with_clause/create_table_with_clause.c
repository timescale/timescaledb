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
	[CreateTableFlagOrigin] = { .arg_names = { "chunk_origin", "partition_origin", "partitioning_origin", "origin", NULL}, .type_id = TEXTOID,},
};

WithClauseResult *
ts_create_table_with_clause_parse(const List *defelems)
{
	return ts_with_clauses_parse(defelems,
								 create_table_with_clauses_def,
								 TS_ARRAY_LEN(create_table_with_clauses_def));
}

Datum
ts_create_table_parse_chunk_time_interval(WithClauseResult option, Oid column_type,
										  Oid *interval_type)
{
	if (option.is_default == false)
	{
		Datum textarg = option.parsed;
		switch (column_type)
		{
			case INT2OID:
			{
				*interval_type = INT2OID;
				return DirectFunctionCall1(int2in, CStringGetDatum(TextDatumGetCString(textarg)));
			}
			case INT4OID:
			{
				*interval_type = INT4OID;
				return DirectFunctionCall1(int4in, CStringGetDatum(TextDatumGetCString(textarg)));
			}
			case INT8OID:
			{
				*interval_type = INT8OID;
				return DirectFunctionCall1(int8in, CStringGetDatum(TextDatumGetCString(textarg)));
			}
			case TIMESTAMPOID:
			case TIMESTAMPTZOID:
			case DATEOID:
			case UUIDOID:
			{
				*interval_type = INTERVALOID;
				return DirectFunctionCall3(interval_in,
										   CStringGetDatum(TextDatumGetCString(textarg)),
										   InvalidOid,
										   -1);
			}
		}
	}
	*interval_type = InvalidOid;
	return UnassignedDatum;
}

Datum
ts_create_table_parse_origin(WithClauseResult option, Oid column_type, Oid *origin_type)
{
	if (option.is_default == false)
	{
		Datum textarg = option.parsed;
		switch (column_type)
		{
			case INT2OID:
			{
				*origin_type = INT2OID;
				return DirectFunctionCall1(int2in, CStringGetDatum(TextDatumGetCString(textarg)));
			}
			case INT4OID:
			{
				*origin_type = INT4OID;
				return DirectFunctionCall1(int4in, CStringGetDatum(TextDatumGetCString(textarg)));
			}
			case INT8OID:
			{
				*origin_type = INT8OID;
				return DirectFunctionCall1(int8in, CStringGetDatum(TextDatumGetCString(textarg)));
			}
			case TIMESTAMPOID:
			{
				*origin_type = TIMESTAMPOID;
				return DirectFunctionCall3(timestamp_in,
										   CStringGetDatum(TextDatumGetCString(textarg)),
										   InvalidOid,
										   -1);
			}
			case TIMESTAMPTZOID:
			case UUIDOID:
			{
				*origin_type = TIMESTAMPTZOID;
				return DirectFunctionCall3(timestamptz_in,
										   CStringGetDatum(TextDatumGetCString(textarg)),
										   InvalidOid,
										   -1);
			}
			case DATEOID:
			{
				*origin_type = DATEOID;
				return DirectFunctionCall1(date_in, CStringGetDatum(TextDatumGetCString(textarg)));
			}
		}
	}
	*origin_type = InvalidOid;
	return UnassignedDatum;
}
