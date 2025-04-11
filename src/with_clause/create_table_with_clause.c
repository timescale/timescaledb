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
	[CreateTableFlagTimeColumn] = {.arg_names = {"time_column", NULL}, .type_id = TEXTOID,},
	[CreateTableFlagChunkTimeInterval] = {.arg_names = {"chunk_time_interval", NULL}, .type_id = TEXTOID,},
};

WithClauseResult *
ts_create_table_with_clause_parse(const List *defelems)
{
	return ts_with_clauses_parse(defelems,
								 create_table_with_clauses_def,
								 TS_ARRAY_LEN(create_table_with_clauses_def));
}

Datum
ts_create_table_parse_chunk_time_interval(WithClauseResult *parsed_options, Oid column_type,
										  Oid *interval_type)
{
	if (parsed_options[CreateTableFlagChunkTimeInterval].is_default == false)
	{
		Datum textarg = parsed_options[CreateTableFlagChunkTimeInterval].parsed;
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
