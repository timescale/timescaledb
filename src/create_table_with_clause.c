/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

#include <postgres.h>
#include <catalog/pg_type.h>

#include "compat/compat.h"
#include "create_table_with_clause.h"
#include "with_clause_parser.h"

static const WithClauseDefinition create_table_with_clauses_def[] = {
	[CreateTableFlagHypertable] = {.arg_names = {"hypertable", NULL}, .type_id = BOOLOID,},
	[CreateTableFlagTimeColumn] = {.arg_names = {"time_column", NULL}, .type_id = TEXTOID,},
};

WithClauseResult *
ts_create_table_with_clause_parse(const List *defelems)
{
	return ts_with_clauses_parse(defelems,
								 create_table_with_clauses_def,
								 TS_ARRAY_LEN(create_table_with_clauses_def));
}
