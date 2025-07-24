/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#pragma once

#include <postgres.h>

#include <nodes/parsenodes.h>
#include <utils.h>

#include "compat/compat.h"

typedef struct WithClauseDefinition
{
	/* Alternative names for the parameters. The first one is the "main" one
	 * when it comes to printouts.*/
	const char *arg_names[5];
	Oid type_id;
	Datum default_val;
} WithClauseDefinition;

typedef struct WithClauseResult
{
	const WithClauseDefinition *definition;
	bool is_default;
	Datum parsed;
} WithClauseResult;

extern TSDLLEXPORT void ts_with_clause_filter(const List *def_elems, List **within_namespace,
											  List **not_within_namespace);

extern TSDLLEXPORT WithClauseResult *
ts_with_clauses_parse(const List *def_elems, const WithClauseDefinition *args, Size nargs);

extern TSDLLEXPORT WithClauseResult *
ts_with_clauses_parse_reset(const List *def_elems, const WithClauseDefinition *args, Size nargs);

extern TSDLLEXPORT char *ts_with_clause_result_deparse_value(const WithClauseResult *result);
