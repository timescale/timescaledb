/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <c.h>

#include <nodes/parsenodes.h>

/* find the length of a statically sized array */
#define TS_ARRAY_LEN(array) (sizeof(array) / sizeof(*array))

typedef struct WithClauseDefinition
{
	const char *arg_name;
	Oid type_id;
	Datum default_val;
} WithClauseDefinition;

typedef struct WithClauseResult
{
	bool is_default;
	Datum parsed;
} WithClauseResult;

extern void ts_with_clause_filter(const List *def_elems, List **within_namespace,
								  List **not_within_namespace);

extern WithClauseResult *ts_with_clauses_parse(const List *def_elems,
											   const WithClauseDefinition *args, Size nargs);
