/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_PROCESS_UTILITY_H
#define TIMESCALEDB_PROCESS_UTILITY_H

#include <postgres.h>
#include <nodes/plannodes.h>
#include <tcop/utility.h>
#include "hypertable_cache.h"
#include "compat/compat.h"

typedef struct ProcessUtilityArgs
{
	Cache *hcache;
	PlannedStmt *pstmt;
	QueryEnvironment *queryEnv;
	ParseState *parse_state;
	Node *parsetree;
	const char *query_string;
	ProcessUtilityContext context;
	ParamListInfo params;
	DestReceiver *dest;
	List *hypertable_list;
#if PG13_GE
	QueryCompletion *completion_tag;
#else
	char *completion_tag;
#endif
#if PG14_GE
	bool readonly_tree;
#endif
} ProcessUtilityArgs;

typedef enum
{
	DDL_CONTINUE,
	DDL_DONE
} DDLResult;

typedef DDLResult (*ts_process_utility_handler_t)(ProcessUtilityArgs *args);

extern void ts_process_utility_set_expect_chunk_modification(bool expect);

#endif /* TIMESCALEDB_PROCESS_UTILITY_H */
