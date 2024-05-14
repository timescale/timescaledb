/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#pragma once

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
	QueryCompletion *completion_tag;
	bool readonly_tree;
} ProcessUtilityArgs;

typedef enum
{
	DDL_CONTINUE,
	DDL_DONE
} DDLResult;

typedef DDLResult (*ts_process_utility_handler_t)(ProcessUtilityArgs *args);

extern void ts_process_utility_set_expect_chunk_modification(bool expect);
