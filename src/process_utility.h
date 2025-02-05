/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#pragma once

#include "compat/compat.h"
#include <postgres.h>
#include "hypertable_cache.h"
#include <nodes/plannodes.h>
#include <tcop/utility.h>

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

/*
 * Procedures that use multiple transactions cannot be run in a transaction
 * block (from a function, from dynamic SQL) or in a subtransaction (from a
 * procedure block with an EXCEPTION clause). Such procedures use
 * PreventInTransactionBlock function to check whether they can be run.
 *
 * Though currently such checks are incomplete, because
 * PreventInTransactionBlock requires isTopLevel argument to throw a
 * consistent error when the call originates from a function. This
 * isTopLevel flag (that is a bit poorly named - see below) is not readily
 * available inside C procedures. The source of truth for it -
 * ProcessUtilityContext parameter is passed to ProcessUtility hooks, but
 * is not included with the function calls. There is an undocumented
 * SPI_inside_nonatomic_context function, that would have been sufficient
 * for isTopLevel flag, but it currently returns false when SPI connection
 * is absent (that is a valid scenario when C procedures are called from
 * top-lelev SQL instead of PLPG procedures or DO blocks) so it cannot be
 * used.
 *
 * To work around this the value of ProcessUtilityContext parameter is
 * saved when TS ProcessUtility hook is entered and can be accessed from
 * C procedures using new ts_process_utility_is_context_nonatomic function.
 * The result is called "non-atomic" instead of "top-level" because the way
 * how isTopLevel flag is determined from the ProcessUtilityContext value
 * in standard_ProcessUtility is insufficient for C procedures - it
 * excludes PROCESS_UTILITY_QUERY_NONATOMIC value (used when called from
 * PLPG procedure without an EXCEPTION clause) that is a valid use case for
 * C procedures with transactions. See details in the description of
 * ExecuteCallStmt function.
 *
 * It is expected that calls to C procedures are done with CALL and always
 * pass though the ProcessUtility hook. The ProcessUtilityContext
 * parameter is set to PROCESS_UTILITY_TOPLEVEL value by default. In
 * unlikely case when a C procedure is called without passing through
 * ProcessUtility hook and the call is done in atomic context, then
 * PreventInTransactionBlock checks will pass, but SPI_commit will fail
 * when checking that all current active snapshots are portal-owned
 * snapshots (the same behaviour that was observed before this change).
 * In atomic context there will be an additional snapshot set in
 * _SPI_execute_plan, see the snapshot handling invariants description
 * in that function.
 */
extern TSDLLEXPORT bool ts_process_utility_is_context_nonatomic(void);

/*
 * Currently in TS ProcessUtility hook the saved ProcessUtilityContext
 * value is reset back to PROCESS_UTILITY_TOPLEVEL on normal exit but
 * is NOT reset in case of ereport exit. C procedures can call this
 * function to reset the saved value before doing the checks that can
 * result in ereport exit.
 */
extern TSDLLEXPORT void ts_process_utility_context_reset(void);
