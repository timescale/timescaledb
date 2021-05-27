/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_TSL_FDW_MODIFY_EXEC_H
#define TIMESCALEDB_TSL_FDW_MODIFY_EXEC_H

#include <postgres.h>
#include <commands/explain.h>
#include <nodes/parsenodes.h>
#include <nodes/plannodes.h>
#include <nodes/execnodes.h>

typedef struct TsFdwModifyState TsFdwModifyState;

typedef enum ModifyCommand
{
	UPDATE_CMD,
	DELETE_CMD,
} ModifyCommand;

extern void fdw_begin_foreign_modify(PlanState *pstate, ResultRelInfo *rri, CmdType operation,
									 List *fdw_private, Plan *subplan);

extern TupleTableSlot *fdw_exec_foreign_insert(TsFdwModifyState *fmstate, EState *estate,
											   TupleTableSlot *slot, TupleTableSlot *planslot);
extern TupleTableSlot *fdw_exec_foreign_update_or_delete(TsFdwModifyState *fmstate, EState *estate,
														 TupleTableSlot *slot,
														 TupleTableSlot *planslot,
														 ModifyCommand cmd);
extern void fdw_finish_foreign_modify(TsFdwModifyState *fmstate);
extern void fdw_explain_modify(PlanState *ps, ResultRelInfo *rri, List *fdw_private,
							   int subplan_index, ExplainState *es);

#endif /* TIMESCALEDB_TSL_FDW_MODIFY_EXEC_H */
