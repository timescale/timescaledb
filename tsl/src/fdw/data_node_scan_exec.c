/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <nodes/extensible.h>
#include <nodes/execnodes.h>
#include <nodes/nodeFuncs.h>
#include <executor/tuptable.h>
#include <utils/memutils.h>
#include <utils/rel.h>

#include "scan_plan.h"
#include "scan_exec.h"
#include "data_node_scan_plan.h"
#include "data_node_scan_exec.h"
#include "nodes/async_append.h"
#include "remote/data_fetcher.h"
#include "guc.h"

/*
 * The execution stage of a DataNodeScan.
 *
 * This implements the execution stage CustomScan interface for a DataNodeScan
 * plan. This is heavily based on the ForeignScan implementation, but allow
 * scans of remote relations that doesn't have a corresponding local foreign
 * table, which is the case for a data node relation.
 */

typedef struct DataNodeScanState
{
	AsyncScanState async_state;
	TsFdwScanState fsstate;
	ExprState *recheck_quals;
	bool systemcol;
} DataNodeScanState;

static void
data_node_scan_begin(CustomScanState *node, EState *estate, int eflags)
{
	DataNodeScanState *sss = (DataNodeScanState *) node;
	CustomScan *cscan = (CustomScan *) node->ss.ps.plan;
	List *fdw_exprs = linitial(cscan->custom_exprs);
	List *recheck_quals = lsecond(cscan->custom_exprs);
	List *fdw_private = list_nth(cscan->custom_private, DataNodeScanFdwPrivate);

	if ((eflags & EXEC_FLAG_EXPLAIN_ONLY) && !ts_guc_enable_remote_explain)
		return;

	fdw_scan_init(&node->ss, &sss->fsstate, cscan->custom_relids, fdw_private, fdw_exprs, eflags);

	sss->recheck_quals = ExecInitQual(recheck_quals, (PlanState *) node);
}

static TupleTableSlot *
data_node_scan_next(CustomScanState *node)
{
	DataNodeScanState *sss = (DataNodeScanState *) node;
	ExprContext *econtext = node->ss.ps.ps_ExprContext;
	MemoryContext oldcontext;
	TupleTableSlot *slot;

	/* Call the Iterate function in short-lived context */
	oldcontext = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);
	slot = fdw_scan_iterate(&node->ss, &sss->fsstate);
	MemoryContextSwitchTo(oldcontext);

	/* Raise an error when system column is requsted, eg. tableoid */
	if (sss->systemcol && !TupIsNull(slot))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("system columns are not accessible on distributed hypertables with current "
						"settings"),
				 errhint("Set timescaledb.enable_per_data_node_queries=false to query system "
						 "columns.")));

	return slot;
}

/*
 * Access method routine to recheck a tuple in EvalPlanQual
 */
static bool
data_node_scan_recheck(CustomScanState *node, TupleTableSlot *slot)
{
	DataNodeScanState *sss = (DataNodeScanState *) node;
	ExprContext *econtext;

	/*
	 * extract necessary information from the custom scan node
	 */
	econtext = node->ss.ps.ps_ExprContext;

	/* Does the tuple meet the remote qual condition? */
	econtext->ecxt_scantuple = slot;

	ResetExprContext(econtext);

	return ExecQual(sss->recheck_quals, econtext);
}

static TupleTableSlot *
data_node_scan_exec(CustomScanState *node)
{
	return ExecScan(&node->ss,
					(ExecScanAccessMtd) data_node_scan_next,
					(ExecScanRecheckMtd) data_node_scan_recheck);
}

static void
data_node_scan_rescan(CustomScanState *node)
{
	fdw_scan_rescan(&node->ss, &((DataNodeScanState *) node)->fsstate);
}

static void
data_node_scan_end(CustomScanState *node)
{
	fdw_scan_end(&((DataNodeScanState *) node)->fsstate);
}

static void
data_node_scan_explain(CustomScanState *node, List *ancestors, ExplainState *es)
{
	CustomScan *scan = (CustomScan *) node->ss.ps.plan;
	List *fdw_private = list_nth(scan->custom_private, DataNodeScanFdwPrivate);

	fdw_scan_explain(&node->ss, fdw_private, es, &((DataNodeScanState *) node)->fsstate);
}

static CustomExecMethods data_node_scan_state_methods = {
	.CustomName = "DataNodeScanState",
	.BeginCustomScan = data_node_scan_begin,
	.EndCustomScan = data_node_scan_end,
	.ExecCustomScan = data_node_scan_exec,
	.ReScanCustomScan = data_node_scan_rescan,
	.ExplainCustomScan = data_node_scan_explain,
};

static void
create_fetcher(AsyncScanState *ass)
{
	DataNodeScanState *dnss = (DataNodeScanState *) ass;
	create_data_fetcher(&dnss->async_state.css.ss, &dnss->fsstate);
}

static void
send_fetch_request(AsyncScanState *ass)
{
	DataNodeScanState *dnss = (DataNodeScanState *) ass;
	DataFetcher *fetcher = dnss->fsstate.fetcher;

	fetcher->funcs->send_fetch_request(fetcher);
}

static void
fetch_data(AsyncScanState *ass)
{
	DataNodeScanState *dnss = (DataNodeScanState *) ass;
	DataFetcher *fetcher = dnss->fsstate.fetcher;

	fetcher->funcs->fetch_data(fetcher);
}

Node *
data_node_scan_state_create(CustomScan *cscan)
{
	DataNodeScanState *dnss =
		(DataNodeScanState *) newNode(sizeof(DataNodeScanState), T_CustomScanState);

	dnss->async_state.css.methods = &data_node_scan_state_methods;
	dnss->systemcol = linitial_int(list_nth(cscan->custom_private, DataNodeScanSystemcol));
	dnss->async_state.init = create_fetcher;
	dnss->async_state.send_fetch_request = send_fetch_request;
	dnss->async_state.fetch_data = fetch_data;
	dnss->fsstate.fetcher_type = intVal(list_nth(cscan->custom_private, DataNodeScanFetcherType));
	return (Node *) dnss;
}
