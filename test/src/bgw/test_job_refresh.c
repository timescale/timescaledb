/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <postgres.h>
#include <fmgr.h>
#include <funcapi.h>
#include <access/htup_details.h>
#include <utils/memutils.h>

#include "compat/compat.h"
#include "export.h"
#include "bgw/scheduler.h"

TS_FUNCTION_INFO_V1(ts_test_job_refresh);

static List *cur_scheduled_jobs = NIL;

/* Test update_scheduled_jobs_list will correctly update with jobs in bgw_job table.
 * Call this function after loading up bgw_job table
 */
Datum
ts_test_job_refresh(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	ListCell *lc;

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;
		TupleDesc tupdesc;

		/* Use top-level memory context to preserve the global static list */
		cur_scheduled_jobs = ts_update_scheduled_jobs_list(cur_scheduled_jobs, TopMemoryContext);

		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		funcctx->user_fctx = list_head(cur_scheduled_jobs);

		if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		{
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("function returning record called in context "
							"that cannot accept type record")));
		}

		funcctx->tuple_desc = BlessTupleDesc(tupdesc);
		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();
	lc = (ListCell *) funcctx->user_fctx;

	if (lc == NULL)
		SRF_RETURN_DONE(funcctx);
	else
	{
		/* Return the current list_cell and advance ptr */
		HeapTuple tuple;
		Datum *values = palloc(sizeof(*values) * funcctx->tuple_desc->natts);
		bool *nulls = palloc(sizeof(*nulls) * funcctx->tuple_desc->natts);

		ts_populate_scheduled_job_tuple(lfirst(lc), values);
		memset(nulls, 0, sizeof(*nulls) * funcctx->tuple_desc->natts);
		tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);

		funcctx->user_fctx = lnext_compat(cur_scheduled_jobs, lc);
		SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
	}

	PG_RETURN_NULL();
}
