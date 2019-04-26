/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <fmgr.h>
#include <funcapi.h>
#include <access/htup_details.h>
#include <foreign/foreign.h>
#include <utils/builtins.h>

#include "server.h"
#include "export.h"

TS_FUNCTION_INFO_V1(test_server_show);
TS_FUNCTION_INFO_V1(tsl_unchecked_add_server);

/*
 * Tests the ts_server_get_servername_list() function.
 */
Datum
test_server_show(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;
		TupleDesc tupdesc;
		List *servernames;
		;

		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("function returning record called in context "
							"that cannot accept type record")));

		servernames = server_get_servername_list();
		funcctx->user_fctx = servernames;
		funcctx->tuple_desc = BlessTupleDesc(tupdesc);
		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();

	if (list_length((List *) funcctx->user_fctx) > 0)
	{
		Datum values[4];
		bool nulls[4] = { true };
		HeapTuple tuple;
		List *servernames = funcctx->user_fctx;
		const char *servername = linitial(servernames);
		ForeignServer *server = GetForeignServerByName(servername, false);
		ListCell *lc;

		funcctx->user_fctx = list_delete_first(servernames);
		funcctx->call_cntr++;

		values[0] = CStringGetDatum(servername);
		nulls[0] = false;

		foreach (lc, server->options)
		{
			DefElem *elem = lfirst(lc);
			Value *v = (Value *) elem->arg;

			Assert(v->type == T_String);

			if (strcmp("host", elem->defname) == 0)
			{
				values[1] = CStringGetTextDatum(v->val.str);
				nulls[1] = false;
			}
			else if (strcmp("port", elem->defname) == 0)
			{
				int32 port = strtoul(v->val.str, NULL, 10) & 0xFFFFFFFF;

				values[2] = Int32GetDatum(port);
				nulls[2] = false;
			}
			else if (strcmp("dbname", elem->defname) == 0)
			{
				values[3] = CStringGetDatum(v->val.str);
				nulls[3] = false;
			}
		}

		tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);

		SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
	}

	SRF_RETURN_DONE(funcctx);
}

/*
 * Performs a server add without setting distributed id or enforcing topolgy constraints.
 */
Datum
tsl_unchecked_add_server(PG_FUNCTION_ARGS)
{
	return server_add_without_dist_id(fcinfo);
}
