/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <funcapi.h>
#include <utils/builtins.h>
#include <utils/lsyscache.h>
#include <utils/syscache.h>

#include "healthcheck.h"
#include "data_node.h"
#include "dist_commands.h"
#include "dist_util.h"

/*
 * Functions and data structures for printing a health check result.
 */
enum Anum_show_conn
{
	Anum_health_node_name = 1,
	Anum_health_healthy,
	Anum_health_in_recovery,
	Anum_health_error,
	_Anum_health_max,
};

#define Natts_health (_Anum_health_max - 1)

/*
 * Health check result.
 *
 * Produces a tuple with four columns:
 *
 * 1. Node name (NULL for the access node itself)
 *
 * 2. A "healthy" boolean. An access node returns "false" in case a data node
 * is not responding.
 *
 * 3. A boolean telling if the node is in recovery. Note that the "healthy"
 * status will be "true" while the node is in recovery.
 *
 * 4. An optional error message explaining the reason for "healthy" being
 * false.
 */
static HeapTuple
create_health_check_tuple(const char *data_node, bool in_recovery, TupleDesc tupdesc)
{
	Datum values[Natts_health];
	bool nulls[Natts_health];

	memset(nulls, true, sizeof(nulls));

	if (NULL != data_node)
	{
		NameData *node_name = palloc(sizeof(NameData));
		namestrcpy(node_name, data_node);
		values[AttrNumberGetAttrOffset(Anum_health_node_name)] = NameGetDatum(node_name);
		nulls[AttrNumberGetAttrOffset(Anum_health_node_name)] = false;
	}

	values[AttrNumberGetAttrOffset(Anum_health_healthy)] = BoolGetDatum(in_recovery ? false : true);
	nulls[AttrNumberGetAttrOffset(Anum_health_healthy)] = false;
	values[AttrNumberGetAttrOffset(Anum_health_in_recovery)] = BoolGetDatum(in_recovery);
	nulls[AttrNumberGetAttrOffset(Anum_health_in_recovery)] = false;

	return heap_form_tuple(tupdesc, values, nulls);
}

static IOFuncSelector
get_io_func_selector_from_format(int format)
{
	switch (format)
	{
		case 0: /* text format */
			return IOFunc_input;
		case 1: /* binary format */
			return IOFunc_receive;
		default:
			/* reserved for future, so likely won't happen */
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("unexpected format of data node response")));
	}

	pg_unreachable();
	/* Silence Windows compiler */
	return IOFunc_input;
}

static void
fill_in_result_error(Datum values[Natts_health], bool nulls[Natts_health], const char *errormsg)
{
	values[AttrNumberGetAttrOffset(Anum_health_error)] = CStringGetTextDatum(errormsg);
	nulls[AttrNumberGetAttrOffset(Anum_health_error)] = false;
}

Datum
ts_dist_health_check(PG_FUNCTION_ARGS)
{
	DistCmdResult *result;
	FuncCallContext *funcctx;
	HeapTuple tuple = NULL;

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;
		TupleDesc tupdesc;

		funcctx = SRF_FIRSTCALL_INIT();

		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("function returning record called in context "
							"that cannot accept type record")));

		funcctx->tuple_desc = BlessTupleDesc(tupdesc);

		if (dist_util_membership() == DIST_MEMBER_ACCESS_NODE)
		{
			List *data_node_list;
			StringInfo cmd = makeStringInfo();
			Oid fnamespaceid = get_func_namespace(fcinfo->flinfo->fn_oid);

			appendStringInfo(cmd,
							 "SELECT * FROM %s.%s()",
							 get_namespace_name(fnamespaceid),
							 get_func_name(fcinfo->flinfo->fn_oid));
			data_node_list = data_node_get_node_name_list();
			result = ts_dist_cmd_invoke_on_data_nodes_using_search_path(cmd->data,
																		NULL,
																		data_node_list,
																		true);
			funcctx->user_fctx = result;
			list_free(data_node_list);
		}

		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();

	switch (dist_util_membership())
	{
		case DIST_MEMBER_ACCESS_NODE:
			result = funcctx->user_fctx;
			unsigned int call_cnt = funcctx->call_cntr;

			if (call_cnt == 0)
			{
				/* On call count 0, produce a tuple for health of AN itself */
				tuple = create_health_check_tuple(NULL, RecoveryInProgress(), funcctx->tuple_desc);
			}
			else if (result == NULL)
			{
				/* No result from data nodes, so nothing to do. This probably
				 * cannot happen in practice. */
				SRF_RETURN_DONE(funcctx);
			}
			else if (call_cnt > ts_dist_cmd_response_count(result))
			{
				/* No more responses so no more tuples to produce */
				ts_dist_cmd_close_response(result);
				funcctx->user_fctx = NULL;
				SRF_RETURN_DONE(funcctx);
			}
			else
			{
				/*
				 * Produce a tuple from a data node's response.
				 *
				 * TODO: Currently, the remote commands to data nodes will
				 * either succeed or throw an error if one of the data nodes
				 * cannot be contacted. Therefore, the access node will never
				 * produce a result showing a data node as unhealthy (unless a
				 * node is in recovery). This needs to be changed so that
				 * connection issues (or errors from data node) won't result
				 * in throwing an error here. Instead, the result should be
				 * "unhealthy" with the appropriate error string if available.
				 */
				const char *node_name = "";
				NameData data_node_name;
				PGresult *pgres = ts_dist_cmd_get_result_by_index(result, call_cnt - 1, &node_name);
				TupleDesc return_tupdesc = funcctx->tuple_desc;
				Datum values[Natts_health];
				bool nulls[Natts_health];

				memset(nulls, true, sizeof(nulls));

				namestrcpy(&data_node_name, node_name);
				nulls[AttrNumberGetAttrOffset(Anum_health_node_name)] = false;
				values[AttrNumberGetAttrOffset(Anum_health_node_name)] =
					NameGetDatum(&data_node_name);
				if (PQresultStatus(pgres) != PGRES_TUPLES_OK)
				{
					values[AttrNumberGetAttrOffset(Anum_health_error)] =
						CStringGetTextDatum(PQresultErrorMessage(pgres));
					nulls[AttrNumberGetAttrOffset(Anum_health_error)] = false;
				}
				else if (PQnfields(pgres) != funcctx->tuple_desc->natts)
				{
					StringInfo error = makeStringInfo();
					appendStringInfo(error,
									 "unexpected number of fields in data node response (%d vs %d) "
									 "%s",
									 PQnfields(pgres),
									 funcctx->tuple_desc->natts,
									 PQgetvalue(pgres, 0, 0));

					fill_in_result_error(values, nulls, error->data);
				}
				else if (PQntuples(pgres) != 1)
				{
					StringInfo error = makeStringInfo();
					appendStringInfo(error,
									 "unexpected number of rows in data node response (%d vs %d)",
									 PQntuples(pgres),
									 1);

					fill_in_result_error(values, nulls, error->data);
				}
				else
				{
					int i;

					for (i = 0; i < return_tupdesc->natts; i++)
					{
						/* Data nodes don't return their own name, and it was already filled in */
						if (i == AttrNumberGetAttrOffset(Anum_health_node_name))
						{
							Assert(PQgetisnull(pgres, 0 /* row */, i /* column */) == 1);
						}
						else if (PQgetisnull(pgres, 0 /* row */, i /* column */) == 1)
						{
							nulls[i] = true;
						}
						else
						{
							Oid typid = PQftype(pgres, i);
							int format = PQfformat(pgres, i);
							IOFuncSelector iofuncsel = get_io_func_selector_from_format(format);
							int16 typlen;
							bool typbyval;
							char typalign;
							char typdelim;
							Oid typioparam;
							Oid typfuncid;

							if (typid != return_tupdesc->attrs[i].atttypid)
							{
								StringInfo error = makeStringInfo();
								appendStringInfo(error,
												 "unexpected field type in data node response %u "
												 "vs %u",
												 typid,
												 return_tupdesc->attrs[i].attrelid);

								fill_in_result_error(values, nulls, error->data);
								break;
							}

							get_type_io_data(typid,
											 iofuncsel,
											 &typlen,
											 &typbyval,
											 &typalign,
											 &typdelim,
											 &typioparam,
											 &typfuncid);

							if (iofuncsel == IOFunc_receive)
							{
								StringInfo data = makeStringInfo();
								appendBinaryStringInfo(data,
													   PQgetvalue(pgres, 0, i),
													   PQgetlength(pgres, 0, i));
								values[i] = OidReceiveFunctionCall(typfuncid,
																   data,
																   typioparam,
																   PQfmod(pgres, i));
							}
							else
							{
								Assert(iofuncsel == IOFunc_input);
								values[i] = OidInputFunctionCall(typfuncid,
																 PQgetvalue(pgres, 0, i),
																 typioparam,
																 PQfmod(pgres, i));
							}

							nulls[i] = false;
						}
					}
				}

				tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
			}
			break;
		case DIST_MEMBER_DATA_NODE:
		case DIST_MEMBER_NONE:
			if (funcctx->call_cntr > 0)
				SRF_RETURN_DONE(funcctx);

			tuple = create_health_check_tuple(NULL, RecoveryInProgress(), funcctx->tuple_desc);
			break;
	}

	SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
}
