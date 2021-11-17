/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <catalog/namespace.h>
#include <utils/fmgrprotos.h>
#include <utils/builtins.h>
#include <utils/memutils.h>
#include <utils/array.h>
#include <utils/guc.h>
#include <storage/procarray.h>
#include <foreign/foreign.h>
#include <foreign/fdwapi.h>
#include <miscadmin.h>
#include <access/reloptions.h>
#include <funcapi.h>

#include "errors.h"
#include "export.h"
#include "compat/compat.h"
#include "hypertable.h"
#include "remote/async.h"
#include "remote/dist_txn.h"
#include "remote/connection.h"
#include "remote/dist_commands.h"
#include "data_node.h"

TS_FUNCTION_INFO_V1(ts_remote_exec);
TS_FUNCTION_INFO_V1(ts_remote_exec_get_result_strings);

/*
 * Print the result of a remote call.
 *
 * We rely on PQprint that takes a file stream (FILE *), but we are limited to
 * using PostgreSQL's file APIs for Windows compatibility, so convenient
 * functions like open_memstream() won't work. Instead, we write to a
 * temporary file stream and then read the result back.
 */
static void
print_result(int elevel, const char *server_name, const PGresult *pg_result)
{
	FILE *result_stream;
	File tmpfile;
	char *result_text = NULL;
	size_t result_text_size = 0;
	PQprintOpt print_opt = {
		.header = 1,
		.align = 1,
		.fieldSep = "|",
	};

	/* elevel is used to specify where to print the result, which is not an error */
	Assert(elevel < ERROR);

	/* If no result to print, i.e., no fields in the result, skip the rest */
	if (PQnfields(pg_result) == 0)
		return;

	/* Open a temporary file for reading */
	tmpfile = OpenTemporaryFile(false);

	/* Open a stream to the same file for writing */
	result_stream = AllocateFile(FilePathName(tmpfile), "wb");

	if (!result_stream)
		elog(ERROR, "could not open message stream for remote_exec");

	/* Print the result to the file stream */
	PQprint(result_stream, pg_result, &print_opt);

	/* Close the writing stream to flush the result */
	FreeFile(result_stream);

	/* Get the size of the written result */
	result_text_size = FileSize(tmpfile);

	/* Read the result into a memory buffer */
	if (result_text_size > 0)
	{
		int nread;

		result_text = malloc(result_text_size);

		nread = FileRead(tmpfile, result_text, result_text_size, 0, 0);
		if (nread != result_text_size)
		{
			free(result_text);
			FileClose(tmpfile);

			elog(ERROR, "unexpected number of bytes (%d) read by remote_exec", nread);
		}
	}

	FileClose(tmpfile);

	if (result_text_size > 0)
		elog(elevel, "[%s]:\n%.*s", server_name, (int) result_text_size, result_text);

	if (result_text != NULL)
		free(result_text);
}

static void
set_connection_settings(TSConnection *conn)
{
	AsyncResponseResult *result;
	const char *search_path = GetConfigOption("search_path", false, false);
	char *search_path_cmd;

	search_path_cmd = psprintf("SET search_path = %s, pg_catalog", search_path);
	result = async_request_wait_ok_result(async_request_send(conn, search_path_cmd));
	async_response_result_close(result);
	pfree(search_path_cmd);
	result = async_request_wait_ok_result(async_request_send(conn, "SET timezone TO DEFAULT"));
	async_response_result_close(result);
	result = async_request_wait_ok_result(async_request_send(conn, "SET datestyle TO DEFAULT"));
	async_response_result_close(result);
	result = async_request_wait_ok_result(async_request_send(conn, "SET intervalstyle TO DEFAULT"));
	async_response_result_close(result);
	result =
		async_request_wait_ok_result(async_request_send(conn, "SET extra_float_digits TO DEFAULT"));
	async_response_result_close(result);
}

static bool
query_is_empty(const char *query)
{
	while ((query != NULL) && (*query != '\0'))
	{
		if (!isspace(*query))
			return false;
		query++;
	}
	return true;
}

#ifdef WIN32
#define strtok_r strtok_s
#endif

static void
split_query_and_execute(TSConnection *conn, const char *server_name, const char *sql)
{
	AsyncResponseResult *result;
	char *sql_copy = pstrdup(sql);
	char *saveptr = NULL;
	char *query = strtok_r(sql_copy, ";", &saveptr);

	for (; !query_is_empty(query); query = strtok_r(NULL, ";", &saveptr))
	{
		elog(NOTICE, "[%s]: %s", server_name, query);
		result = async_request_wait_ok_result(async_request_send(conn, query));
		print_result(NOTICE, server_name, async_response_result_get_pg_result(result));
		async_response_result_close(result);
	}

	pfree(sql_copy);
}

extern List *hypertable_data_node_array_to_list(ArrayType *serverarr);

Datum
ts_remote_exec(PG_FUNCTION_ARGS)
{
	ArrayType *data_nodes = PG_ARGISNULL(0) ? NULL : PG_GETARG_ARRAYTYPE_P(0);
	char *sql = TextDatumGetCString(PG_GETARG_DATUM(1));
	List *data_node_list;
	ListCell *lc;

	if (data_nodes == NULL)
		data_node_list = data_node_get_node_name_list();
	else
		data_node_list = data_node_array_to_node_name_list(data_nodes);

	if (list_length(data_node_list) == 0)
		ereport(ERROR,
				(errcode(ERRCODE_TS_INSUFFICIENT_NUM_DATA_NODES), errmsg("no data nodes defined")));

	foreach (lc, data_node_list)
	{
		const char *node_name = lfirst(lc);
		TSConnection *conn = data_node_get_connection(node_name, REMOTE_TXN_USE_PREP_STMT, true);

		/* Configure connection to be compatible with current options of the test env */
		set_connection_settings(conn);

		/* Split query into separate commands using ';' as a delimeter */
		split_query_and_execute(conn, node_name, sql);

		/* Restore original connection settings */
		if (!remote_connection_configure(conn))
			elog(ERROR, "Could not restore connection settings");
	}

	list_free(data_node_list);

	PG_RETURN_VOID();
}

Datum
ts_remote_exec_get_result_strings(PG_FUNCTION_ARGS)
{
	ArrayType *data_nodes = PG_ARGISNULL(0) ? NULL : PG_GETARG_ARRAYTYPE_P(0);
	char *sql = TextDatumGetCString(PG_GETARG_DATUM(1));
	List *data_node_list = NIL;
	FuncCallContext *funcctx;
	PGresult *result;
	unsigned node_i, nodes_tuples, num_dist_res;

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;
		TupleDesc tupdesc;

		if (data_nodes == NULL)
			data_node_list = data_node_get_node_name_list();
		else
			data_node_list = data_node_array_to_node_name_list(data_nodes);

		if (list_length(data_node_list) == 0)
			ereport(ERROR,
					(errcode(ERRCODE_TS_INSUFFICIENT_NUM_DATA_NODES),
					 errmsg("no data nodes defined")));

		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_SCALAR)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("function returning record called in context "
							"that cannot accept type record")));

		funcctx->user_fctx = ts_dist_cmd_invoke_on_data_nodes(sql, data_node_list, true);
		if (!funcctx->user_fctx)
			ereport(ERROR, (errmsg("Cannot run distributed command")));

		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();
	num_dist_res = ts_dist_cmd_response_count(funcctx->user_fctx);

	for (nodes_tuples = 0, node_i = 0; node_i < num_dist_res;
		 ++node_i, nodes_tuples += PQntuples(result))
	{
		const char *node_name;
		result = ts_dist_cmd_get_result_by_index(funcctx->user_fctx, node_i, &node_name);
		if (PQresultStatus(result) != PGRES_TUPLES_OK)
			ereport(ERROR,
					(errcode(ERRCODE_CONNECTION_EXCEPTION),
					 errmsg("%s", PQresultErrorMessage(result))));

		/* Move on to the next node if you have processed more than the tuples of the already
		   iterated over nodes plus the tuples of the current node. */
		if (funcctx->call_cntr >= nodes_tuples + PQntuples(result))
			continue;

		char **fields = palloc(sizeof(*fields) * PQnfields(result));
		Datum *cstrings = palloc(sizeof(*cstrings) * PQnfields(result));
		int i, cstrings_count, tup_num;
		ArrayType *array = NULL;

		tup_num = funcctx->call_cntr - nodes_tuples;
		for (i = 0, cstrings_count = 0; i < PQnfields(result); ++i)
		{
			if (!PQgetisnull(result, tup_num, i))
			{
				fields[i] = PQgetvalue(result, tup_num, i);
				if (*fields[i] == '\0')
					continue;

				cstrings[cstrings_count++] = CStringGetDatum(fields[i]);
			}
		}
		if (cstrings_count)
			array = construct_array(cstrings,
									cstrings_count,
									CSTRINGOID,
									-2, /* pg_type.typlen */
									false,
									TYPALIGN_CHAR);
		pfree(fields);
		SRF_RETURN_NEXT(funcctx, PointerGetDatum(array));
	}

	ts_dist_cmd_close_response(funcctx->user_fctx);
	SRF_RETURN_DONE(funcctx);
}
