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
#include <storage/procarray.h>
#include <foreign/foreign.h>
#include <foreign/fdwapi.h>
#include <miscadmin.h>
#include <access/reloptions.h>

#include "errors.h"
#include "export.h"
#include "hypertable.h"
#include "remote/async.h"
#include "remote/dist_txn.h"
#include "remote/connection.h"
#include "data_node.h"

TS_FUNCTION_INFO_V1(ts_remote_exec);

static void
print_result(int elevel, const char *server_name, AsyncResponseResult *result)
{
	PGresult *pg_result = async_response_result_get_pg_result(result);
	FILE *result_stream;
	char *result_text = NULL;
	size_t result_text_size = 0;
	PQprintOpt print_opt;

	result_stream = open_memstream(&result_text, &result_text_size);
	if (!result_stream)
		ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("memory allocation failed")));

	memset(&print_opt, 0, sizeof(print_opt));
	print_opt.header = 1;
	print_opt.align = 1;
	print_opt.fieldSep = "|";
	PQprint(result_stream, pg_result, &print_opt);

	fclose(result_stream);

	if (!result_text_size)
		return;

	elog(elevel, "[%s]:\n%.*s", server_name, (int) result_text_size, result_text);
	free(result_text);
}

static void
set_connection_settings(TSConnection *conn)
{
	AsyncResponseResult *result;
	char *search_path;

	search_path = psprintf("SET search_path = %s, pg_catalog", namespace_search_path);
	result = async_request_wait_ok_result(async_request_send(conn, search_path));
	async_response_result_close(result);
	pfree(search_path);
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
	while (*query != '\0')
	{
		if (!isspace(*query))
			return false;
		query++;
	}
	return true;
}

static void
split_query_and_execute(TSConnection *conn, const char *server_name, const char *sql)
{
	AsyncResponseResult *result;
	char *sql_copy = pstrdup(sql);
	char *query;

	query = strtok(sql_copy, ";");
	for (; query; query = strtok(NULL, ";"))
	{
		if (query_is_empty(query))
			break;
		elog(NOTICE, "[%s]: %s", server_name, query);
		result = async_request_wait_ok_result(async_request_send(conn, query));
		print_result(NOTICE, server_name, result);
		async_response_result_close(result);
	}

	pfree(sql_copy);
}

extern List *hypertable_data_node_array_to_list(ArrayType *serverarr);

Datum
ts_remote_exec(PG_FUNCTION_ARGS)
{
	ArrayType *servers = PG_ARGISNULL(0) ? NULL : PG_GETARG_ARRAYTYPE_P(0);
	char *sql = TextDatumGetCString(PG_GETARG_DATUM(1));
	List *servername_list;
	ListCell *lc;

	if (servers == NULL)
		servername_list = data_node_get_node_name_list();
	else
		servername_list = hypertable_data_node_array_to_list(servers);

	if (list_length(servername_list) == 0)
		ereport(ERROR, (errcode(ERRCODE_TS_NO_DATA_NODES), errmsg("no servers defined")));

	foreach (lc, servername_list)
	{
		const char *server_name = lfirst(lc);
		ForeignServer *foreign_server;
		UserMapping *um;
		TSConnection *conn;

		foreign_server = GetForeignServerByName(server_name, false);
		um = GetUserMapping(GetUserId(), foreign_server->serverid);
		conn = remote_dist_txn_get_connection(um, REMOTE_TXN_USE_PREP_STMT);

		/* Configure connection to be compatible with current options of the test env */
		set_connection_settings(conn);

		/* Split query into separate commands using ';' as a delimeter */
		split_query_and_execute(conn, server_name, sql);

		/* Restore original connection settings */
		remote_connection_configure(conn);
	}

	list_free(servername_list);

	PG_RETURN_VOID();
}
