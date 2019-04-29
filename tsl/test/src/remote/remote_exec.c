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

#include "errors.h"
#include "export.h"
#include "compat.h"
#include "hypertable.h"
#include "remote/async.h"
#include "remote/dist_txn.h"
#include "remote/connection.h"
#include "server.h"

TS_FUNCTION_INFO_V1(ts_remote_exec);

/*
 * Print the result of a remote call.
 *
 * We rely on PQprint that takes a file stream (FILE *), but we are limited to
 * using PostgreSQL's file APIs for Windows compatibility, so convenient
 * functions like open_memstream() won't work. Instead, we write to a
 * temporary file stream and then read the result back.
 */
static void
print_result(int elevel, const char *server_name, AsyncResponseResult *result)
{
	PGresult *pg_result = async_response_result_get_pg_result(result);
	FILE *result_stream;
	File tmpfile;
	char *result_text = NULL;
	size_t result_text_size = 0;
	PQprintOpt print_opt = {
		.header = 1,
		.align = 1,
		.fieldSep = "|",
	};

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
#if PG12_GE
	result_text_size = FileSize(tmpfile);
#else
	result_text_size = FileSeek(tmpfile, 0, SEEK_END);
	FileSeek(tmpfile, 0, SEEK_SET);
#endif

	/* Read the result into a memory buffer */
	if (result_text_size > 0)
	{
		int nread;

		result_text = malloc(result_text_size);

#if PG12_GE
		nread = FileRead(tmpfile, result_text, result_text_size, 0, 0);
#else
		nread = FileRead(tmpfile, result_text, result_text_size, 0);
#endif
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
set_connection_settings(PGconn *conn)
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
	while (*query != '\0')
	{
		if (!isspace(*query))
			return false;
		query++;
	}
	return true;
}

static void
split_query_and_execute(PGconn *conn, const char *server_name, const char *sql)
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

extern List *hypertable_server_array_to_list(ArrayType *serverarr);

Datum
ts_remote_exec(PG_FUNCTION_ARGS)
{
	ArrayType *servers = PG_ARGISNULL(0) ? NULL : PG_GETARG_ARRAYTYPE_P(0);
	char *sql = TextDatumGetCString(PG_GETARG_DATUM(1));
	List *servername_list;
	ListCell *lc;

	if (servers == NULL)
		servername_list = server_get_servername_list();
	else
		servername_list = hypertable_server_array_to_list(servers);

	if (list_length(servername_list) == 0)
		ereport(ERROR, (errcode(ERRCODE_TS_NO_SERVERS), errmsg("no servers defined")));

	foreach (lc, servername_list)
	{
		const char *server_name = lfirst(lc);
		ForeignServer *foreign_server;
		UserMapping *um;
		PGconn *conn;

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
