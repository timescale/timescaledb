
/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <postgres.h>
#include <fmgr.h>

#include "export.h"
#include "remote/dist_commands.h"

TS_FUNCTION_INFO_V1(tsl_invoke_distributed_commands);
TS_FUNCTION_INFO_V1(tsl_invoke_faulty_distributed_command);

#define LOG_PG_STATUS(RESULT, TARGET)                                                              \
	elog(INFO,                                                                                     \
		 "%s result: %s",                                                                          \
		 TARGET,                                                                                   \
		 PQresStatus(PQresultStatus(ts_dist_cmd_get_server_result(RESULT, TARGET))));

Datum
tsl_invoke_distributed_commands(PG_FUNCTION_ARGS)
{
	List *servers = list_make2("server1", "server3");
	DistCmdResult *results;
	PreparedDistCmd *prepped_cmd;
	const char *test_args[3] = { "1976-09-18 00:00:00-07", "47", "103.4" };

	results = ts_dist_cmd_invoke_on_all_servers(
		"CREATE TABLE public.disttable1(time timestamptz, device int, temp float);");
	LOG_PG_STATUS(results, "server1");
	LOG_PG_STATUS(results, "server2");
	LOG_PG_STATUS(results, "server3");
	ts_dist_cmd_close_response(results);

	results = ts_dist_cmd_invoke_on_servers("CREATE TABLE public.disttable2(time timestamptz, "
											"device int, temp float);",
											servers);
	Assert(ts_dist_cmd_get_server_result(results, "server2") == NULL);
	LOG_PG_STATUS(results, "server1");
	LOG_PG_STATUS(results, "server3");
	ts_dist_cmd_close_response(results);

	prepped_cmd = ts_dist_cmd_prepare_command("INSERT INTO public.disttable1 VALUES ($1, $2, $3)",
											  3,
											  servers);
	results = ts_dist_cmd_invoke_prepared_command(prepped_cmd, test_args);
	LOG_PG_STATUS(results, "server1");
	LOG_PG_STATUS(results, "server3");
	ts_dist_cmd_close_prepared_command(prepped_cmd);

	PG_RETURN_VOID();
}

Datum
tsl_invoke_faulty_distributed_command(PG_FUNCTION_ARGS)
{
	ts_dist_cmd_invoke_on_servers("INSERT INTO disttable2 VALUES (CURRENT_TIMESTAMP, 42, 72.5);",
								  NULL);
	PG_RETURN_VOID();
}
