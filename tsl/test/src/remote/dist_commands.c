/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <postgres.h>
#include <fmgr.h>

#include <export.h>
#include <remote/dist_commands.h>
#include <data_node.h>

TS_FUNCTION_INFO_V1(tsl_invoke_distributed_commands);
TS_FUNCTION_INFO_V1(tsl_invoke_faulty_distributed_command);

#define LOG_PG_STATUS(RESULT, TARGET)                                                              \
	elog(INFO,                                                                                     \
		 "%s result: %s",                                                                          \
		 TARGET,                                                                                   \
		 PQresStatus(PQresultStatus(ts_dist_cmd_get_data_node_result(RESULT, TARGET))));

Datum
tsl_invoke_distributed_commands(PG_FUNCTION_ARGS)
{
	List *data_nodes = data_node_get_node_name_list_with_aclcheck(ACL_USAGE);
	List *subset_nodes;
	DistCmdResult *results;
	PreparedDistCmd *prepped_cmd;
	const char *test_args[3] = { "1976-09-18 00:00:00-07", "47", "103.4" };
	ListCell *lc;

	if (data_nodes == NIL)
		elog(ERROR, "no data nodes specified");

	results = ts_dist_cmd_invoke_on_all_data_nodes(
		"CREATE TABLE public.disttable1(time timestamptz, device int, temp float);");

	foreach (lc, data_nodes)
	{
		const char *node = lfirst(lc);

		LOG_PG_STATUS(results, node);
	}

	ts_dist_cmd_close_response(results);

	/* Invoke on subset of nodes */
	subset_nodes = list_copy(data_nodes);
	subset_nodes = list_truncate(subset_nodes, list_length(data_nodes) - 1);

	if (list_length(subset_nodes) == 0)
		elog(ERROR, "Too few nodes to execute test");

	results = ts_dist_cmd_invoke_on_data_nodes("CREATE TABLE public.disttable2(time timestamptz, "
											   "device int, temp float);",
											   subset_nodes);
	Assert(ts_dist_cmd_get_data_node_result(results, llast(data_nodes)) == NULL);

	foreach (lc, subset_nodes)
	{
		const char *node = lfirst(lc);

		LOG_PG_STATUS(results, node);
	}

	ts_dist_cmd_close_response(results);

	prepped_cmd = ts_dist_cmd_prepare_command("INSERT INTO public.disttable1 VALUES ($1, $2, $3)",
											  3,
											  subset_nodes);
	results = ts_dist_cmd_invoke_prepared_command(prepped_cmd, test_args);

	foreach (lc, subset_nodes)
	{
		const char *node = lfirst(lc);

		LOG_PG_STATUS(results, node);
	}

	ts_dist_cmd_close_prepared_command(prepped_cmd);

	PG_RETURN_VOID();
}

Datum
tsl_invoke_faulty_distributed_command(PG_FUNCTION_ARGS)
{
	ts_dist_cmd_invoke_on_all_data_nodes(
		"INSERT INTO public.disttable2 VALUES (CURRENT_TIMESTAMP, 42, 72.5);");
	PG_RETURN_VOID();
}
