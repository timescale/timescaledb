/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_TSL_REMOTE_DIST_COMMANDS_H
#define TIMESCALEDB_TSL_REMOTE_DIST_COMMANDS_H

#include <catalog.h>
#include <libpq-fe.h>

typedef struct DistCmdResult DistCmdResult;
typedef struct List PreparedDistCmd;

extern DistCmdResult *ts_dist_cmd_invoke_on_data_nodes(const char *sql, List *node_names,
													   bool transactional);
extern DistCmdResult *ts_dist_cmd_invoke_on_data_nodes_using_search_path(const char *sql,
																		 const char *search_path,
																		 List *node_names,
																		 bool transactional);
extern DistCmdResult *ts_dist_cmd_invoke_on_all_data_nodes(const char *sql);
extern DistCmdResult *ts_dist_cmd_invoke_func_call_on_all_data_nodes(FunctionCallInfo fcinfo);
extern DistCmdResult *ts_dist_cmd_invoke_func_call_on_data_nodes(FunctionCallInfo fcinfo,
																 List *data_nodes);
extern void ts_dist_cmd_func_call_on_data_nodes(FunctionCallInfo fcinfo, List *data_nodes);
extern PGresult *ts_dist_cmd_get_data_node_result(DistCmdResult *response, const char *node_name);

extern void ts_dist_cmd_close_response(DistCmdResult *response);

#define ts_dist_cmd_run_on_data_nodes(command, nodes)                                              \
	ts_dist_cmd_close_response(ts_dist_cmd_invoke_on_data_nodes(command, nodes, true));

extern PreparedDistCmd *ts_dist_cmd_prepare_command(const char *sql, size_t n_params,
													List *node_names);
extern PreparedDistCmd *ts_dist_cmd_prepare_command_on_all_data_nodes(const char *sql,
																	  size_t n_params);

extern DistCmdResult *ts_dist_cmd_invoke_prepared_command(PreparedDistCmd *command,
														  const char *const *param_values);

extern void ts_dist_cmd_close_prepared_command(PreparedDistCmd *command);

extern Datum ts_dist_cmd_exec(PG_FUNCTION_ARGS);

#endif
