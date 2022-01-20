/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_TSL_REMOTE_DIST_COMMANDS_H
#define TIMESCALEDB_TSL_REMOTE_DIST_COMMANDS_H

#include "ts_catalog/catalog.h"

#include "async.h"

typedef struct DistCmdResult DistCmdResult;
typedef struct List PreparedDistCmd;
typedef struct DistCmdDescr
{
	const char *sql;
	StmtParams *params;

} DistCmdDescr;

extern DistCmdResult *ts_dist_multi_cmds_params_invoke_on_data_nodes(List *cmd_descriptors,
																	 List *data_nodes,
																	 bool transactional);
extern DistCmdResult *ts_dist_cmd_invoke_on_data_nodes(const char *sql, List *node_names,
													   bool transactional);
extern DistCmdResult *ts_dist_cmd_params_invoke_on_data_nodes(const char *sql, StmtParams *params,
															  List *data_nodes, bool transactional);
extern DistCmdResult *ts_dist_multi_cmds_invoke_on_data_nodes_using_search_path(
	List *cmd_descriptors, const char *search_path, List *node_names, bool transactional);
extern DistCmdResult *ts_dist_cmd_invoke_on_data_nodes_using_search_path(const char *sql,
																		 const char *search_path,
																		 List *node_names,
																		 bool transactional);
extern DistCmdResult *ts_dist_cmd_invoke_on_all_data_nodes(const char *sql);
extern DistCmdResult *ts_dist_cmd_invoke_func_call_on_all_data_nodes(FunctionCallInfo fcinfo);
extern DistCmdResult *ts_dist_cmd_invoke_func_call_on_data_nodes(FunctionCallInfo fcinfo,
																 List *data_nodes);
extern Datum ts_dist_cmd_get_single_scalar_result_by_index(DistCmdResult *result, Size index,
														   bool *isnull, const char **node_name);
extern void ts_dist_cmd_func_call_on_data_nodes(FunctionCallInfo fcinfo, List *data_nodes);
extern PGresult *ts_dist_cmd_get_result_by_node_name(DistCmdResult *response,
													 const char *node_name);
extern PGresult *ts_dist_cmd_get_result_by_index(DistCmdResult *response, Size index,
												 const char **node_name);
extern void ts_dist_cmd_clear_result_by_index(DistCmdResult *response, Size index);
extern Size ts_dist_cmd_response_count(DistCmdResult *result);
extern long ts_dist_cmd_total_row_count(DistCmdResult *result);
extern void ts_dist_cmd_close_response(DistCmdResult *response);

#define ts_dist_cmd_run_on_data_nodes(command, nodes, transactional)                               \
	ts_dist_cmd_close_response(ts_dist_cmd_invoke_on_data_nodes(command, nodes, transactional));

extern PreparedDistCmd *ts_dist_cmd_prepare_command(const char *sql, size_t n_params,
													List *node_names);
extern PreparedDistCmd *ts_dist_cmd_prepare_command_on_all_data_nodes(const char *sql,
																	  size_t n_params);

extern DistCmdResult *ts_dist_cmd_invoke_prepared_command(PreparedDistCmd *command,
														  const char *const *param_values);

extern void ts_dist_cmd_close_prepared_command(PreparedDistCmd *command);

extern Datum ts_dist_cmd_exec(PG_FUNCTION_ARGS);

#endif
