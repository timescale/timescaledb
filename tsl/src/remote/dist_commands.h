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

extern DistCmdResult *ts_dist_cmd_invoke_on_servers(const char *sql, List *server_names);
extern DistCmdResult *ts_dist_cmd_invoke_on_all_servers(const char *sql);

extern PGresult *ts_dist_cmd_get_server_result(DistCmdResult *response, const char *server_name);

extern void ts_dist_cmd_close_response(DistCmdResult *response);

#define ts_dist_cmd_run_on_servers(command, servers)                                               \
	ts_dist_cmd_close_response(ts_dist_cmd_invoke_on_servers(command, servers));

extern PreparedDistCmd *ts_dist_cmd_prepare_command(const char *sql, size_t n_params,
													List *server_names);
extern PreparedDistCmd *ts_dist_cmd_prepare_command_on_all_servers(const char *sql,
																   size_t n_params);

extern DistCmdResult *ts_dist_cmd_invoke_prepared_command(PreparedDistCmd *command,
														  const char *const *param_values);

extern void ts_dist_cmd_close_prepared_command(PreparedDistCmd *command);

#endif
