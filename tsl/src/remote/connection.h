/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_TSL_REMOTE_CONNECTION_H
#define TIMESCALEDB_TSL_REMOTE_CONNECTION_H

#include <postgres.h>
#include <foreign/foreign.h>

#include <libpq-fe.h>

#include "async.h"

typedef enum ConnOptionType
{
	CONN_OPTION_TYPE_NONE,
	CONN_OPTION_TYPE_USER,
	CONN_OPTION_TYPE_SERVER,
} ConnOptionType;

/* Open a connection with a remote endpoint.
 *  Note that this is a raw connection that does not obey txn semantics and is allocated using
 * malloc. Most users should use `remote_dist_txn_get_connection` or
 * `remote_connection_cache_get_connection` instead. Must be closed with `remote_connection_close`
 */
PGconn *remote_connection_open(char *server_name, List *server_options, List *user_options);
PGconn *remote_connection_open_default(char *server_name);
void remote_connection_close(PGconn *conn);

extern void remote_connection_report_error(int elevel, PGresult *res, PGconn *conn, bool clear,
										   const char *sql);

extern ConnOptionType remote_connection_option_type(const char *keyword);
extern bool remote_connection_valid_user_option(const char *keyword);
extern bool remote_connection_valid_server_option(const char *keyword);
extern unsigned int remote_connection_get_cursor_number(void);
void remote_connection_reset_cursor_number(void);
extern unsigned int remote_connection_get_prep_stmt_number(void);
extern void remote_connection_configure(PGconn *conn);

extern bool remote_connection_cancel_query(PGconn *conn);

/* wrappers around async stuff to emulate sync communication */

#define remote_connection_query_ok_result(conn, query)                                             \
	async_response_result_get_pg_result(                                                           \
		async_request_wait_ok_result(async_request_send(conn, query)))

#define remote_connection_query_any_result(conn, query)                                            \
	async_response_result_get_pg_result(                                                           \
		async_request_wait_any_result(async_request_send(conn, query)))

#define remote_connection_exec_ok_command(conn, sql)                                               \
	async_request_wait_ok_command(async_request_send(conn, sql))

#define remote_connection_prepare(conn, sql_statement, nvars)                                      \
	async_request_wait_prepared_statement(async_request_send_prepare(conn, sql_statement, nvars))

#define remote_connection_query_prepared_ok_result(prepared_stmt, values)                          \
	async_response_result_get_pg_result(                                                           \
		async_request_wait_ok_result(async_request_send_prepared_stmt(prepared_stmt, values)))

#define remote_connection_query_with_params_ok_result(conn, sql_statement, n_values, values)       \
	async_response_result_get_pg_result(async_request_wait_ok_result(                              \
		async_request_send_with_params(conn, sql_statement, n_values, values)));

#define remote_connection_query_with_params_any_result(conn, sql_statement, n_values, values)      \
	async_response_result_get_pg_result(async_request_wait_any_result(                             \
		async_request_send_with_params(conn, sql_statement, n_values, values)));

#define remote_connection_result_close(res) PQclear(res);

#endif /* TIMESCALEDB_TSL_REMOTE_CONNECTION_H */
