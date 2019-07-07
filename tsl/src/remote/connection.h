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
#include "stmt_params.h"

typedef struct TSConnection TSConnection;

typedef enum ConnOptionType
{
	CONN_OPTION_TYPE_NONE,
	CONN_OPTION_TYPE_USER,
	CONN_OPTION_TYPE_DATA_NODE,
} ConnOptionType;

/* Open a connection with a remote endpoint.
 *  Note that this is a raw connection that does not obey txn semantics and is allocated using
 * malloc. Most users should use `remote_dist_txn_get_connection` or
 * `remote_connection_cache_get_connection` instead. Must be closed with `remote_connection_close`
 */
TSConnection *remote_connection_open_default(const char *data_node_name);
TSConnection *remote_connection_open(const char *data_node_name, List *data_node_options,
									 List *user_options, bool set_dist_id);
void remote_connection_close(TSConnection *conn);

extern void remote_connection_report_error(int elevel, PGresult *res, TSConnection *conn,
										   bool clear, const char *sql);

extern ConnOptionType remote_connection_option_type(const char *keyword);
extern bool remote_connection_valid_user_option(const char *keyword);
extern bool remote_connection_valid_data_node_option(const char *keyword);
extern unsigned int remote_connection_get_cursor_number(void);
void remote_connection_reset_cursor_number(void);
extern unsigned int remote_connection_get_prep_stmt_number(void);
extern void remote_connection_configure(TSConnection *conn);

extern bool remote_connection_cancel_query(TSConnection *conn);

extern PGconn *remote_connection_get_pg_conn(TSConnection *conn);
extern bool remote_connection_is_processing(TSConnection *conn);
extern void remote_connection_set_processing(TSConnection *conn, bool processing);
extern void remote_connection_configure_if_changed(TSConnection *conn);

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
		async_request_send_with_params(conn,                                                       \
									   sql_statement,                                              \
									   stmt_params_create_from_values(values, n_values),           \
									   0)));

#define remote_connection_query_with_params_any_result(conn, sql_statement, n_values, values)      \
	async_response_result_get_pg_result(async_request_wait_any_result(                             \
		async_request_send_with_params(conn,                                                       \
									   sql_statement,                                              \
									   stmt_params_create_from_values(values, n_values),           \
									   0)));

#define remote_connection_result_close(res) PQclear(res);

#endif /* TIMESCALEDB_TSL_REMOTE_CONNECTION_H */
