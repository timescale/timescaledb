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

/* Associated with a connection foreign server and user id */
typedef struct TSConnectionId
{
	Oid server_id;
	Oid user_id;
} TSConnectionId;

typedef enum ConnOptionType
{
	CONN_OPTION_TYPE_NONE,
	CONN_OPTION_TYPE_USER,
	CONN_OPTION_TYPE_NODE,
} ConnOptionType;

/* Open a connection with a remote endpoint. Note that this is a raw
 * connection that does not obey txn semantics and is allocated using
 * malloc. Most users should use `remote_dist_txn_get_connection` or
 * `remote_connection_cache_get_connection` instead. Must be closed with
 * `remote_connection_close`
 */
extern TSConnection *remote_connection_open_with_options(const char *node_name,
														 List *connection_options,
														 bool set_dist_id);
extern TSConnection *remote_connection_open_by_id(TSConnectionId id);
extern TSConnection *remote_connection_open(Oid server_id, Oid user_id);
extern TSConnection *remote_connection_open_nothrow(Oid server_id, Oid user_id, char **errmsg);
extern bool remote_connection_set_autoclose(TSConnection *conn, bool autoclose);
extern bool remote_connection_ping(const char *node_name);
extern void remote_connection_close(TSConnection *conn);
extern PGresult *remote_connection_exec(TSConnection *conn, const char *cmd);
extern PGresult *remote_connection_execf(TSConnection *conn, const char *fmt, ...)
	pg_attribute_printf(2, 3);
extern PGresult *remote_connection_query_ok(TSConnection *conn, const char *query);
extern PGresult *remote_connection_queryf_ok(TSConnection *conn, const char *fmt, ...)
	pg_attribute_printf(2, 3);
extern void remote_connection_cmd_ok(TSConnection *conn, const char *cmd);
extern void remote_connection_cmdf_ok(TSConnection *conn, const char *fmt, ...)
	pg_attribute_printf(2, 3);
extern ConnOptionType remote_connection_option_type(const char *keyword);
extern bool remote_connection_valid_user_option(const char *keyword);
extern bool remote_connection_valid_node_option(const char *keyword);
extern unsigned int remote_connection_get_cursor_number(void);
extern void remote_connection_reset_cursor_number(void);
extern unsigned int remote_connection_get_prep_stmt_number(void);
extern bool remote_connection_configure(TSConnection *conn);
extern bool remote_connection_cancel_query(TSConnection *conn);
extern PGconn *remote_connection_get_pg_conn(TSConnection *conn);
extern bool remote_connection_is_processing(TSConnection *conn);
extern void remote_connection_set_processing(TSConnection *conn, bool processing);
extern bool remote_connection_configure_if_changed(TSConnection *conn);
extern void remote_connection_elog(TSConnection *conn, int elevel);

/* Functions operating on PGresult objects */
extern void remote_result_cmd_ok(PGresult *res);
extern PGresult *remote_result_query_ok(PGresult *res);
extern void remote_result_close(PGresult *res);
extern void remote_result_elog(PGresult *res, int elevel);

/* wrappers around async stuff to emulate sync communication */

#define remote_connection_id(server_oid, user_oid)                                                 \
	{                                                                                              \
		.server_id = (server_oid), .user_id = (user_oid)                                           \
	}

#define remote_connection_id_set(id, server_oid, user_oid)                                         \
	do                                                                                             \
	{                                                                                              \
		(id)->server_id = (server_oid);                                                            \
		(id)->user_id = (user_oid);                                                                \
	} while (0)

typedef struct RemoteConnectionStats
{
	unsigned int connections_created;
	unsigned int connections_closed;
	unsigned int results_created;
	unsigned int results_cleared;
} RemoteConnectionStats;

#if TS_DEBUG
extern void remote_connection_stats_reset(void);
extern RemoteConnectionStats *remote_connection_stats_get(void);
#endif

extern void _remote_connection_init(void);
extern void _remote_connection_fini(void);

#endif /* TIMESCALEDB_TSL_REMOTE_CONNECTION_H */
