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

typedef struct TSConnectionError
{
	/* Local error information */
	int errcode;
	const char *msg;
	const char *host;
	const char *nodename;
	const char *connmsg;
	/* Remote error information, if available */
	struct
	{
		int elevel;
		int errcode;
		const char *sqlstate;
		const char *msg;
		const char *hint;
		const char *detail;
		const char *context;
		const char *stmtpos;
		const char *sqlcmd;
	} remote;
} TSConnectionError;

/* Open a connection with a remote endpoint. Note that this is a raw
 * connection that does not obey txn semantics and is allocated using
 * malloc. Most users should use `remote_dist_txn_get_connection` or
 * `remote_connection_cache_get_connection` instead. Must be closed with
 * `remote_connection_close`
 */
extern TSConnection *remote_connection_open_with_options(const char *node_name,
														 List *connection_options,
														 bool set_dist_id);
extern TSConnection *remote_connection_open_with_options_nothrow(const char *node_name,
																 List *connection_options,
																 char **errmsg);
extern TSConnection *remote_connection_open_by_id(TSConnectionId id);
extern TSConnection *remote_connection_open(Oid server_id, Oid user_id);
extern TSConnection *remote_connection_open_nothrow(Oid server_id, Oid user_id, char **errmsg);
extern List *remote_connection_prepare_auth_options(const ForeignServer *server, Oid user_id);
extern bool remote_connection_set_autoclose(TSConnection *conn, bool autoclose);
extern int remote_connection_xact_depth_get(const TSConnection *conn);
extern int remote_connection_xact_depth_inc(TSConnection *conn);
extern int remote_connection_xact_depth_dec(TSConnection *conn);
extern void remote_connection_xact_transition_begin(TSConnection *conn);
extern void remote_connection_xact_transition_end(TSConnection *conn);
extern bool remote_connection_xact_is_transitioning(const TSConnection *conn);
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
extern bool remote_connection_check_extension(TSConnection *conn);
extern void remote_validate_extension_version(TSConnection *conn, const char *data_node_version);
extern char *remote_connection_get_connstr(const char *node_name);

typedef enum TSConnectionResult
{
	CONN_OK,
	CONN_TIMEOUT,
	CONN_DISCONNECT,
	CONN_NO_RESPONSE,
} TSConnectionResult;

typedef enum TSConnectionStatus
{
	CONN_IDLE,		 /* No command being processed */
	CONN_PROCESSING, /* Command/query is being processed */
	CONN_COPY_IN,	/* Connection is in COPY_IN mode */
} TSConnectionStatus;

TSConnectionResult remote_connection_drain(TSConnection *conn, TimestampTz endtime,
										   PGresult **result);
extern bool remote_connection_cancel_query(TSConnection *conn);
extern PGconn *remote_connection_get_pg_conn(const TSConnection *conn);
extern bool remote_connection_is_processing(const TSConnection *conn);
extern void remote_connection_set_status(TSConnection *conn, TSConnectionStatus status);
extern TSConnectionStatus remote_connection_get_status(const TSConnection *conn);
extern bool remote_connection_configure_if_changed(TSConnection *conn);
extern const char *remote_connection_node_name(const TSConnection *conn);
extern bool remote_connection_set_single_row_mode(TSConnection *conn);

/* Functions operating on PGresult objects */
extern void remote_result_cmd_ok(PGresult *res);
extern PGresult *remote_result_query_ok(PGresult *res);
extern void remote_result_close(PGresult *res);

/* wrappers around async stuff to emulate sync communication */

extern TSConnectionId remote_connection_id(const Oid server_oid, const Oid user_oid);
extern void remote_connection_id_set(TSConnectionId *const id, const Oid server_oid,
									 const Oid user_oid);

typedef struct RemoteConnectionStats
{
	unsigned int connections_created;
	unsigned int connections_closed;
	unsigned int results_created;
	unsigned int results_cleared;
} RemoteConnectionStats;

#ifdef TS_DEBUG
extern void remote_connection_stats_reset(void);
extern RemoteConnectionStats *remote_connection_stats_get(void);
#endif

/*
 * Connection functions for COPY mode.
 */
extern bool remote_connection_begin_copy(TSConnection *conn, const char *copycmd, bool binary,
										 TSConnectionError *err);
extern bool remote_connection_end_copy(TSConnection *conn, TSConnectionError *err);
extern bool remote_connection_put_copy_data(TSConnection *conn, const char *buffer, size_t len,
											TSConnectionError *err);

/* Error handling functions for connections */
extern void remote_connection_get_error(const TSConnection *conn, TSConnectionError *err);
extern void remote_connection_get_result_error(const PGresult *res, TSConnectionError *err);

/*
 * The following are macros for emitting errors related to connections or
 * remote command execution. They need to be macros to preserve the error
 * context of where they are called (line number, statement, etc.).
 */
#define remote_connection_error_elog(err, elevel)                                                  \
	ereport(elevel,                                                                                \
			((err)->remote.errcode != 0 ? errcode((err)->remote.errcode) :                         \
										  errcode((err)->errcode),                                 \
			 (err)->remote.msg ?                                                                   \
				 errmsg_internal("[%s]: %s", (err)->nodename, (err)->remote.msg) :                 \
				 ((err)->connmsg ? errmsg_internal("[%s]: %s", (err)->nodename, (err)->connmsg) :  \
								   errmsg_internal("[%s]: %s", (err)->nodename, (err)->msg)),      \
			 (err)->remote.detail ? errdetail_internal("%s", (err)->remote.detail) : 0,            \
			 (err)->remote.hint ? errhint("%s", (err)->remote.hint) : 0,                           \
			 (err)->remote.sqlcmd ? errcontext("Remote SQL command: %s", (err)->remote.sqlcmd) :   \
									0))

/*
 * Report an error we got from the remote host.
 *
 * elevel: error level to use (typically ERROR, but might be less)
 * res: PGresult containing the error
 */
#define remote_result_elog(pgres, elevel)                                                          \
	do                                                                                             \
	{                                                                                              \
		PG_TRY();                                                                                  \
		{                                                                                          \
			TSConnectionError err;                                                                 \
			remote_connection_get_result_error(pgres, &err);                                       \
			remote_connection_error_elog(&err, elevel);                                            \
		}                                                                                          \
		PG_CATCH();                                                                                \
		{                                                                                          \
			PQclear(pgres);                                                                        \
			PG_RE_THROW();                                                                         \
		}                                                                                          \
		PG_END_TRY();                                                                              \
	} while (0)

#define remote_connection_elog(conn, elevel)                                                       \
	do                                                                                             \
	{                                                                                              \
		TSConnectionError err;                                                                     \
		remote_connection_get_error(conn, &err);                                                   \
		remote_connection_error_elog(&err, elevel);                                                \
	} while (0)

extern void _remote_connection_init(void);
extern void _remote_connection_fini(void);

#endif /* TIMESCALEDB_TSL_REMOTE_CONNECTION_H */
