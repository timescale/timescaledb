/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

/*
 * This file contains source code that was copied and/or modified from the
 * PostgreSQL database, which is licensed under the open-source PostgreSQL
 * License. Please see the NOTICE at the top level directory for a copy of
 * the PostgreSQL License.
 */
#include <postgres.h>

#include <access/htup_details.h>
#include <catalog/pg_user_mapping.h>
#include <catalog/pg_foreign_server.h>
#include <access/xact.h>
#include <mb/pg_wchar.h>
#include <miscadmin.h>
#include <pgstat.h>
#include <storage/latch.h>
#include <utils/hsearch.h>
#include <utils/inval.h>
#include <utils/memutils.h>
#include <utils/syscache.h>
#include <utils/uuid.h>
#include <utils/builtins.h>

#include <commands/defrem.h>
#include <export.h>
#include <telemetry/telemetry_metadata.h>
#include "connection.h"
#include "guc.h"
#include "async.h"
#include "utils.h"
#include "dist_util.h"

/* for assigning cursor numbers and prepared statement numbers */
static unsigned int cursor_number = 0;
static unsigned int prep_stmt_number = 0;

typedef struct TSConnection
{
	PGconn *pg_conn;	/* PostgreSQL connection */
	bool processing;	/* TRUE if there is ongoin Async request processing */
	NameData node_name; /* Associated data node name */
	char *tz_name;		/* Timezone name last sent over connection */
} TSConnection;

static PQconninfoOption *
get_libpq_options()
{
	/* make static to fetch once per backend */
	static PQconninfoOption *libpq_options = NULL;

	if (libpq_options == NULL)
	{
		/* Note that the options array is Malloc'ed */
		libpq_options = PQconndefaults();
	}

	if (libpq_options == NULL)
	{
		/* probably OOM */
		elog(ERROR, "could not get default libpq optionns");
	}

	return libpq_options;
}

static bool
is_libpq_option(const char *keyword, char **display_option)
{
	PQconninfoOption *lopt;

	for (lopt = get_libpq_options(); lopt->keyword; lopt++)
	{
		if (strcmp(lopt->keyword, keyword) == 0)
		{
			if (display_option != NULL)
				*display_option = lopt->dispchar;
			return true;
		}
	}
	return false;
}

ConnOptionType
remote_connection_option_type(const char *keyword)
{
	char *display_option;

	if (!is_libpq_option(keyword, &display_option))
		return CONN_OPTION_TYPE_NONE;

	/* Hide debug options, as well as settings we override internally. */
	if (strchr(display_option, 'D') || strcmp(keyword, "fallback_application_name") == 0 ||
		strcmp(keyword, "client_encoding") == 0)
		return CONN_OPTION_TYPE_NONE;

	/*
	 * "user" and any secret options are allowed only on user mappings.
	 * Everything else is a data node option.
	 */
	if (strchr(display_option, '*') || strcmp(keyword, "user") == 0)
		return CONN_OPTION_TYPE_USER;

	return CONN_OPTION_TYPE_NODE;
}

bool
remote_connection_valid_user_option(const char *keyword)
{
	return remote_connection_option_type(keyword) == CONN_OPTION_TYPE_USER;
}

bool
remote_connection_valid_node_option(const char *keyword)
{
	return remote_connection_option_type(keyword) == CONN_OPTION_TYPE_NODE;
}

static int
extract_connection_options(List *defelems, const char **keywords, const char **values)
{
	ListCell *lc;
	int i = 0;

	foreach (lc, defelems)
	{
		DefElem *d = (DefElem *) lfirst(lc);

		if (is_libpq_option(d->defname, NULL))
		{
			keywords[i] = d->defname;
			values[i] = defGetString(d);
			i++;
		}
	}
	return i;
}

/*
 * For non-superusers, insist that the connstr specify a password.  This
 * prevents a password from being picked up from .pgpass, a service file,
 * the environment, etc.  We don't want the postgres user's passwords
 * to be accessible to non-superusers.  (See also dblink_connstr_check in
 * contrib/dblink.)
 */
static void
check_conn_params(const char **keywords, const char **values)
{
	int i;

	/* no check required if superuser */
	if (superuser())
		return;

	/* ok if params contain a non-empty password */
	for (i = 0; keywords[i] != NULL; i++)
	{
		if (strcmp(keywords[i], "password") == 0 && values[i][0] != '\0')
			return;
	}

	ereport(ERROR,
			(errcode(ERRCODE_S_R_E_PROHIBITED_SQL_STATEMENT_ATTEMPTED),
			 errmsg("password is required"),
			 errdetail("Non-superusers must provide a password in the user mapping.")));
}

/*
 * Internal connection configure.
 *
 * This function will send internal configuration settings if they have
 * changed. It is used to pass on configuration settings before executing a
 * command requested by module users.
 *
 * ATTENTION! This function should *not* use
 * `remote_connection_exec_ok_command` since this function is called
 * indirectly whenever a remote command is executed, which would lead to
 * infinite recursion. Stick to `PQ*` functions.
 */
void
remote_connection_configure_if_changed(TSConnection *conn)
{
	const char *local_tz_name = pg_get_timezone_name(session_timezone);

	/*
	 * We need to enforce the same timezone setting across nodes. Otherwise,
	 * we might get the wrong result when we push down things like
	 * date_trunc(text, timestamptz). To safely do that, we also need the
	 * timezone databases to be the same on all data nodes.
	 *
	 * We save away the timezone name so that we know what we last sent over
	 * the connection. If the time zone changed since last time we sent a
	 * command, we will send a SET TIMEZONE command with the new timezone
	 * first.
	 */
	if (conn->tz_name == NULL ||
		(local_tz_name && pg_strcasecmp(conn->tz_name, local_tz_name) != 0))
	{
		char *set_timezone_cmd = psprintf("SET TIMEZONE = '%s'", local_tz_name);
		PQexec(remote_connection_get_pg_conn(conn), set_timezone_cmd);
		pfree(set_timezone_cmd);
		free(conn->tz_name);
		conn->tz_name = strdup(local_tz_name);
	}
}

/*
 * Issue SET commands to make sure remote session is configured properly.
 *
 * We do this just once at connection, assuming nothing will change the
 * values later.  Since we'll never send volatile function calls to the
 * remote, there shouldn't be any way to break this assumption from our end.
 * It's possible to think of ways to break it at the remote end, eg making a
 * foreign table point to a view that includes a set_config call --- but once
 * you admit the possibility of a malicious view definition, there are any
 * number of ways to break things.
 */
void
remote_connection_configure(TSConnection *conn)
{
	/* Timezone is indirectly set with the first command executed. See
	 * async.c */

	/* Force the search path to contain only pg_catalog (see deparse.c) */
	remote_connection_exec_ok_command(conn, "SET search_path = pg_catalog");

	/*
	 * Set values needed to ensure unambiguous data output from remote.  (This
	 * logic should match what pg_dump does.  See also set_transmission_modes
	 * in fdw.c.)
	 */
	remote_connection_exec_ok_command(conn, "SET datestyle = ISO");
	remote_connection_exec_ok_command(conn, "SET intervalstyle = postgres");
	remote_connection_exec_ok_command(conn, "SET extra_float_digits = 3");
}

static TSConnection *
remote_connection_create(PGconn *pg_conn, bool processing, const char *node_name)
{
	TSConnection *conn = malloc(sizeof(TSConnection));

	conn->pg_conn = pg_conn;
	conn->processing = processing;
	namestrcpy(&conn->node_name, node_name);
	conn->tz_name = NULL;

	return conn;
}

PGconn *
remote_connection_get_pg_conn(TSConnection *conn)
{
	Assert(conn != NULL);
	return conn->pg_conn;
}

bool
remote_connection_is_processing(TSConnection *conn)
{
	Assert(conn != NULL);
	return conn->processing;
}

void
remote_connection_set_processing(TSConnection *conn, bool processing)
{
	Assert(conn != NULL);
	conn->processing = processing;
}

/*
 * Configure remote connection using current instance UUID.
 *
 * This allows remote side to reason about whether this connection has been
 * originated by access node.
 */
static void
remote_connection_set_peer_dist_id(TSConnection *conn)
{
	PGresult *res;
	char *request;
	Datum id_string = DirectFunctionCall1(uuid_out, ts_telemetry_metadata_get_uuid());

	request = psprintf("SELECT * FROM _timescaledb_internal.set_peer_dist_id('%s')",
					   DatumGetCString(id_string));
	res = remote_connection_query_ok_result(conn, request);
	remote_connection_result_close(res);
}

static TSConnection *
remote_connection_open_internal(const char *hostname, List *host_options, List *user_options)
{
	PGconn *pg_conn = NULL;
	const char **keywords;
	const char **values;
	int n;

	/*
	 * Construct connection params from generic options of ForeignServer
	 * and UserMapping.  (Some of them might not be libpq options, in
	 * which case we'll just waste a few array slots.)  Add 3 extra slots
	 * for fallback_application_name, client_encoding, end marker.
	 */
	n = list_length(host_options) + list_length(user_options) + 3;
	keywords = (const char **) palloc(n * sizeof(char *));
	values = (const char **) palloc(n * sizeof(char *));

	n = 0;
	n += extract_connection_options(host_options, keywords + n, values + n);
	n += extract_connection_options(user_options, keywords + n, values + n);

	/* Use "timescaledb_fdw" as fallback_application_name. */
	keywords[n] = "fallback_application_name";
	values[n] = "timescaledb";
	n++;

	/* Set client_encoding so that libpq can convert encoding properly. */
	keywords[n] = "client_encoding";
	values[n] = GetDatabaseEncodingName();
	n++;

	keywords[n] = values[n] = NULL;

	/* verify connection parameters and make connection */
	check_conn_params(keywords, values);

	pg_conn = PQconnectdbParams(keywords, values, false);

	pfree(keywords);
	pfree(values);

	if (NULL == pg_conn)
		return NULL;

	return remote_connection_create(pg_conn, false, hostname);
}

/*
 * Opens a connection.
 *
 * Raw connections are not part of the transaction and do not have transactions
 * auto-started. They must be explicitly closed by
 * remote_connection_close. Note that connections are allocated using malloc
 * and so if you do not call remote_connection_close, you'll have a memory
 * leak. Note that the connection cache handles all of this for you so use
 * that if you can.
 */
TSConnection *
remote_connection_open(const char *node_name, List *node_options, List *user_options,
					   bool set_dist_id)
{
	TSConnection *volatile conn = NULL;

	/*
	 * Use PG_TRY block to ensure closing connection on error.
	 */
	PG_TRY();
	{
		conn = remote_connection_open_internal(node_name, node_options, user_options);

		if (NULL == conn)
			ereport(ERROR,
					(errcode(ERRCODE_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION),
					 errmsg("could not connect to \"%s\"", node_name)));

		Assert(NULL != conn->pg_conn);

		if (PQstatus(conn->pg_conn) != CONNECTION_OK)
			ereport(ERROR,
					(errcode(ERRCODE_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION),
					 errmsg("could not connect to \"%s\"", node_name),
					 errdetail_internal("%s", pchomp(PQerrorMessage(conn->pg_conn)))));

		/*
		 * Check that non-superuser has used password to establish connection;
		 * otherwise, he's piggybacking on the postgres server's user
		 * identity. See also dblink_security_check() in contrib/dblink.
		 */
		if (!superuser() && !PQconnectionUsedPassword(conn->pg_conn))
			ereport(ERROR,
					(errcode(ERRCODE_S_R_E_PROHIBITED_SQL_STATEMENT_ATTEMPTED),
					 errmsg("athentication required on target host"),
					 errdetail("Non-superuser cannot connect if the target host does "
							   "not request a password."),
					 errhint("Target host's authentication method must be changed.")));

		/* Prepare new session for use */
		/* TODO: should this happen in connection or session? */
		remote_connection_configure(conn);

		/* Inform remote node about instance UUID */
		if (set_dist_id)
			remote_connection_set_peer_dist_id(conn);
	}
	PG_CATCH();
	{
		/* Release PGconn data structure if we managed to create one */
		if (NULL != conn)
			remote_connection_close(conn);
		PG_RE_THROW();
	}
	PG_END_TRY();

	return conn;
}

/*
 * Open a connection using current user UserMapping. If UserMapping is not found,
 * it might succeed opening a connection if a current user is a superuser.
 */
TSConnection *
remote_connection_open_default(const char *node_name)
{
	ForeignServer *fs = GetForeignServerByName(node_name, false);
	UserMapping *um = get_user_mapping(GetUserId(), fs->serverid, true);

	/* This try block is needed as GetUserMapping throws an error rather than returning NULL if a
	 * user mapping isn't found.  The catch block allows superusers to perform this operation
	 * without a user mapping. */

	if (NULL == um)
	{
		elog(DEBUG1,
			 "UserMapping not found for user id `%u` and node `%s`. Trying to open remote "
			 "connection as superuser",
			 GetUserId(),
			 fs->servername);
	}

	return remote_connection_open(node_name, fs->options, um ? um->options : NULL, true);
}

#define PING_QUERY "SELECT 1"

bool
remote_connection_ping(const char *server_name)
{
	ForeignServer *fs = GetForeignServerByName(server_name, false);
	UserMapping *um = get_user_mapping(GetUserId(), fs->serverid, true);
	TSConnection *conn =
		remote_connection_open_internal(server_name, fs->options, um ? um->options : NULL);
	bool success = false;

	if (NULL == conn)
		return false;

	if (PQstatus(conn->pg_conn) == CONNECTION_OK)
	{
		AsyncRequest *req;
		AsyncResponseResult *rsp;
		PGresult *res;

		req = async_request_send(conn, PING_QUERY);

		Assert(NULL != req);
		rsp = async_request_wait_any_result(req);
		Assert(NULL != rsp);

		res = async_response_result_get_pg_result(rsp);

		if (PQresultStatus(res) == PGRES_TUPLES_OK)
			success = true;

		async_response_result_close(rsp);

		pfree(req);
	}

	remote_connection_close(conn);

	return success;
}

void
remote_connection_close(TSConnection *conn)
{
	Assert(conn != NULL);
	PQfinish(conn->pg_conn);
	conn->pg_conn = NULL;
	free(conn->tz_name);
	free(conn);
}

/*
 * Assign a "unique" number for a cursor.
 *
 * TODO should this be moved into the session?
 *
 * These really only need to be unique per connection within a transaction.
 * For the moment we ignore the per-connection point and assign them across
 * all connections in the transaction, but we ask for the connection to be
 * supplied in case we want to refine that.
 *
 * Note that even if wraparound happens in a very long transaction, actual
 * collisions are highly improbable; just be sure to use %u not %d to print.
 */
unsigned int
remote_connection_get_cursor_number()
{
	return ++cursor_number;
}

void
remote_connection_reset_cursor_number()
{
	cursor_number = 0;
}

/*
 * Assign a "unique" number for a prepared statement.
 *
 * This works much like remote_connection_get_cursor_number, except that we never reset the counter
 * within a session.  That's because we can't be 100% sure we've gotten rid
 * of all prepared statements on all connections, and it's not really worth
 * increasing the risk of prepared-statement name collisions by resetting.
 */
unsigned int
remote_connection_get_prep_stmt_number()
{
	return ++prep_stmt_number;
}

/*
 * Report an error we got from the remote host.
 *
 * elevel: error level to use (typically ERROR, but might be less)
 * res: PGresult containing the error
 * conn: connection we did the query on
 * clear: if true, PQclear the result (otherwise caller will handle it)
 * sql: NULL, or text of remote command we tried to execute
 *
 * Note: callers that choose not to throw ERROR for a remote error are
 * responsible for making sure that the associated ConnCacheEntry gets
 * marked with have_error = true.
 */
void
remote_connection_report_error(int elevel, PGresult *res, TSConnection *conn, bool clear,
							   const char *sql)
{
	/* If requested, PGresult must be released before leaving this function. */
	PG_TRY();
	{
		char *diag_sqlstate = PQresultErrorField(res, PG_DIAG_SQLSTATE);
		char *message_primary = PQresultErrorField(res, PG_DIAG_MESSAGE_PRIMARY);
		char *message_detail = PQresultErrorField(res, PG_DIAG_MESSAGE_DETAIL);
		char *message_hint = PQresultErrorField(res, PG_DIAG_MESSAGE_HINT);
		char *message_context = PQresultErrorField(res, PG_DIAG_CONTEXT);
		int sqlstate;

		if (diag_sqlstate)
			sqlstate = MAKE_SQLSTATE(diag_sqlstate[0],
									 diag_sqlstate[1],
									 diag_sqlstate[2],
									 diag_sqlstate[3],
									 diag_sqlstate[4]);
		else
			sqlstate = ERRCODE_CONNECTION_FAILURE;

		/*
		 * If we don't get a message from the PGresult, try the PGconn.  This
		 * is needed because for connection-level failures, PQexec may just
		 * return NULL, not a PGresult at all.
		 */
		if (message_primary == NULL)
			message_primary = pchomp(PQerrorMessage(remote_connection_get_pg_conn(conn)));

		ereport(elevel,
				(errcode(sqlstate),
				 message_primary ?
					 errmsg_internal("[%s]: %s", NameStr(conn->node_name), message_primary) :
					 errmsg("could not obtain message string for remote error"),
				 message_detail ? errdetail_internal("%s", message_detail) : 0,
				 message_hint ? errhint("%s", message_hint) : 0,
				 message_context ? errcontext("%s", message_context) : 0,
				 sql ? errcontext("Remote SQL command: %s", sql) : 0));
	}
	PG_CATCH();
	{
		if (clear)
			PQclear(res);
		PG_RE_THROW();
	}
	PG_END_TRY();
	if (clear)
		PQclear(res);
}

/*
 * Drain a connection of all data coming in and discard the results. Return
 * success if all data is drained before the deadline expires.
 *
 * This is mainly used in abort processing. This result being returned
 * might be for a query that is being interrupted by transaction abort, or it might
 * be a query that was initiated as part of transaction abort to get the remote
 * side back to the appropriate state.
 *
 * It's not a huge problem if we throw an ERROR here, but if we get into error
 * recursion trouble, we'll end up slamming the connection shut, which will
 * necessitate failing the entire toplevel transaction even if subtransactions
 * were used.  Try to use WARNING where we can.
 *
 * end_time is the time at which we should give up and assume the remote
 * side is dead.
 */

static bool
remote_connection_drain(PGconn *conn, TimestampTz endtime)
{
	for (;;)
	{
		PGresult *res;

		while (PQisBusy(conn))
		{
			int wc;
			TimestampTz now = GetCurrentTimestamp();
			long secs;
			int microsecs;
			long cur_timeout;

			/* If timeout has expired, give up, else get sleep time. */
			if (now >= endtime)
			{
				return false;
			}
			TimestampDifference(now, endtime, &secs, &microsecs);

			/* To protect against clock skew, limit sleep to one minute. */
			cur_timeout = Min(60000, secs * USECS_PER_SEC + microsecs);

			/* Sleep until there's something to do */
			wc = WaitLatchOrSocket(MyLatch,
								   WL_LATCH_SET | WL_SOCKET_READABLE | WL_TIMEOUT,
								   PQsocket(conn),
								   cur_timeout,
								   PG_WAIT_EXTENSION);
			ResetLatch(MyLatch);

			CHECK_FOR_INTERRUPTS();

			/* Data available in socket? */
			if (wc & WL_SOCKET_READABLE)
			{
				if (!PQconsumeInput(conn))
					/* connection trouble; treat the same as a timeout */
					return false;
			}
		}

		res = PQgetResult(conn);
		if (res == NULL)
			return true; /* query is complete */
		PQclear(res);
	}
	Assert(false);
}

/*
 * Cancel the currently-in-progress query and ignore the result.  Returns true if we successfully
 * cancel the query and discard any pending result, and false if not.
 */
bool
remote_connection_cancel_query(TSConnection *conn)
{
	PGcancel *cancel;
	char errbuf[256];
	TimestampTz endtime;

	if (!conn)
		return true;

	/*
	 * If it takes too long to cancel the query and discard the result, assume
	 * the connection is dead.
	 */
	endtime = TimestampTzPlusMilliseconds(GetCurrentTimestamp(), 30000);

	/* We assume that processing is over no matter if cancel completes
	 * successfully or not. */
	remote_connection_set_processing(conn, false);

	/*
	 * Issue cancel request.  Unfortunately, there's no good way to limit the
	 * amount of time that we might block inside PQgetCancel().
	 */
	if ((cancel = PQgetCancel(conn->pg_conn)))
	{
		if (!PQcancel(cancel, errbuf, sizeof(errbuf)))
		{
			ereport(WARNING,
					(errcode(ERRCODE_CONNECTION_FAILURE),
					 errmsg("could not send cancel request: %s", errbuf)));
			PQfreeCancel(cancel);
			return false;
		}
		PQfreeCancel(cancel);
	}

	return remote_connection_drain(conn->pg_conn, endtime);
}
