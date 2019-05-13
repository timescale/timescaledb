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
#include "dist_util.h"

/* for assigning cursor numbers and prepared statement numbers */
static unsigned int cursor_number = 0;
static unsigned int prep_stmt_number = 0;

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
	 * Everything else is a server option.
	 */
	if (strchr(display_option, '*') || strcmp(keyword, "user") == 0)
		return CONN_OPTION_TYPE_USER;

	return CONN_OPTION_TYPE_SERVER;
}

bool
remote_connection_valid_user_option(const char *keyword)
{
	return remote_connection_option_type(keyword) == CONN_OPTION_TYPE_USER;
}

bool
remote_connection_valid_server_option(const char *keyword)
{
	return remote_connection_option_type(keyword) == CONN_OPTION_TYPE_SERVER;
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
remote_connection_configure(PGconn *conn)
{
	/* Force the search path to contain only pg_catalog (see deparse.c) */
	remote_connection_exec_ok_command(conn, "SET search_path = pg_catalog");
	/*
	 * Set remote timezone; this is basically just cosmetic, since all
	 * transmitted and returned timestamptzs should specify a zone explicitly
	 * anyway.  However it makes the regression test outputs more predictable.
	 *
	 * We don't risk setting remote zone equal to ours, since the remote
	 * server might use a different timezone database.  Instead, use UTC
	 * (quoted, because very old servers are picky about case).
	 */
	remote_connection_exec_ok_command(conn, "SET timezone = 'UTC'");

	/*
	 * Set values needed to ensure unambiguous data output from remote.  (This
	 * logic should match what pg_dump does.  See also set_transmission_modes
	 * in fdw.c.)
	 */
	remote_connection_exec_ok_command(conn, "SET datestyle = ISO");
	remote_connection_exec_ok_command(conn, "SET intervalstyle = postgres");
	remote_connection_exec_ok_command(conn, "SET extra_float_digits = 3");
}

/*
 * Configure remote connection using current instance UUID.
 *
 * This allows remote side to reason about whether this connection has been
 * originated by frontend server.
 */
static void
remote_connection_set_peer_dist_id(PGconn *conn)
{
	PGresult *res;
	char *request;
	Datum id_string = DirectFunctionCall1(uuid_out, ts_telemetry_metadata_get_uuid());

	request = psprintf("SELECT * FROM _timescaledb_internal.set_peer_dist_id('%s')",
					   DatumGetCString(id_string));
	res = remote_connection_query_ok_result(conn, request);
	remote_connection_result_close(res);
}

/*
 * Opens a connection.
 *
 *  Raw connection are not part of the transaction and do not have transactions auto-started.
 *  They must be explicitly closed by remote_connection_close. Note that connections are allocated
 * using malloc and so if you do not call remote_connection_close, you'll have a memory leak. Note
 * that the connection cache handles all of this for you so use that if you can.
 */

PGconn *
remote_connection_open(char *server_name, List *server_options, List *user_options,
					   bool set_dist_id)
{
	PGconn *volatile conn = NULL;

	/*
	 * Use PG_TRY block to ensure closing connection on error.
	 */
	PG_TRY();
	{
		const char **keywords;
		const char **values;
		int n;

		/*
		 * Construct connection params from generic options of ForeignServer
		 * and UserMapping.  (Some of them might not be libpq options, in
		 * which case we'll just waste a few array slots.)  Add 3 extra slots
		 * for fallback_application_name, client_encoding, end marker.
		 */
		n = list_length(server_options) + list_length(user_options) + 3;
		keywords = (const char **) palloc(n * sizeof(char *));
		values = (const char **) palloc(n * sizeof(char *));

		n = 0;
		n += extract_connection_options(server_options, keywords + n, values + n);
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

		conn = PQconnectdbParams(keywords, values, false);
		if (!conn || PQstatus(conn) != CONNECTION_OK)
			ereport(ERROR,
					(errcode(ERRCODE_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION),
					 errmsg("could not connect to server \"%s\"", server_name),
					 errdetail_internal("%s", pchomp(PQerrorMessage(conn)))));

		/*
		 * Check that non-superuser has used password to establish connection;
		 * otherwise, he's piggybacking on the postgres server's user
		 * identity. See also dblink_security_check() in contrib/dblink.
		 */
		if (!superuser() && !PQconnectionUsedPassword(conn))
			ereport(ERROR,
					(errcode(ERRCODE_S_R_E_PROHIBITED_SQL_STATEMENT_ATTEMPTED),
					 errmsg("password is required"),
					 errdetail(
						 "Non-superuser cannot connect if the server does not request a password."),
					 errhint("Target server's authentication method must be changed.")));

		/* Prepare new session for use */
		/* TODO: should this happen in connection or session? */
		remote_connection_configure(conn);

		/* Inform remote server about instance UUID */
		if (set_dist_id)
			remote_connection_set_peer_dist_id(conn);

		pfree(keywords);
		pfree(values);
	}
	PG_CATCH();
	{
		/* Release PGconn data structure if we managed to create one */
		if (conn)
			PQfinish(conn);
		PG_RE_THROW();
	}
	PG_END_TRY();

	return conn;
}

/*
 * Open a connection using current user UserMapping. If UserMapping is not found,
 * it might succeed opening a connection if a current user is a superuser.
 */
PGconn *
remote_connection_open_default(char *server_name)
{
	ForeignServer *fs = GetForeignServerByName(server_name, false);
	volatile UserMapping *um = NULL;

	/* This try block is needed as GetUserMapping throws an error rather than returning NULL if a
	 * user mapping isn't found.  The catch block allows superusers to perform this operation
	 * without a user mapping. */
	PG_TRY();
	{
		um = GetUserMapping(GetUserId(), fs->serverid);
	}
	PG_CATCH();
	{
		elog(DEBUG1,
			 "UserMapping not found for user id `%u` and server `%s`. Trying to open remote "
			 "connection as superuser",
			 GetUserId(),
			 fs->servername);
		um = NULL;
		FlushErrorState();
	}
	PG_END_TRY();

	return remote_connection_open(server_name, fs->options, um ? um->options : NULL, true);
}

void
remote_connection_close(PGconn *conn)
{
	PQfinish(conn);
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
 * Report an error we got from the remote server.
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
remote_connection_report_error(int elevel, PGresult *res, PGconn *conn, bool clear, const char *sql)
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
			message_primary = pchomp(PQerrorMessage(conn));

		ereport(elevel,
				(errcode(sqlstate),
				 message_primary ? errmsg_internal("remote connection error: %s", message_primary) :
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
remote_connection_cancel_query(PGconn *conn)
{
	PGcancel *cancel;
	char errbuf[256];
	TimestampTz endtime;

	/*
	 * If it takes too long to cancel the query and discard the result, assume
	 * the connection is dead.
	 */
	endtime = TimestampTzPlusMilliseconds(GetCurrentTimestamp(), 30000);

	/*
	 * Issue cancel request.  Unfortunately, there's no good way to limit the
	 * amount of time that we might block inside PQgetCancel().
	 */
	if ((cancel = PQgetCancel(conn)))
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

	return remote_connection_drain(conn, endtime);
}
