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
#include <access/xact.h>
#include <access/reloptions.h>
#include <catalog/pg_foreign_server.h>
#include <catalog/pg_user_mapping.h>
#include <commands/defrem.h>
#include <foreign/foreign.h>
#include <libpq-events.h>
#include <libpq/libpq.h>
#include <mb/pg_wchar.h>
#include <miscadmin.h>
#include <nodes/makefuncs.h>
#include <port.h>
#include <postmaster/postmaster.h>
#include <storage/latch.h>
#include <utils/builtins.h>
#include <utils/fmgrprotos.h>
#include <utils/inval.h>
#include <utils/guc.h>
#include <utils/syscache.h>

#include <annotations.h>
#include <dist_util.h>
#include <errors.h>
#include <extension_constants.h>
#include <guc.h>
#ifdef USE_TELEMETRY
#include <telemetry/telemetry_metadata.h>
#endif
#include "connection.h"
#include "data_node.h"
#include "debug_point.h"
#include "utils.h"
#include "ts_catalog/metadata.h"
#include "config.h"

/*
 * Connection library for TimescaleDB.
 *
 * This library file contains convenience functionality around the libpq
 * API. The major additional functionality offered includes:
 *
 * - Lifecycle management: a connection is tied to the memory context it is
 *   created in and result objects are tied to the connection they are created
 *   from. The aim is to avoid memory leaks of libpq objects that aren't
 *   allocated on a PostgreSQL memory context.
 *
 * - Connection configuration suitable for TimescaleDB, ensuring the data
 *	 nodes use the same relevant configurations as the access node (e.g., time
 *	 zone).
 *
 * NOTE that it is strongly adviced that connection-related functions do not
 * throw exceptions with, e.g., elog(ERROR). While exceptions can be caught
 * with PG_TRY-CATCH for cleanup, it is not possible to safely continue the
 * transaction that threw the exception as if no error occurred (see the
 * following post if unconvinced:
 * https://www.postgresql.org/message-id/27190.1508727890%40sss.pgh.pa.us).
 *
 * In some cases, we need to be able to continue a transaction even if a
 * connection fails. One example is the removal of a data node, which must be
 * able to proceed even if the node is no longer available to respond to a
 * connection. Another example is performing a liveness check for node status.
 *
 * Therefore, it is best to defer throwing exceptions to high-level functions
 * that know when it is appropriate.
 */

/* for assigning cursor numbers and prepared statement numbers */
static unsigned int cursor_number = 0;
static unsigned int prep_stmt_number = 0;
static RemoteConnectionStats connstats = { 0 };

static int eventproc(PGEventId eventid, void *eventinfo, void *data);

TSConnectionId
remote_connection_id(const Oid server_oid, const Oid user_oid)
{
	TSConnectionId id = { .server_id = server_oid, .user_id = user_oid };
	return id;
}

void
remote_connection_id_set(TSConnectionId *const id, Oid const server_oid, Oid const user_oid)
{
	id->server_id = server_oid;
	id->user_id = user_oid;
}

/*
 * A simple circular list implementation for tracking libpq connection and
 * result objects. We can't use pg_list here since it is bound to PostgreSQL's
 * memory management system, while libpq is not.
 */
typedef struct ListNode
{
	struct ListNode *next;
	struct ListNode *prev;
} ListNode;

#define IS_DETACHED_ENTRY(entry) ((entry)->next == NULL && (entry)->prev == NULL)

/*
 * Detach a list node.
 *
 * Detaches a list node from the list, unless it is the anchor/head (which is
 * a no-op).
 */
static inline void
list_detach(ListNode *entry)
{
	ListNode *prev = entry->prev;
	ListNode *next = entry->next;

	next->prev = prev;
	prev->next = next;
	/* Clear entry fields */
	entry->prev = NULL;
	entry->next = NULL;
}

/*
 * Insert a list node entry after the prev node.
 */
static inline void
list_insert_after(ListNode *entry, ListNode *prev)
{
	ListNode *next = prev->next;

	next->prev = entry;
	entry->next = next;
	entry->prev = prev;
	prev->next = entry;
}

/*
 * List entry that holds a PGresult object.
 */
typedef struct ResultEntry
{
	struct ListNode ln; /* Must be first entry */
	TSConnection *conn; /* The connection the result was created on */
	PGresult *result;
} ResultEntry;

typedef struct TSConnection
{
	ListNode ln;	 /* Must be first entry */
	PGconn *pg_conn; /* PostgreSQL connection */
	TSConnectionStatus status;
	NameData node_name;				 /* Associated data node name */
	char tz_name[TZ_STRLEN_MAX + 1]; /* Timezone name last sent over connection */
	int xact_depth;					 /* 0 => no transaction, 1 => main transaction, > 1 =>
									  * levels of subtransactions */
	bool xact_transitioning;		 /* TRUE if connection is transitioning to
									  * another transaction state */
	ListNode results;				 /* Head of PGresult list */
	bool binary_copy;
	MemoryContext mcxt;
	MemoryContextCallback mcxt_cb;
	bool mcxt_cb_invoked;
	WaitEventSet *wes;
	int sockeventpos;
} TSConnection;

/*
 * List of all connections we create. Used to auto-free connections and/or
 * PGresults at transaction end.
 */
static ListNode connections = { &connections, &connections };

static bool
fill_simple_error(TSConnectionError *err, int errcode, const char *errmsg, const TSConnection *conn)
{
	if (NULL == err)
		return false;

	MemSet(err, 0, sizeof(*err));

	err->errcode = errcode;
	err->msg = errmsg;
	if (err->msg == NULL || strlen(err->msg) == 0)
	{
		char *connection_message = PQerrorMessage(conn->pg_conn);
		if (connection_message)
		{
			err->msg = pstrdup(connection_message);
		}
		else
		{
			err->msg = "unknown error";
		}
	}
	err->host = pstrdup(PQhost(conn->pg_conn));
	err->nodename = pstrdup(remote_connection_node_name(conn));

	return false;
}

static bool
fill_connection_error(TSConnectionError *err, int errcode, const char *errmsg,
					  const TSConnection *conn)
{
	if (NULL == err)
		return false;

	fill_simple_error(err, errcode, errmsg, conn);
	/* The connection error from the remote side contains its own ERROR prefix
	 * and is ended by a newline. Make sure to strip that before emitting a
	 * local error. */
	err->connmsg = pchomp(PQerrorMessage(conn->pg_conn));

	if (strncmp("ERROR:  ", err->connmsg, 8) == 0)
		err->connmsg += 8;

	return false;
}

static char *
get_error_field_copy(const PGresult *res, int fieldcode)
{
	const char *msg = PQresultErrorField(res, fieldcode);

	if (NULL == msg)
		return NULL;
	return pchomp(msg);
}

/*
 * Convert libpq error severity to local error level.
 */
static int
severity_to_elevel(const char *severity)
{
	/* According to https://www.postgresql.org/docs/current/libpq-exec.html,
	 * libpq only returns the severity levels listed below. */
	static const struct
	{
		const char *severity;
		int elevel;
	} severity_levels[] = { {
								.severity = "ERROR",
								.elevel = ERROR,
							},
							{
								.severity = "FATAL",
								.elevel = FATAL,
							},
							{
								.severity = "PANIC",
								.elevel = PANIC,
							},
							{
								.severity = "WARNING",
								.elevel = WARNING,
							},
							{
								.severity = "NOTICE",
								.elevel = NOTICE,
							},
							{
								.severity = "DEBUG",
								.elevel = DEBUG1,
							},
							{
								.severity = "INFO",
								.elevel = INFO,
							},
							{
								.severity = "LOG",
								.elevel = LOG,
							},
							/* End marker */
							{
								.severity = NULL,
								.elevel = 0,
							} };
	int i;

	if (NULL == severity)
		return 0;

	i = 0;

	while (NULL != severity_levels[i].severity)
	{
		if (strcmp(severity_levels[i].severity, severity) == 0)
			return severity_levels[i].elevel;
		i++;
	}

	pg_unreachable();

	return ERROR;
}

/*
 * Fill a connection error based on the result of a remote query.
 */
static bool
fill_result_error(TSConnectionError *err, int errcode, const char *errmsg, const PGresult *res)
{
	const ResultEntry *entry = PQresultInstanceData(res, eventproc);
	const char *sqlstate;

	if (NULL == err || NULL == res || NULL == entry)
	{
		if (err)
		{
			MemSet(err, 0, sizeof(*err));
			err->errcode = errcode;
			err->msg = errmsg;
			err->nodename = "";
		}
		return false;
	}

	Assert(entry->conn);

	fill_simple_error(err, errcode, errmsg, entry->conn);
	err->remote.elevel = severity_to_elevel(PQresultErrorField(res, PG_DIAG_SEVERITY_NONLOCALIZED));
	err->remote.sqlstate = get_error_field_copy(res, PG_DIAG_SQLSTATE);
	err->remote.msg = get_error_field_copy(res, PG_DIAG_MESSAGE_PRIMARY);
	err->remote.detail = get_error_field_copy(res, PG_DIAG_MESSAGE_DETAIL);
	err->remote.hint = get_error_field_copy(res, PG_DIAG_MESSAGE_HINT);
	err->remote.context = get_error_field_copy(res, PG_DIAG_CONTEXT);
	err->remote.stmtpos = get_error_field_copy(res, PG_DIAG_STATEMENT_POSITION);
	/*
	 * Try to find at least some non-empty error message. The result error
	 * message may be not set if a node segfaults.
	 */
	if (err->remote.msg == NULL || strlen(err->remote.msg) == 0)
	{
		char *result_message = PQresultErrorMessage(res);
		if (result_message && strlen(result_message))
		{
			err->remote.msg = pstrdup(result_message);
		}
	}

	if (err->remote.msg == NULL || strlen(err->remote.msg) == 0)
	{
		char *connection_message = PQerrorMessage(entry->conn->pg_conn);
		if (connection_message && strlen(connection_message))
		{
			err->remote.msg = pstrdup(connection_message);
		}
	}

	if (err->remote.msg == NULL || strlen(err->remote.msg) == 0)
	{
		err->remote.msg = "unknown error";
	}

	sqlstate = err->remote.sqlstate;

	if (sqlstate && strlen(sqlstate) == 5)
		err->remote.errcode =
			MAKE_SQLSTATE(sqlstate[0], sqlstate[1], sqlstate[2], sqlstate[3], sqlstate[4]);
	else
		err->remote.errcode = ERRCODE_INTERNAL_ERROR;

	return false;
}

/*
 * The following event handlers make sure all PGresult are freed with
 * PQClear() when its parent connection is closed.
 *
 * It is still recommended to explicitly call PGclear() or
 * remote_connection_result_close(), however, especially when PGresults are
 * created in a tight loop (e.g., when scanning many tuples on a remote
 * table).
 */
#define EVENTPROC_FAILURE 0
#define EVENTPROC_SUCCESS 1

/*
 * Invoked on PQfinish(conn). Frees all PGresult objects created on the
 * connection, apart from those already freed with PQclear().
 */
static int
handle_conn_destroy(PGEventConnDestroy *event)
{
	TSConnection *conn = PQinstanceData(event->conn, eventproc);
	unsigned int results_count = 0;
	ListNode *curr;

	Assert(NULL != conn);

	curr = conn->results.next;

	while (curr != &conn->results)
	{
		ResultEntry *entry = (ResultEntry *) curr;
		PGresult *result = entry->result;

		curr = curr->next;
		PQclear(result);
		/* No need to free curr here since PQclear will invoke
		 * handle_result_destroy() which will free it */
		results_count++;
	}

	if (results_count > 0)
		elog(DEBUG3, "cleared %u result objects on connection %p", results_count, conn);

	connstats.connections_closed++;

	conn->pg_conn = NULL;
	list_detach(&conn->ln);

	FreeWaitEventSet(conn->wes);

	/* No need to delete the memory context here if handler was invoked by the
	 * MemoryContextDelete callback */
	if (!conn->mcxt_cb_invoked)
		MemoryContextDelete(conn->mcxt);

	return EVENTPROC_SUCCESS;
}

/*
 * Invoked on PQgetResult(conn). Adds the PGresult to the list in the parent
 * TSConnection.
 */
static int
handle_result_create(PGEventResultCreate *event)
{
	TSConnection *conn = PQinstanceData(event->conn, eventproc);
	ResultEntry *entry;

	Assert(NULL != conn);
	entry = MemoryContextAllocZero(conn->mcxt, sizeof(ResultEntry));

	if (NULL == entry)
		return EVENTPROC_FAILURE;

	entry->ln.next = entry->ln.prev = NULL;
	entry->conn = conn;
	entry->result = event->result;
	/* Add entry as new head and set instance data */
	list_insert_after(&entry->ln, &conn->results);
	PQresultSetInstanceData(event->result, eventproc, entry);

	elog(DEBUG3, "created result %p on connection %p", event->result, conn);

	connstats.results_created++;

	return EVENTPROC_SUCCESS;
}

/*
 * Invoked on PQclear(result). Removes the PGresult from the list in the
 * parent TSConnection.
 */
static int
handle_result_destroy(PGEventResultDestroy *event)
{
	ResultEntry *entry = PQresultInstanceData(event->result, eventproc);

	Assert(NULL != entry);

	/* Detach entry */
	list_detach(&entry->ln);

	elog(DEBUG3, "destroyed result %p", entry->result);

	pfree(entry);

	connstats.results_cleared++;

	return EVENTPROC_SUCCESS;
}

/*
 * Main event handler invoked when events happen on a PGconn.
 *
 * According to the libpq API, the function should return a non-zero value if
 * it succeeds and zero if it fails. We use EVENTPROC_SUCCESS and
 * EVENTPROC_FAILURE in place of these two options.
 */
static int
eventproc(PGEventId eventid, void *eventinfo, void *data)
{
	int res = EVENTPROC_SUCCESS;

	switch (eventid)
	{
		case PGEVT_CONNDESTROY:
			res = handle_conn_destroy((PGEventConnDestroy *) eventinfo);
			break;
		case PGEVT_RESULTCREATE:
			res = handle_result_create((PGEventResultCreate *) eventinfo);
			break;
		case PGEVT_RESULTDESTROY:
			res = handle_result_destroy((PGEventResultDestroy *) eventinfo);
			break;
		case PGEVT_RESULTCOPY:
			/* Not used in the code, so not handled */
			Assert(false);
			break;
		default:
			/* Not of interest, so return success */
			break;
	}

	return res;
}

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
		elog(ERROR, "could not get default libpq options");
	}

	return libpq_options;
}

static void
unset_libpq_envvar(void)
{
	PQconninfoOption *lopt;
	PQconninfoOption *options = PQconndefaults();

	TS_OOM_CHECK(options, "out of memory");

	/* Explicitly unset all libpq environment variables.
	 *
	 * By default libpq uses environment variables as a fallback
	 * to specify connection options, potentially they could be in
	 * a conflict with PostgreSQL variables and introduce
	 * security risks.
	 */
	for (lopt = options; lopt->keyword; lopt++)
	{
		if (lopt->envvar)
			unsetenv(lopt->envvar);
	}

	PQconninfoFree(options);
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
extract_connection_options(List *defelems, const char **keywords, const char **values,
						   const char **user)
{
	ListCell *lc;
	int option_pos = 0;

	Assert(keywords != NULL);
	Assert(values != NULL);
	Assert(user != NULL);

	*user = NULL;
	foreach (lc, defelems)
	{
		DefElem *d = (DefElem *) lfirst(lc);

		if (is_libpq_option(d->defname, NULL))
		{
			keywords[option_pos] = d->defname;
			values[option_pos] = defGetString(d);
			if (strcmp(d->defname, "user") == 0)
			{
				Assert(*user == NULL);
				*user = values[option_pos];
			}
			option_pos++;
		}
	}

	return option_pos;
}

static bool
prepend_enforced_conn_settings(TSConnection *conn, StringInfo cmdbuf)
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
	if (conn->tz_name[0] == '\0' ||
		(local_tz_name && pg_strcasecmp(conn->tz_name, local_tz_name) != 0))
	{
		StringInfo newcmd = makeStringInfo();

		strncpy(conn->tz_name, local_tz_name, TZ_STRLEN_MAX);
		appendStringInfo(newcmd, "SET TIMEZONE = '%s'", local_tz_name);

		if (cmdbuf->len > 0)
			appendStringInfo(newcmd, ";%s", cmdbuf->data);

		*cmdbuf = *newcmd;

		return true;
	}

	return false;
}

/*
 * Internal connection configure.
 *
 * This function will send internal configuration settings if they have
 * changed. It is used to pass on configuration settings before executing a
 * command requested by module users.
 *
 * Returns true if the current configuration is OK (no change) or was
 * successfully applied, otherwise false.
 */
bool
remote_connection_configure_if_changed(TSConnection *conn)
{
	StringInfoData cmd = {
		.data = NULL,
		.len = 0,
		.maxlen = 0,
	};
	bool success = true;

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
	if (prepend_enforced_conn_settings(conn, &cmd))
	{
		PGresult *result = remote_connection_exec(conn, cmd.data);
		success = (PQresultStatus(result) == PGRES_COMMAND_OK);
		PQclear(result);
	}

	return success;
}

/*
 * Default options/commands to set on every new connection.
 *
 * Timezone is indirectly set with the first command executed.
 */
static const char *default_connection_options[] = {
	/*
	 * Force the search path to contain only pg_catalog, which will force
	 * functions to output fully qualified identifier names (i.e., they will
	 * include the schema).
	 */
	"SET search_path = pg_catalog",
	/*
	 * Set values needed to ensure unambiguous data output from remote.  (This
	 * logic should match what pg_dump does.  See also set_transmission_modes
	 * in fdw.c.)
	 */
	"SET datestyle = ISO",
	"SET intervalstyle = postgres",
	"SET extra_float_digits = 3",
	/*
	 * Prepared statement data fetcher sets it to "force" which might be
	 * suboptimal for other kinds of queries.
	 */
	"RESET plan_cache_mode",
	"SET statement_timeout = 0",
	NULL,
};

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
bool
remote_connection_configure(TSConnection *conn)
{
	const char *cmd;
	StringInfoData sql;
	PGresult *result;
	bool success = true;
	int i = 0;

	initStringInfo(&sql);

	while ((cmd = default_connection_options[i]) != NULL)
	{
		appendStringInfo(&sql, "%s;", cmd);
		i++;
	}

	result = remote_connection_exec(conn, sql.data);
	success = PQresultStatus(result) == PGRES_COMMAND_OK;
	PQclear(result);
	pfree(sql.data);

	return success;
}

static void
connection_memcxt_reset_cb(void *arg)
{
	TSConnection *conn = arg;

	conn->mcxt_cb_invoked = true;

	/* Close the connection and free all attached resources, unless already
	 * closed explicitly before being freed. */
	if (conn->pg_conn != NULL)
		PQfinish(conn->pg_conn);
}

/*
 * Create a new connection.
 *
 * The returned connection object is allocated on the current memory context
 * and is tied to its life-cycle. The connection object includes natively
 * allocated memory from libpq (via malloc) which will be freed via callbacks
 * when the main memory context is freed.
 */
static TSConnection *
remote_connection_create(PGconn *pg_conn, bool processing, const char *node_name)
{
	MemoryContext mcxt =
		AllocSetContextCreate(CurrentMemoryContext, "TSConnection", ALLOCSET_SMALL_SIZES);
	TSConnection *conn = MemoryContextAllocZero(mcxt, sizeof(TSConnection));
	int ret;

	/* Must register the event procedure before attaching any instance data */
	ret = PQregisterEventProc(pg_conn, eventproc, "remote connection", conn);

	if (ret == 0)
	{
		MemoryContextDelete(mcxt);
		return NULL;
	}

	ret = PQsetInstanceData(pg_conn, eventproc, conn);
	Assert(ret != 0);

	conn->ln.next = conn->ln.prev = NULL;
	conn->pg_conn = pg_conn;
	remote_connection_set_status(conn, processing ? CONN_PROCESSING : CONN_IDLE);
	namestrcpy(&conn->node_name, node_name);
	conn->tz_name[0] = '\0';
	conn->xact_depth = 0;
	conn->xact_transitioning = false;
	/* Initialize results head */
	conn->results.next = &conn->results;
	conn->results.prev = &conn->results;
	conn->binary_copy = false;
	conn->mcxt = mcxt;
	conn->wes = CreateWaitEventSet(mcxt, 3);
	AddWaitEventToSet(conn->wes, WL_LATCH_SET, PGINVALID_SOCKET, MyLatch, NULL);
	AddWaitEventToSet(conn->wes, WL_EXIT_ON_PM_DEATH, PGINVALID_SOCKET, NULL, NULL);
	/* Register the socket to get the position in the events array. The actual
	 * events used here does not matter, since it will be modified as
	 * appropriate when needed. */
	conn->sockeventpos =
		AddWaitEventToSet(conn->wes, WL_SOCKET_READABLE, PQsocket(conn->pg_conn), NULL, NULL);

	/* Register a memory context callback that will ensure the connection is
	 * always closed and the resources are freed */
	conn->mcxt_cb.func = connection_memcxt_reset_cb;
	conn->mcxt_cb.arg = conn;
	MemoryContextRegisterResetCallback(mcxt, &conn->mcxt_cb);
	list_insert_after(&conn->ln, &connections);
	elog(DEBUG3, "created connection %p", conn);
	connstats.connections_created++;

	return conn;
}

int
remote_connection_xact_depth_get(const TSConnection *conn)
{
	Assert(conn->xact_depth >= 0);
	return conn->xact_depth;
}

int
remote_connection_xact_depth_inc(TSConnection *conn)
{
	Assert(conn->xact_depth >= 0);
	return ++conn->xact_depth;
}

int
remote_connection_xact_depth_dec(TSConnection *conn)
{
	Assert(conn->xact_depth > 0);
	return --conn->xact_depth;
}

void
remote_connection_xact_transition_begin(TSConnection *conn)
{
	Assert(!conn->xact_transitioning);
	conn->xact_transitioning = true;
}

void
remote_connection_xact_transition_end(TSConnection *conn)
{
	Assert(conn->xact_transitioning);
	conn->xact_transitioning = false;
}

bool
remote_connection_xact_is_transitioning(const TSConnection *conn)
{
	return conn->xact_transitioning;
}

PGconn *
remote_connection_get_pg_conn(const TSConnection *conn)
{
	Assert(conn != NULL);
	return conn->pg_conn;
}

bool
remote_connection_is_processing(const TSConnection *conn)
{
	Assert(conn != NULL);
	return conn->status != CONN_IDLE;
}

void
remote_connection_set_status(TSConnection *conn, TSConnectionStatus status)
{
	Assert(conn != NULL);
	conn->status = status;

	/* Should be blocking except when doing COPY. */
	Assert(PQisnonblocking(conn->pg_conn) == (conn->status == CONN_COPY_IN));
}

TSConnectionStatus
remote_connection_get_status(const TSConnection *conn)
{
	return conn->status;
}

const char *
remote_connection_node_name(const TSConnection *conn)
{
#ifndef NDEBUG
	const char *hide_node_name =
		GetConfigOption("timescaledb.hide_data_node_name_in_errors", true, false);
	if (hide_node_name && strcmp(hide_node_name, "on") == 0)
	{
		return "<hidden node name>";
	}
#endif
	return NameStr(conn->node_name);
}

void
remote_connection_get_error(const TSConnection *conn, TSConnectionError *err)
{
	fill_connection_error(err, ERRCODE_CONNECTION_FAILURE, NULL, conn);
}

void
remote_connection_get_result_error(const PGresult *res, TSConnectionError *err)
{
	fill_result_error(err, ERRCODE_CONNECTION_EXCEPTION, NULL, res);
}

static long
timeout_diff_ms(TimestampTz endtime)
{
	TimestampTz now;
	long secs;
	int microsecs;

	if (endtime == TS_NO_TIMEOUT)
		return -1;

	now = GetCurrentTimestamp();
	if (now >= endtime)
		return 0;

	TimestampDifference(now, endtime, &secs, &microsecs);
	return secs * 1000 + (microsecs / 1000);
}

PGresult *
remote_connection_get_result(const TSConnection *conn, TimestampTz endtime)
{
	PGresult *pgres = NULL;
	int busy = 1;

	do
	{
		CHECK_FOR_INTERRUPTS();

		busy = PQisBusy(conn->pg_conn);

		if (busy == 1)
		{
			uint32 events;
			WaitEvent event;
			long timeout_ms;
			int ret;

			events = WL_SOCKET_READABLE;
			if (endtime != TS_NO_TIMEOUT)
				events |= WL_TIMEOUT;
			timeout_ms = timeout_diff_ms(endtime);

			/* Busy, wait for readable */
			ModifyWaitEvent(conn->wes, conn->sockeventpos, events, NULL);
			ret = WaitEventSetWait(conn->wes, timeout_ms, &event, 1, PG_WAIT_EXTENSION);

			/* Timeout */
			if (ret == 0)
				break;

			if (event.events & WL_LATCH_SET)
			{
				/* Check for interrupts at top of the loop */
				ResetLatch(MyLatch);
			}

			if (event.events & WL_SOCKET_READABLE)
			{
				Assert(event.pos == conn->sockeventpos);
				Assert(event.fd == PQsocket(conn->pg_conn));

				if (PQconsumeInput(conn->pg_conn) == 0)
				{
					pgres = PQmakeEmptyPGresult(conn->pg_conn, PGRES_FATAL_ERROR);
					TS_OOM_CHECK(pgres, "out of memory");
					PQfireResultCreateEvents(conn->pg_conn, pgres);
					return pgres;
				}
			}
		}
		else if (busy == 0)
		{
			/* PQgetResult would not block */
			pgres = PQgetResult(conn->pg_conn);
		}
		else
		{
			pg_unreachable();
			Assert(false);
		}
	} while (busy == 1);

	return pgres;
}

/*
 * Execute a remote command.
 *
 * The execution blocks until a result is received or a failure occurs. Unlike
 * PQexec() and PQexecParams(), however, this function observes PostgreSQL
 * interrupts (e.g., a query is canceled). Like PQexecParams(), the PGresult
 * returned describes only the last command executed in a multi-command
 * string.
 */
PGresult *
remote_connection_exec_timeout(TSConnection *conn, const char *cmd, TimestampTz endtime)
{
	WaitEvent event;
	PGresult *res = NULL;
	int ret = 0;
	size_t cmdlen = strlen(cmd);
	StringInfoData cmd_buf = {
		.data = (char *) cmd,
		.len = cmdlen,
		.maxlen = cmdlen + 1,
	};

	prepend_enforced_conn_settings(conn, &cmd_buf);

	do
	{
		uint32 events;
		long timeout_ms;

		CHECK_FOR_INTERRUPTS();

		events = WL_SOCKET_WRITEABLE;
		if (endtime != TS_NO_TIMEOUT)
			events |= WL_TIMEOUT;
		timeout_ms = timeout_diff_ms(endtime);

		/* Wait for writable socket in outer loop */
		ModifyWaitEvent(conn->wes, conn->sockeventpos, events, NULL);
		ret = WaitEventSetWait(conn->wes, timeout_ms, &event, 1, PG_WAIT_EXTENSION);

		/* Timeout */
		if (ret == 0)
			break;

		if (event.events & WL_LATCH_SET)
		{
			ResetLatch(MyLatch);
			CHECK_FOR_INTERRUPTS();
		}
		if (event.events & WL_SOCKET_WRITEABLE)
		{
			PGresult *last_result;

			ret = PQsendQuery(conn->pg_conn, cmd_buf.data);

			if (ret == 0)
			{
				res = PQmakeEmptyPGresult(conn->pg_conn, PGRES_FATAL_ERROR);
				TS_OOM_CHECK(res, "out of memory");
				PQfireResultCreateEvents(conn->pg_conn, res);
				return res;
			}

			/* Command sent, so now wait for readable result in inner loop */
			last_result = NULL;

			/*
			 * Read all results, but return only one in order to recreate the
			 * behavior of the blocking PQexec() call. We need to handle
			 * PG12,13 differently to be compatible across all versions of
			 * PostgreSQL. In libpq's PQexec() errors from all the results are
			 * concactinated, but that is not possible here due to lack of
			 * access to internals. PG14 handles that automatically, however.
			 */
			while ((res = remote_connection_get_result(conn, endtime)) != NULL)
			{
				if (last_result)
				{
#if PG14_LT
					if (PQresultStatus(last_result) == PGRES_FATAL_ERROR &&
						PQresultStatus(res) == PGRES_FATAL_ERROR)
					{
						PQclear(res);
						res = last_result;
					}
					else
						PQclear(last_result);
#else
					PQclear(last_result);
#endif
				}

				last_result = res;

				if (PQresultStatus(res) == PGRES_COPY_IN || PQresultStatus(res) == PGRES_COPY_OUT ||
					PQresultStatus(res) == PGRES_COPY_BOTH ||
					PQstatus(conn->pg_conn) == CONNECTION_BAD)
					break;
			}

			res = last_result;
		}
	} while (res == NULL);

	/*
	 * Workaround for the libpq disconnect case.
	 *
	 * libpq disconnect will create an empty result object without generating
	 * events, which is usually done for a regular errors.
	 *
	 * In order to be compatible with our error handling code, force
	 * create result event, if the result object does not have
	 * it already.
	 */
	if (res)
	{
		ResultEntry *entry = PQresultInstanceData(res, eventproc);

		if (entry == NULL)
			PQfireResultCreateEvents(conn->pg_conn, res);
	}

	return res;
}

PGresult *
remote_connection_exec(TSConnection *conn, const char *cmd)
{
	return remote_connection_exec_timeout(conn, cmd, TS_NO_TIMEOUT);
}

/*
 * Must be a macro since va_start() must be called in the function that takes
 * a variable number of arguments.
 */
#define stringinfo_va(fmt, sql)                                                                    \
	do                                                                                             \
	{                                                                                              \
		initStringInfo((sql));                                                                     \
		for (;;)                                                                                   \
		{                                                                                          \
			va_list args;                                                                          \
			int needed;                                                                            \
			va_start(args, fmt);                                                                   \
			needed = appendStringInfoVA((sql), fmt, args);                                         \
			va_end(args);                                                                          \
			if (needed == 0)                                                                       \
				break;                                                                             \
			/* Increase the buffer size and try again. */                                          \
			enlargeStringInfo((sql), needed);                                                      \
		}                                                                                          \
	} while (0);

/*
 * Execute a remote command.
 *
 * Like remote_connection_exec but takes a variable number of arguments.
 */
PGresult *
remote_connection_execf(TSConnection *conn, const char *fmt, ...)
{
	PGresult *res;
	StringInfoData sql;

	stringinfo_va(fmt, &sql);
	res = remote_connection_exec(conn, sql.data);
	pfree(sql.data);

	return res;
}

PGresult *
remote_connection_queryf_ok(TSConnection *conn, const char *fmt, ...)
{
	StringInfoData sql;
	PGresult *res;

	stringinfo_va(fmt, &sql);
	res = remote_result_query_ok(remote_connection_exec(conn, sql.data));
	pfree(sql.data);
	return res;
}

PGresult *
remote_connection_query_ok(TSConnection *conn, const char *query)
{
	return remote_result_query_ok(remote_connection_exec(conn, query));
}

void
remote_connection_cmd_ok(TSConnection *conn, const char *cmd)
{
	remote_result_cmd_ok(remote_connection_exec(conn, cmd));
}

void
remote_connection_cmdf_ok(TSConnection *conn, const char *fmt, ...)
{
	StringInfoData sql;

	stringinfo_va(fmt, &sql);
	remote_result_cmd_ok(remote_connection_exec(conn, sql.data));
	pfree(sql.data);
}

static PGresult *
remote_result_ok(PGresult *res, ExecStatusType expected)
{
	if (PQresultStatus(res) != expected)
		remote_result_elog(res, ERROR);

	return res;
}

void
remote_result_cmd_ok(PGresult *res)
{
	PQclear(remote_result_ok(res, PGRES_COMMAND_OK));
}

PGresult *
remote_result_query_ok(PGresult *res)
{
	return remote_result_ok(res, PGRES_TUPLES_OK);
}

/**
 * Validate extension version.
 */
void
remote_validate_extension_version(TSConnection *conn, const char *data_node_version)
{
	if (!dist_util_is_compatible_version(data_node_version, TIMESCALEDB_VERSION))
		ereport(ERROR,
				(errcode(ERRCODE_TS_DATA_NODE_INVALID_CONFIG),
				 errmsg("remote PostgreSQL instance has an incompatible timescaledb extension "
						"version"),
				 errdetail_internal("Access node version: %s, remote version: %s.",
									TIMESCALEDB_VERSION_MOD,
									data_node_version)));
}

/*
 * Check timescaledb extension version on a data node.
 *
 * Compare remote connection extension version with the one installed
 * locally on the access node.
 *
 * Return false if extension is not found, true otherwise.
 */
bool
remote_connection_check_extension(TSConnection *conn)
{
	PGresult *res;

	res = remote_connection_execf(conn,
								  "SELECT extversion FROM pg_extension WHERE extname = %s",
								  quote_literal_cstr(EXTENSION_NAME));

	/* Just to capture any bugs in the SELECT above */
	Assert(PQnfields(res) == 1);

	switch (PQntuples(res))
	{
		case 0: /* extension does not exists */
			PQclear(res);
			return false;

		case 1:
			break;

		default: /* something strange happend */
			ereport(WARNING,
					(errcode(ERRCODE_TS_DATA_NODE_INVALID_CONFIG),
					 errmsg("more than one TimescaleDB extension loaded")));
			break;
	}

	/* validate extension version on data node and make sure that it is
	 * compatible */
	remote_validate_extension_version(conn, PQgetvalue(res, 0, 0));

	PQclear(res);
	return true;
}

/*
 * Configure remote connection using current instance UUID.
 *
 * This allows remote side to reason about whether this connection has been
 * originated by access node.
 *
 * Returns true on success and false on error, in which case the optional
 * errmsg parameter can be used to retrieve an error message.
 */
static bool
remote_connection_set_peer_dist_id(TSConnection *conn)
{
	bool isnull;
	Datum uuid = ts_metadata_get_value(METADATA_UUID_KEY_NAME, UUIDOID, &isnull);
	Datum id_string = DirectFunctionCall1(uuid_out, uuid);
	PGresult *res;
	bool success = true;

	res = remote_connection_execf(conn,
								  "SELECT * FROM _timescaledb_internal.set_peer_dist_id('%s')",
								  DatumGetCString(id_string));
	success = PQresultStatus(res) == PGRES_TUPLES_OK;
	PQclear(res);

	return success;
}

/* fallback_application_name, client_encoding, end marker */
#define REMOTE_CONNECTION_SESSION_OPTIONS_N 3

/* passfile */
#define REMOTE_CONNECTION_PASSWORD_OPTIONS_N 1

/* sslmode, sslrootcert, sslcert, sslkey */
#define REMOTE_CONNECTION_SSL_OPTIONS_N 4

#define REMOTE_CONNECTION_OPTIONS_TOTAL_N                                                          \
	(REMOTE_CONNECTION_SESSION_OPTIONS_N + REMOTE_CONNECTION_PASSWORD_OPTIONS_N +                  \
	 REMOTE_CONNECTION_SSL_OPTIONS_N)

/* default password file basename */
#define DEFAULT_PASSFILE_NAME "passfile"

static void
set_password_options(const char **keywords, const char **values, int *option_start)
{
	int option_pos = *option_start;

	/* Set user specified password file path using timescaledb.passfile or
	 * use default path assuming that the file is stored in the
	 * data directory */
	keywords[option_pos] = "passfile";
	if (ts_guc_passfile)
		values[option_pos] = ts_guc_passfile;
	else
		values[option_pos] = psprintf("%s/" DEFAULT_PASSFILE_NAME, DataDir);
	option_pos++;

	*option_start = option_pos;
}

typedef enum PathKind
{
	PATH_KIND_CRT,
	PATH_KIND_KEY
} PathKind;

/* Path description for human consumption */
static const char *path_kind_text[PATH_KIND_KEY + 1] = {
	[PATH_KIND_CRT] = "certificate",
	[PATH_KIND_KEY] = "private key",
};

/* Path extension string for file system */
static const char *path_kind_ext[PATH_KIND_KEY + 1] = {
	[PATH_KIND_CRT] = "crt",
	[PATH_KIND_KEY] = "key",
};

/*
 * Helper function to report error.
 *
 * This is needed to avoid code coverage reporting low coverage for error
 * cases in `make_user_path` that cannot be reached in normal situations.
 */
static void
report_path_error(PathKind path_kind, const char *user_name)
{
	elog(ERROR,
		 "cannot write %s for user \"%s\": path too long",
		 path_kind_text[path_kind],
		 user_name);
}

/*
 * Make a user path with the given extension and user name in a portable and
 * safe manner.
 *
 * We use MD5 to compute a filename for the user name, which allows all forms
 * of user names. It is not necessary for the function to be cryptographically
 * secure, only to have a low risk of collisions, and MD5 is fast and with a
 * low risk of collisions.
 *
 * Will return the resulting path, or abort with an error.
 */
static StringInfo
make_user_path(const char *user_name, PathKind path_kind)
{
	char ret_path[MAXPGPATH];
	char hexsum[33];
	StringInfo result;
	const char *errstr;

	pg_md5_hash_compat(user_name, strlen(user_name), hexsum, &errstr);

	if (strlcpy(ret_path, ts_guc_ssl_dir ? ts_guc_ssl_dir : DataDir, MAXPGPATH) > MAXPGPATH)
		report_path_error(path_kind, user_name);
	canonicalize_path(ret_path);

	if (!ts_guc_ssl_dir)
	{
		join_path_components(ret_path, ret_path, EXTENSION_NAME);
		join_path_components(ret_path, ret_path, "certs");
	}

	join_path_components(ret_path, ret_path, hexsum);

	result = makeStringInfo();
	appendStringInfo(result, "%s.%s", ret_path, path_kind_ext[path_kind]);
	return result;
}

static void
set_ssl_options(const char *user_name, const char **keywords, const char **values,
				int *option_start)
{
	int option_pos = *option_start;
	const char *ssl_enabled;
	const char *ssl_ca_file;

	ssl_enabled = GetConfigOption("ssl", true, false);

	if (!ssl_enabled || strcmp(ssl_enabled, "on") != 0)
		return;

#ifndef NDEBUG
	ssl_enabled = GetConfigOption("timescaledb.debug_enable_ssl", true, false);
	if (ssl_enabled && strcmp(ssl_enabled, "on") != 0)
	{
		keywords[option_pos] = "sslmode";
		values[option_pos] = "disable";
		option_pos++;
		*option_start = option_pos;
		return;
	}
#endif

	/* If SSL is enabled on AN then we assume it is also should be used for DN
	 * connections as well, otherwise we need to introduce some other way to
	 * control it */
	keywords[option_pos] = "sslmode";
	values[option_pos] = "require";
	option_pos++;

	ssl_ca_file = GetConfigOption("ssl_ca_file", true, false);

	/* Use ssl_ca_file as the root certificate when verifying the
	 * data node we connect to */
	if (ssl_ca_file)
	{
		keywords[option_pos] = "sslrootcert";
		values[option_pos] = ssl_ca_file;
		option_pos++;
	}

	/* Search for the user certificate in the user subdirectory of either
	 * timescaledb.ssl_dir or data directory. The user subdirectory is
	 * currently hardcoded. */

	keywords[option_pos] = "sslcert";
	values[option_pos] = make_user_path(user_name, PATH_KIND_CRT)->data;
	option_pos++;

	keywords[option_pos] = "sslkey";
	values[option_pos] = make_user_path(user_name, PATH_KIND_KEY)->data;
	option_pos++;

	/* if ts_set_ssl_options_hook is enabled then invoke that hook */
	if (ts_set_ssl_options_hook)
		ts_set_ssl_options_hook(user_name);

	*option_start = option_pos;
}

/*
 * Finish the connection and, optionally, save the connection error.
 */
static void
finish_connection(PGconn *conn, char **errmsg)
{
	if (NULL != errmsg)
	{
		if (NULL == conn)
			*errmsg = "invalid connection";
		else
			*errmsg = pchomp(PQerrorMessage(conn));
	}

	PQfinish(conn);
}

/*
 * Take options belonging to a foreign server and add additional default and
 * other user/ssl related options as appropriate
 */
static void
setup_full_connection_options(List *connection_options, const char ***all_keywords,
							  const char ***all_values)
{
	const char *user_name = NULL;
	const char **keywords;
	const char **values;
	int option_count;
	int option_pos;

	/*
	 * Construct connection params from generic options of ForeignServer
	 * and user. (Some of them might not be libpq options, in
	 * which case we'll just waste a few array slots.)  Add 3 extra slots
	 * for fallback_application_name, client_encoding, end marker.
	 * One additional slot to set passfile and 4 slots for ssl options.
	 */
	option_count = list_length(connection_options) + REMOTE_CONNECTION_OPTIONS_TOTAL_N;
	keywords = (const char **) palloc(option_count * sizeof(char *));
	values = (const char **) palloc(option_count * sizeof(char *));

	option_pos = extract_connection_options(connection_options, keywords, values, &user_name);

	if (NULL == user_name)
		user_name = GetUserNameFromId(GetUserId(), false);

	/* Use the extension name as fallback_application_name. */
	keywords[option_pos] = "fallback_application_name";
	values[option_pos] = EXTENSION_NAME;
	option_pos++;

	/* Set client_encoding so that libpq can convert encoding properly. */
	keywords[option_pos] = "client_encoding";
	values[option_pos] = GetDatabaseEncodingName();
	option_pos++;

	/* Set passfile options */
	set_password_options(keywords, values, &option_pos);

	/* Set client specific SSL connection options */
	set_ssl_options(user_name, keywords, values, &option_pos);

	/* Set end marker */
	keywords[option_pos] = values[option_pos] = NULL;
	Assert(option_pos <= option_count);

	*all_keywords = keywords;
	*all_values = values;
}

/*
 * Open a connection and assign it the given node name.
 *
 * This will only open a connection to a specific node, but not do any other
 * session initialization. In particular, it will not perform any validation
 * nor configure the connection since it cannot know that it connects to a
 * data node database or not. For that, please use the
 * `remote_connection_open_session` function.
 *
 * The connection's life-cycle is tied to the current memory context via its
 * delete callback. As a result, the connection will be automatically closed
 * and freed when the memory context is deleted.
 *
 * This function does not (and should not) throw (PostgreSQL) errors. Instead,
 * an error message is optionally returned via the "errmsg" parameter.
 */
TSConnection *
remote_connection_open(const char *node_name, List *connection_options, TimestampTz endtime,
					   char **errmsg)
{
	PGconn *pg_conn = NULL;
	TSConnection *ts_conn = NULL;
	const char **keywords;
	const char **values;
	PostgresPollingStatusType status;

	if (NULL != errmsg)
		*errmsg = NULL;

	setup_full_connection_options(connection_options, &keywords, &values);
	pg_conn = PQconnectStartParams(keywords, values, 0 /* Do not expand dbname param */);

	/* Cast to (char **) to silence warning with MSVC compiler */
	pfree((char **) keywords);
	pfree((char **) values);

	if (NULL == pg_conn)
		return NULL;

	if (PQstatus(pg_conn) == CONNECTION_BAD)
	{
		finish_connection(pg_conn, errmsg);
		return NULL;
	}

	status = PGRES_POLLING_WRITING;

	do
	{
		long timeout_ms;
		int events;
		int io_flag;
		int rc;

		if (status == PGRES_POLLING_READING)
			io_flag = WL_SOCKET_READABLE;
#ifdef WIN32
		/* Windows needs a different test while waiting for connection-made */
		else if (PQstatus(pg_conn) == CONNECTION_STARTED)
			io_flag = WL_SOCKET_CONNECTED;
#endif
		else
			io_flag = WL_SOCKET_WRITEABLE;

		if (endtime == TS_NO_TIMEOUT)
			events = io_flag;
		else
			events = io_flag | WL_TIMEOUT;

		timeout_ms = timeout_diff_ms(endtime);

		/*
		 * Wait for latch or socket event. Note that it is not possible to
		 * reuse a WaitEventSet using the same socket file descriptor in each
		 * iteration of the loop since PQconnectPoll() might change the file
		 * descriptor across calls. Therefore, it is important to create a new
		 * WaitEventSet in every iteration of the loop and retreiving the
		 * correct file descriptor (socket) with PQsocket().
		 */
		rc = WaitLatchOrSocket(MyLatch,
							   WL_EXIT_ON_PM_DEATH | WL_LATCH_SET | events,
							   PQsocket(pg_conn),
							   timeout_ms,
							   PG_WAIT_EXTENSION);

		if (rc & WL_TIMEOUT)
		{
			finish_connection(pg_conn, errmsg);
			return NULL;
		}

		if (rc & WL_LATCH_SET)
		{
			ResetLatch(MyLatch);
			CHECK_FOR_INTERRUPTS();
		}

		if (rc & io_flag)
		{
			/*
			 * PQconnectPoll() is supposed to be non-blocking, but it
			 * isn't. PQconnectPoll() will internally try to send a startup
			 * packet and do DNS lookups (if necessary) and can therefore
			 * block. So, if there is a network issue (e.g., black hole
			 * routing) the connection attempt will hang on
			 * PQconnectPoll(). There's nothing that can be done about it,
			 * unless the blocking operations are moved out of PQconnectPoll()
			 * and integrated with the wait loop.
			 */
			status = PQconnectPoll(pg_conn);
		}
	} while (status != PGRES_POLLING_OK && status != PGRES_POLLING_FAILED);

	if (PQstatus(pg_conn) != CONNECTION_OK)
	{
		finish_connection(pg_conn, errmsg);
		return NULL;
	}

	ts_conn = remote_connection_create(pg_conn, false, node_name);

	if (NULL == ts_conn)
		finish_connection(pg_conn, errmsg);

	return ts_conn;
}

/*
 * Open a connection to a data node and perform basic session initialization.
 *
 * This function will raise errors on failures.
 */
TSConnection *
remote_connection_open_session(const char *node_name, List *connection_options, bool set_dist_id)
{
	char *err = NULL;
	TSConnection *conn = remote_connection_open(node_name, connection_options, TS_NO_TIMEOUT, &err);

	if (NULL == conn)
		ereport(ERROR,
				(errcode(ERRCODE_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION),
				 errmsg("could not connect to \"%s\"", node_name),
				 err == NULL ? 0 : errdetail_internal("%s", err)));

	/*
	 * Use PG_TRY block to ensure closing connection on error.
	 */
	PG_TRY();
	{
		Assert(NULL != conn->pg_conn);

		if (PQstatus(conn->pg_conn) != CONNECTION_OK)
			ereport(ERROR,
					(errcode(ERRCODE_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION),
					 errmsg("could not connect to \"%s\"", node_name),
					 errdetail_internal("%s", pchomp(PQerrorMessage(conn->pg_conn)))));

		/* Prepare new session for use */
		if (!remote_connection_configure(conn))
			ereport(ERROR,
					(errcode(ERRCODE_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION),
					 errmsg("could not configure remote connection to \"%s\"", node_name),
					 errdetail_internal("%s", PQerrorMessage(conn->pg_conn))));

		/* Check a data node extension version and show a warning
		 * message if it differs */
		remote_connection_check_extension(conn);

		if (set_dist_id)
		{
			/* Inform remote node about instance UUID */
			if (!remote_connection_set_peer_dist_id(conn))
				ereport(ERROR,
						(errcode(ERRCODE_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION),
						 errmsg("could not set distributed ID for \"%s\"", node_name),
						 errdetail_internal("%s", PQerrorMessage(conn->pg_conn))));
		}
	}
	PG_CATCH();
	{
		/* Release PGconn data structure if we managed to create one */
		remote_connection_close(conn);
		PG_RE_THROW();
	}
	PG_END_TRY();

	return conn;
}

TSConnection *
remote_connection_open_session_by_id(TSConnectionId id)
{
	ForeignServer *server = GetForeignServer(id.server_id);
	List *connection_options = remote_connection_prepare_auth_options(server, id.user_id);

	return remote_connection_open_session(server->servername, connection_options, true);
}

/*
 * Based on PG's GetUserMapping, but this version does not fail when a user
 * mapping is not found.
 */
static UserMapping *
get_user_mapping(Oid userid, Oid serverid)
{
	Datum datum;
	HeapTuple tp;
	bool isnull;
	UserMapping *um;

	tp = SearchSysCache2(USERMAPPINGUSERSERVER,
						 ObjectIdGetDatum(userid),
						 ObjectIdGetDatum(serverid));

	if (!HeapTupleIsValid(tp))
	{
		/* Not found for the specific user -- try PUBLIC */
		tp = SearchSysCache2(USERMAPPINGUSERSERVER,
							 ObjectIdGetDatum(InvalidOid),
							 ObjectIdGetDatum(serverid));
	}

	if (!HeapTupleIsValid(tp))
		return NULL;

	um = (UserMapping *) palloc(sizeof(UserMapping));
	um->umid = ((Form_pg_user_mapping) GETSTRUCT(tp))->oid;
	um->userid = userid;
	um->serverid = serverid;

	/* Extract the umoptions */
	datum = SysCacheGetAttr(USERMAPPINGUSERSERVER, tp, Anum_pg_user_mapping_umoptions, &isnull);
	if (isnull)
		um->options = NIL;
	else
		um->options = untransformRelOptions(datum);

	ReleaseSysCache(tp);

	return um;
}

static bool
options_contain(List *options, const char *key)
{
	ListCell *lc;

	foreach (lc, options)
	{
		DefElem *d = (DefElem *) lfirst(lc);

		if (strcmp(d->defname, key) == 0)
			return true;
	}

	return false;
}

/*
 * Add athentication info (username and optionally password) to the connection
 * options).
 */
List *
remote_connection_prepare_auth_options(const ForeignServer *server, Oid user_id)
{
	const UserMapping *um = get_user_mapping(user_id, server->serverid);
	List *options = list_copy(server->options);

	/* If a user mapping exists, then use the "user" and "password" options
	 * from the user mapping (we assume that these options exist, or the
	 * connection will later fail). Otherwise, just add the "user" and rely on
	 * other authentication mechanisms. */
	if (NULL != um)
		options = list_concat(options, um->options);

	if (!options_contain(options, "user"))
	{
		char *user_name = GetUserNameFromId(user_id, false);
		options = lappend(options, makeDefElem("user", (Node *) makeString(user_name), -1));
	}

	return options;
}

/*
 * Append the given string to the buffer, with suitable quoting for passing
 * the string as a value in a keyword/value pair in a libpq connection string.
 *
 * The implementation is based on libpq appendConnStrVal().
 */
static void
remote_connection_append_connstr_value(StringInfo buf, const char *str)
{
	const char *s;
	bool needquotes;

	/*
	 * If the string is one or more plain ASCII characters, no need to quote
	 * it. This is quite conservative, but better safe than sorry.
	 */
	needquotes = true;
	for (s = str; *s; s++)
	{
		if (!((*s >= 'a' && *s <= 'z') || (*s >= 'A' && *s <= 'Z') || (*s >= '0' && *s <= '9') ||
			  *s == '_' || *s == '.'))
		{
			needquotes = true;
			break;
		}
		needquotes = false;
	}

	if (needquotes)
	{
		appendStringInfoChar(buf, '\'');
		while (*str)
		{
			/* ' and \ must be escaped by to \' and \\ */
			if (*str == '\'' || *str == '\\')
				appendStringInfoChar(buf, '\\');

			appendStringInfoChar(buf, *str);
			str++;
		}
		appendStringInfoChar(buf, '\'');
	}
	else
		appendStringInfoString(buf, str);
}

char *
remote_connection_get_connstr(const char *node_name)
{
	ForeignServer *server;
	List *connection_options;
	const char **keywords;
	const char **values;
	StringInfoData connstr;
	StringInfoData connstr_escape;
	int i;

	server = data_node_get_foreign_server(node_name, ACL_NO_CHECK, false, false);
	connection_options = remote_connection_prepare_auth_options(server, GetUserId());
	setup_full_connection_options(connection_options, &keywords, &values);

	/* Cycle through the options and create the connection string */
	initStringInfo(&connstr);
	i = 0;
	while (keywords[i] != NULL)
	{
		appendStringInfo(&connstr, " %s=", keywords[i]);
		remote_connection_append_connstr_value(&connstr, values[i]);
		i++;
	}
	Assert(keywords[i] == NULL && values[i] == NULL);

	initStringInfo(&connstr_escape);
	enlargeStringInfo(&connstr_escape, connstr.len * 2 + 1);
	connstr_escape.len += PQescapeString(connstr_escape.data, connstr.data, connstr.len);

	/* Cast to (char **) to silence warning with MSVC compiler */
	pfree((char **) keywords);
	pfree((char **) values);
	pfree(connstr.data);

	return connstr_escape.data;
}

#define PING_QUERY "SELECT 1"

bool
remote_connection_ping(const char *node_name, TimestampTz endtime)
{
	Oid server_id = get_foreign_server_oid(node_name, false);
	ForeignServer *server = GetForeignServer(server_id);
	Oid fdwid = get_foreign_data_wrapper_oid(EXTENSION_FDW_NAME, false);
	List *connection_options;
	TSConnection *conn;
	bool success = false;

	if (server->fdwid != fdwid)
	{
		elog(WARNING, "invalid node type for \"%s\"", server->servername);
		return false;
	}

	connection_options = remote_connection_prepare_auth_options(server, GetUserId());
	conn = remote_connection_open(server->servername, connection_options, endtime, NULL);

	if (NULL == conn)
		return false;

	if (PQstatus(conn->pg_conn) == CONNECTION_OK)
	{
		PGresult *res = remote_connection_exec_timeout(conn, PING_QUERY, endtime);
		success = (PQresultStatus(res) == PGRES_TUPLES_OK);
	}

	remote_connection_close(conn);

	return success;
}

void
remote_connection_close(TSConnection *conn)
{
	/* The PQfinish callback handler will take care of freeing the resources,
	 * including the TSConnection object. */
	PQfinish(conn->pg_conn);
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

#define MAX_CONN_WAIT_TIMEOUT_MS 60000

/*
 * Drain a connection of all data coming in and discard the results. Return
 * CONN_OK if all data is drained before the deadline expires.
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
TSConnectionResult
remote_connection_drain(TSConnection *conn, TimestampTz endtime, PGresult **result)
{
	volatile TSConnectionResult connresult = CONN_OK;
	PGresult *volatile last_res = NULL;
	PGconn *pg_conn = remote_connection_get_pg_conn(conn);

	/* In what follows, do not leak any PGresults on an error. */
	PG_TRY();
	{
		for (;;)
		{
			PGresult *res;

			while (PQisBusy(pg_conn))
			{
				int wc;
				TimestampTz now = GetCurrentTimestamp();
				long remaining_secs;
				int remaining_usecs;
				long cur_timeout_ms;

				/* If timeout has expired, give up, else get sleep time. */
				if (now >= endtime)
				{
					connresult = CONN_TIMEOUT;
					goto exit;
				}

				TimestampDifference(now, endtime, &remaining_secs, &remaining_usecs);

				/* To protect against clock skew, limit sleep to one minute. */
				cur_timeout_ms =
					Min(MAX_CONN_WAIT_TIMEOUT_MS, remaining_secs * USECS_PER_SEC + remaining_usecs);

				/* Sleep until there's something to do */
				wc = WaitLatchOrSocket(MyLatch,
									   WL_LATCH_SET | WL_SOCKET_READABLE | WL_EXIT_ON_PM_DEATH |
										   WL_TIMEOUT,
									   PQsocket(pg_conn),
									   cur_timeout_ms,
									   PG_WAIT_EXTENSION);
				ResetLatch(MyLatch);

				CHECK_FOR_INTERRUPTS();

				/* Data available in socket? */
				if ((wc & WL_SOCKET_READABLE) && (0 == PQconsumeInput(pg_conn)))
				{
					connresult = CONN_DISCONNECT;
					goto exit;
				}
			}

			res = PQgetResult(pg_conn);

			if (res == NULL)
			{
				/* query is complete */
				remote_connection_set_status(conn, CONN_IDLE);
				connresult = CONN_OK;
				break;
			}
			else if (PQresultStatus(res) == PGRES_COPY_OUT)
			{
				/*
				 * We are inside the COPY subprotocol, need to sychronize with
				 * the server.
				 */
				int end_res = PQendcopy(pg_conn);
				if (end_res != 0)
				{
					TSConnectionError err;
					remote_connection_get_error(conn, &err);
					remote_connection_error_elog(&err, WARNING);
				}
			}

			PQclear(last_res);
			last_res = res;
		}
	exit:;
	}
	PG_CATCH();
	{
		PQclear(last_res);
		PG_RE_THROW();
	}
	PG_END_TRY();

	switch (connresult)
	{
		case CONN_OK:
			if (last_res == NULL)
				connresult = CONN_NO_RESPONSE;
			else if (result != NULL)
				*result = last_res;
			else
				PQclear(last_res);
			break;
		case CONN_TIMEOUT:
		case CONN_DISCONNECT:
			PQclear(last_res);
			break;
		case CONN_NO_RESPONSE:
			Assert(last_res == NULL);
			break;
	}

	return connresult;
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
	TSConnectionError err;
	bool success;

	if (!conn)
		return true;

	memset(&err, 0, sizeof(TSConnectionError));

	/*
	 * Catch exceptions so that we can ensure the status is IDLE after the
	 * cancel operation even in case of errors being thrown. Note that we
	 * cannot set the status before we drain, since the drain function needs
	 * to know the status (e.g., if the connection is in COPY_IN mode).
	 */
	PG_TRY();
	{
		if (conn->status == CONN_COPY_IN && !remote_connection_end_copy(conn, &err))
			remote_connection_error_elog(&err, WARNING);

		/*
		 * If it takes too long to cancel the query and discard the result, assume
		 * the connection is dead.
		 */
		endtime = TimestampTzPlusMilliseconds(GetCurrentTimestamp(), 30000);

		/*
		 * Issue cancel request.  Unfortunately, there's no good way to limit the
		 * amount of time that we might block inside PQcancel().
		 */
		if ((cancel = PQgetCancel(conn->pg_conn)))
		{
			if (!PQcancel(cancel, errbuf, sizeof(errbuf)))
			{
				ereport(WARNING,
						(errcode(ERRCODE_CONNECTION_FAILURE),
						 errmsg("could not send cancel request: %s", errbuf)));
				PQfreeCancel(cancel);
				remote_connection_set_status(conn, CONN_IDLE);
				return false;
			}
			PQfreeCancel(cancel);
		}

		switch (remote_connection_drain(conn, endtime, NULL))
		{
			case CONN_OK:
				/* Successfully, drained */
			case CONN_NO_RESPONSE:
				/* No response, likely beceause there was nothing to cancel */
				success = true;
				break;
			default:
				success = false;
				break;
		}
	}
	PG_CATCH();
	{
		remote_connection_set_status(conn, CONN_IDLE);
		PG_RE_THROW();
	}
	PG_END_TRY();

	remote_connection_set_status(conn, CONN_IDLE);

	return success;
}

void
remote_result_close(PGresult *res)
{
	PQclear(res);
}

bool
remote_connection_set_single_row_mode(TSConnection *conn)
{
	return PQsetSingleRowMode(conn->pg_conn);
}

static bool
send_binary_copy_header(const TSConnection *conn, TSConnectionError *err)
{
	/* File header for binary format */
	static const char file_header[] = {
		'P', 'G', 'C', 'O', 'P', 'Y', '\n', '\377', '\r', '\n', '\0', /* Signature */
		0,	 0,	  0,   0,											  /* 4 bytes flags */
		0,	 0,	  0,   0 /* 4 bytes header extension length (unused) */
	};

	int res = PQputCopyData(conn->pg_conn, file_header, sizeof(file_header));

	if (res != 1)
		return fill_connection_error(err,
									 ERRCODE_CONNECTION_FAILURE,
									 "could not set binary COPY mode",
									 conn);
	return true;
}

bool
remote_connection_begin_copy(TSConnection *conn, const char *copycmd, bool binary,
							 TSConnectionError *err)
{
	PGconn *pg_conn = remote_connection_get_pg_conn(conn);
	PGresult *volatile res = NULL;

	if (PQisnonblocking(pg_conn))
		return fill_simple_error(err,
								 ERRCODE_FEATURE_NOT_SUPPORTED,
								 "distributed copy doesn't support non-blocking connections",
								 conn);

	if (conn->status != CONN_IDLE)
		return fill_simple_error(err,
								 ERRCODE_INTERNAL_ERROR,
								 "connection not IDLE when beginning COPY",
								 conn);

#ifndef NDEBUG
	/* Set some variables for testing. */
	const char *error_after_option =
		GetConfigOption("timescaledb.debug_broken_sendrecv_error_after", true, false);
	if (error_after_option)
	{
		res = PQexec(pg_conn,
					 psprintf("set timescaledb.debug_broken_sendrecv_error_after = '%s';",
							  error_after_option));
		if (PQresultStatus(res) != PGRES_COMMAND_OK)
		{
			remote_connection_get_result_error(res, err);
			PQclear(res);
			return false;
		}
		PQclear(res);
	}
#endif

	/* Run the COPY query. */
	res = PQexec(pg_conn, copycmd);

	if (PQresultStatus(res) != PGRES_COPY_IN)
	{
		fill_result_error(err,
						  ERRCODE_CONNECTION_FAILURE,
						  "unable to start remote COPY on data node",
						  res);
		PQclear(res);
		return false;
	}

	PQclear(res);

	if (binary && !send_binary_copy_header(conn, err))
		goto err_end_copy;

	/* Switch the connection into nonblocking mode for the duration of COPY. */
	if (PQsetnonblocking(pg_conn, 1) != 0)
	{
		(void) fill_simple_error(err,
								 ERRCODE_CONNECTION_EXCEPTION,
								 "failed to set the connection into nonblocking mode",
								 conn);
		goto err_end_copy;
	}

	conn->binary_copy = binary;
	remote_connection_set_status(conn, CONN_COPY_IN);

	return true;
err_end_copy:
	PQputCopyEnd(pg_conn, err->msg);

	return false;
}

int
remote_connection_put_copy_data(TSConnection *conn, const char *buffer, size_t len,
								TSConnectionError *err)
{
	int res;

	res = PQputCopyData(remote_connection_get_pg_conn(conn), buffer, len);

	if (res == -1)
		return fill_connection_error(err,
									 ERRCODE_CONNECTION_EXCEPTION,
									 "could not send COPY data",
									 conn);

	return res;
}

static bool
send_end_binary_copy_data(const TSConnection *conn, TSConnectionError *err)
{
	const uint16 buf = pg_hton16((uint16) -1);

	if (PQputCopyData(conn->pg_conn, (char *) &buf, sizeof(buf)) != 1)
		return fill_simple_error(err, ERRCODE_INTERNAL_ERROR, "could not end binary COPY", conn);

	return true;
}

/*
 * End COPY on the remote connection.
 * This function is used to clean up after errors as well, so it works in a
 * recovery fashion: it tries to bring the connection into predictable and
 * usable state, even if there are some errors or discrepancies between its
 * actual and expected state.
 */
bool
remote_connection_end_copy(TSConnection *conn, TSConnectionError *err)
{
	PGresult *res = NULL;

	/*
	 * In any case, try to switch the connection into the blocking mode, because
	 * that's what the non-COPY code expects.
	 */
	if (PQisnonblocking(conn->pg_conn))
	{
		/*
		 * We have to flush the connection before we can switch it into blocking
		 * mode.
		 */
		for (;;)
		{
			CHECK_FOR_INTERRUPTS();

			int flush_result = PQflush(conn->pg_conn);

			if (flush_result == 1)
			{
				/*
				 * In some rare cases, flush might report that it's busy, but
				 * actually there was an error and the socket became invalid.
				 * Check for it. This is something we have observed in COPY
				 * queries used for performance testing with tsbench, but not
				 * sure how it happens exactly, must be in the depths of
				 * pqReadData called by pqFlush.
				 */
				int socket = PQsocket(conn->pg_conn);
				if (socket == PGINVALID_SOCKET)
				{
					return fill_connection_error(err,
												 ERRCODE_CONNECTION_EXCEPTION,
												 "failed to flush the COPY connection",
												 conn);
				}

				/*
				 * The socket is busy, wait. We don't care about the wait result
				 * here, because whether it is a timeout or the socket became
				 * writeable, we just retry.
				 */
				(void) WaitLatchOrSocket(MyLatch,
										 WL_TIMEOUT | WL_SOCKET_WRITEABLE | WL_EXIT_ON_PM_DEATH,
										 socket,
										 /* timeout = */ 1000,
										 /* wait_event_info = */ 0);
			}
			else if (flush_result == 0)
			{
				/* Flushed all. */
				break;
			}
			else
			{
				/* Error. */
				return fill_connection_error(err,
											 ERRCODE_CONNECTION_EXCEPTION,
											 "failed to flush the COPY connection",
											 conn);
			}
		}

		/* Switch the connection into blocking mode. */
		if (PQsetnonblocking(conn->pg_conn, 0) != 0)
		{
			return fill_connection_error(err,
										 ERRCODE_CONNECTION_EXCEPTION,
										 "failed to set the connection into blocking mode",
										 conn);
		}
	}

	/*
	 * Shouldn't have been called for a connection we know is not in COPY mode.
	 */
	if (conn->status != CONN_COPY_IN)
		return fill_simple_error(err,
								 ERRCODE_INTERNAL_ERROR,
								 "connection not in COPY_IN state when ending COPY",
								 conn);

	/*
	 * Check whether it's still in COPY mode. The dist_copy manages COPY
	 * protocol itself because it needs to work with multiple connections
	 * concurrently. The remote server might leave the COPY mode for own
	 * reasons, as well. If we discover this, update our info with the actual
	 * status, but still report the error.
	 */
	res = PQgetResult(conn->pg_conn);
	if (res == NULL || PQresultStatus(res) != PGRES_COPY_IN)
	{
		remote_connection_set_status(conn, res == NULL ? CONN_IDLE : CONN_PROCESSING);
	}

	/*
	 * Finish the COPY if needed.
	 */
	if (remote_connection_get_status(conn) == CONN_COPY_IN)
	{
		if (conn->binary_copy && !send_end_binary_copy_data(conn, err))
			return false;

		if (PQputCopyEnd(conn->pg_conn, NULL) != 1)
			return fill_connection_error(err,
										 ERRCODE_CONNECTION_EXCEPTION,
										 "could not end remote COPY",
										 conn);

		remote_connection_set_status(conn, CONN_PROCESSING);
	}

	bool success = true;
	while ((res = PQgetResult(conn->pg_conn)))
	{
		ExecStatusType status = PQresultStatus(res);
		if (status != PGRES_COMMAND_OK)
		{
			success = false;
			remote_connection_get_result_error(res, err);
		}
	}

	remote_connection_set_status(conn, CONN_IDLE);

	return success;
}

#ifdef TS_DEBUG
/*
 * Reset the current connection stats.
 */
void
remote_connection_stats_reset(void)
{
	MemSet(&connstats, 0, sizeof(RemoteConnectionStats));
}

/*
 * Get the current connection stats.
 */
RemoteConnectionStats *
remote_connection_stats_get(void)
{
	return &connstats;
}
#endif

void
_remote_connection_init(void)
{
	unset_libpq_envvar();
}

void
_remote_connection_fini(void)
{
}
