/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <access/xact.h>
#include <access/htup_details.h>
#include <foreign/foreign.h>
#include <nodes/makefuncs.h>
#include <nodes/parsenodes.h>
#include <catalog/pg_foreign_server.h>
#include <catalog/namespace.h>
#include <catalog/pg_namespace.h>
#include <commands/dbcommands.h>
#include <commands/defrem.h>
#include <commands/event_trigger.h>
#include <utils/builtins.h>
#include <libpq/crypt.h>
#include <miscadmin.h>
#include <funcapi.h>

#include <hypertable_server.h>
#include <extension.h>
#include <compat.h>
#include <catalog.h>

#include "fdw/timescaledb_fdw.h"
#if !PG96
#include "remote/async.h"
#include "remote/connection.h"
#endif
#include "server.h"
#include "hypertable.h"
#include "hypertable_cache.h"
#include "errors.h"
#include "dist_util.h"
#include "utils/uuid.h"

#define TS_DEFAULT_POSTGRES_PORT 5432
#define TS_DEFAULT_POSTGRES_HOST "localhost"

#if !PG96
static const char *ping_query = "SELECT 1";
#endif

/*
 * Create a user mapping.
 *
 * Returns the OID of the created user mapping.
 *
 * Non-superusers must provide a password.
 */
static Oid
create_user_mapping(const char *username, const char *server_username, const char *servername,
					const char *password, bool if_not_exists)
{
	ObjectAddress objaddr;
	RoleSpec rolespec = {
		.type = T_RoleSpec,
		.roletype = ROLESPEC_CSTRING,
		.rolename = (char *) username,
		.location = -1,
	};
	CreateUserMappingStmt stmt = {
		.type = T_CreateUserMappingStmt,
#if PG96
		.user = (Node *) &rolespec,
#else
		.user = &rolespec,
		.if_not_exists = if_not_exists,
#endif
		.servername = (char *) servername,
		.options = NIL,
	};

	Assert(NULL != username && NULL != server_username && NULL != servername);

	stmt.options =
		list_make1(makeDefElemCompat("user", (Node *) makeString(pstrdup(server_username)), -1));

	/* Non-superusers must provide a password */
	if (!superuser() && (NULL == password || password[0] == '\0'))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_PARAMETER),
				 errmsg("no password specified for user \"%s\"", server_username),
				 errhint("Specify a password to use when connecting to server \"%s\"",
						 servername)));

	if (NULL != password)
		stmt.options =
			lappend(stmt.options,
					makeDefElemCompat("password", (Node *) makeString(pstrdup(password)), -1));

	objaddr = CreateUserMapping(&stmt);

	return objaddr.objectId;
}

/*
 * Create a foreign server.
 *
 * Returns the OID of the created foreign server.
 */
static Oid
create_foreign_server(const char *servername, const char *host, int32 port, const char *dbname,
					  bool if_not_exists)
{
	ObjectAddress objaddr;
	CreateForeignServerStmt stmt = {
		.type = T_CreateForeignServerStmt,
		.servername = (char *) servername,
		.fdwname = TIMESCALEDB_FDW_NAME,
		.options =
			list_make3(makeDefElemCompat("host", (Node *) makeString(pstrdup(host)), -1),
					   makeDefElemCompat("port", (Node *) makeInteger(port), -1),
					   makeDefElemCompat("dbname", (Node *) makeString(pstrdup(dbname)), -1)),
#if !PG96
		.if_not_exists = if_not_exists,
#endif
	};

	if (NULL == host)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 (errmsg("invalid host"),
				  (errhint("A hostname or IP address must be specified when "
						   "a foreign server does not already exist.")))));

	objaddr = CreateForeignServer(&stmt);

	return objaddr.objectId;
}

/* Attribute numbers for datum returned by create_server() */
enum Anum_create_server
{
	Anum_create_server_name = 1,
	Anum_create_server_host,
	Anum_create_server_port,
	Anum_create_server_dbname,
	Anum_create_server_user,
	Anum_create_server_server_user,
	Anum_create_server_created,
	_Anum_create_server_max,
};

#define Natts_create_server (_Anum_create_server_max - 1)

static Datum
create_server_datum(FunctionCallInfo fcinfo, const char *servername, const char *host, int32 port,
					const char *dbname, const char *username, const char *server_username,
					bool created)
{
	TupleDesc tupdesc;
	Datum values[Natts_create_server];
	bool nulls[Natts_create_server] = { false };
	HeapTuple tuple;

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("function returning record called in "
						"context that cannot accept type record")));

	tupdesc = BlessTupleDesc(tupdesc);
	values[AttrNumberGetAttrOffset(Anum_create_server_name)] = CStringGetDatum(servername);
	values[AttrNumberGetAttrOffset(Anum_create_server_host)] = CStringGetTextDatum(host);
	values[AttrNumberGetAttrOffset(Anum_create_server_port)] = Int32GetDatum(port);
	values[AttrNumberGetAttrOffset(Anum_create_server_dbname)] = CStringGetDatum(dbname);
	values[AttrNumberGetAttrOffset(Anum_create_server_user)] = CStringGetDatum(username);
	values[AttrNumberGetAttrOffset(Anum_create_server_server_user)] =
		CStringGetDatum(server_username);
	values[AttrNumberGetAttrOffset(Anum_create_server_created)] = BoolGetDatum(created);
	tuple = heap_form_tuple(tupdesc, values, nulls);

	return HeapTupleGetDatum(tuple);
}

static Datum
create_hypertable_server_datum(FunctionCallInfo fcinfo, HypertableServer *server)
{
	TupleDesc tupdesc;
	Datum values[Natts_hypertable_server];
	bool nulls[Natts_hypertable_server] = { false };
	HeapTuple tuple;

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("function returning record called in "
						"context that cannot accept type record")));

	tupdesc = BlessTupleDesc(tupdesc);
	values[AttrNumberGetAttrOffset(Anum_hypertable_server_hypertable_id)] =
		Int32GetDatum(server->fd.hypertable_id);
	values[AttrNumberGetAttrOffset(Anum_hypertable_server_server_hypertable_id)] =
		Int32GetDatum(server->fd.server_hypertable_id);
	values[AttrNumberGetAttrOffset(Anum_hypertable_server_server_name)] =
		NameGetDatum(&server->fd.server_name);
	tuple = heap_form_tuple(tupdesc, values, nulls);

	return HeapTupleGetDatum(tuple);
}

static UserMapping *
get_user_mapping(Oid userid, Oid serverid)
{
	UserMapping *um;

	PG_TRY();
	{
		um = GetUserMapping(userid, serverid);
	}
	PG_CATCH();
	{
		um = NULL;
		FlushErrorState();
	}
	PG_END_TRY();

	return um;
}

#if !PG96 /* Remote server bootstrapping only supported on PG10 and above */

static List *
create_server_options(const char *host, int32 port, const char *dbname, const char *user,
					  const char *password)
{
	List *server_options;
	DefElem *host_elm = makeDefElemCompat("host", (Node *) makeString(pstrdup(host)), -1);
	DefElem *port_elm = makeDefElemCompat("port", (Node *) makeInteger(port), -1);
	DefElem *dbname_elm = makeDefElemCompat("dbname", (Node *) makeString(pstrdup(dbname)), -1);
	DefElem *user_elm = makeDefElemCompat("user", (Node *) makeString(pstrdup(user)), -1);
	DefElem *password_elm;

	server_options = list_make4(host_elm, port_elm, dbname_elm, user_elm);
	if (password)
	{
		password_elm = makeDefElemCompat("password", (Node *) makeString(pstrdup(password)), -1);
		lappend(server_options, password_elm);
	}
	return server_options;
}

static void
server_bootstrap_database(const char *servername, const char *host, int32 port, const char *dbname,
						  bool if_not_exists, const char *bootstrap_database,
						  const char *bootstrap_user, const char *bootstrap_password)
{
	PGconn *conn;
	List *server_options;

	server_options =
		create_server_options(host, port, bootstrap_database, bootstrap_user, bootstrap_password);
	conn = remote_connection_open((char *) servername, server_options, NULL, false);

	PG_TRY();
	{
		bool database_exists = false;
		char *request;
		PGresult *res;

		request =
			psprintf("SELECT 1 FROM pg_database WHERE datname = %s", quote_literal_cstr(dbname));
		res = remote_connection_query_any_result(conn, request);
		if (PQntuples(res) > 0)
			database_exists = true;
		remote_connection_result_close(res);

		if (database_exists)
		{
			if (!if_not_exists)
				ereport(ERROR,
						(errcode(ERRCODE_DUPLICATE_OBJECT),
						 errmsg("database \"%s\" already exists on the remote server", dbname),
						 errhint("Set if_not_exists => TRUE to add the server to an existing "
								 "database.")));
			else
				elog(NOTICE, "remote server database \"%s\" already exists, skipping", dbname);
		}
		else
		{
			request = psprintf("CREATE DATABASE %s", quote_identifier(dbname));
			res = remote_connection_query_ok_result(conn, request);
			remote_connection_result_close(res);
		}
	}
	PG_CATCH();
	{
		remote_connection_close(conn);
		PG_RE_THROW();
	}
	PG_END_TRY();

	remote_connection_close(conn);
}

static void
server_bootstrap_extension(const char *servername, const char *host, int32 port, const char *dbname,
						   bool if_not_exists, const char *user, const char *user_password)
{
	PGconn *conn;
	List *server_options;

	server_options = create_server_options(host, port, dbname, user, user_password);
	conn = remote_connection_open((char *) servername, server_options, NULL, false);

	PG_TRY();
	{
		PGresult *res;
		char *request;
		const char *schema_name = ts_extension_schema_name();
		const char *schema_name_quoted = quote_identifier(schema_name);
		Oid schema_oid = get_namespace_oid(schema_name, true);

		if (schema_oid != PG_PUBLIC_NAMESPACE)
		{
			request = psprintf("CREATE SCHEMA %s%s",
							   if_not_exists ? "IF NOT EXISTS " : "",
							   schema_name_quoted);
			res = remote_connection_query_ok_result(conn, request);
			remote_connection_result_close(res);
		}
		request = psprintf("CREATE EXTENSION %s " EXTENSION_NAME " WITH SCHEMA %s CASCADE",
						   if_not_exists ? "IF NOT EXISTS" : "",
						   schema_name_quoted);
		res = remote_connection_query_ok_result(conn, request);
		remote_connection_result_close(res);
	}
	PG_CATCH();
	{
		remote_connection_close(conn);
		PG_RE_THROW();
	}
	PG_END_TRY();

	remote_connection_close(conn);
}

static void
server_bootstrap(const char *servername, const char *host, int32 port, const char *dbname,
				 bool if_not_exists, const char *bootstrap_database, const char *bootstrap_user,
				 const char *bootstrap_password)
{
	server_bootstrap_database(servername,
							  host,
							  port,
							  dbname,
							  if_not_exists,
							  bootstrap_database,
							  bootstrap_user,
							  bootstrap_password);

	server_bootstrap_extension(servername,
							   host,
							   port,
							   dbname,
							   if_not_exists,
							   bootstrap_user,
							   bootstrap_password);
}

static void
add_distributed_id_to_backend(const char *servername, const char *host, int32 port,
							  const char *dbname, bool if_not_exists, const char *user,
							  const char *user_password)
{
	PGconn *conn;
	List *server_options;

	server_options = create_server_options(host, port, dbname, user, user_password);
	conn = remote_connection_open((char *) servername, server_options, NULL, false);

	PG_TRY();
	{
		PGresult *res;
		char *request;
		Datum id_string = DirectFunctionCall1(uuid_out, dist_util_get_id());

		request = psprintf("SELECT * FROM _timescaledb_internal.set_dist_id('%s')",
						   DatumGetCString(id_string));
		res = remote_connection_query_ok_result(conn, request);
		remote_connection_result_close(res);
	}
	PG_CATCH();
	{
		remote_connection_close(conn);
		PG_RE_THROW();
	}
	PG_END_TRY();

	remote_connection_close(conn);
}

static void
remove_distributed_id_from_backend(const char *servername)
{
	ForeignServer *fs = GetForeignServerByName(servername, false);
	UserMapping *um;
	PGconn *conn;

	/* This try block is needed as GetUserMapping throws an error rather than returning NULL if a
	 * user mapping isn't found.  The catch block allows superusers to perform this operation
	 * without a user mapping. */
	PG_TRY();
	{
		um = GetUserMapping(GetUserId(), fs->serverid);
	}
	PG_CATCH();
	{
		um = NULL;
	}
	PG_END_TRY();
	conn = remote_connection_open((char *) servername, fs->options, um ? um->options : NULL, true);

	PG_TRY();
	{
		PGresult *res =
			remote_connection_query_ok_result(conn,
											  "SELECT * FROM "
											  "_timescaledb_internal.remove_from_dist_db();");
		remote_connection_result_close(res);
	}
	PG_CATCH();
	{
		remote_connection_close(conn);
		PG_RE_THROW();
	}
	PG_END_TRY();

	remote_connection_close(conn);
}

#endif /* !PG96 */

/* set_distid may need to be false for some otherwise invalid configurations that are useful for
 * testing */
static Datum
server_add_internal(PG_FUNCTION_ARGS, bool set_distid)
{
	const char *servername = PG_ARGISNULL(0) ? NULL : PG_GETARG_CSTRING(0);
	const char *host =
		PG_ARGISNULL(1) ? TS_DEFAULT_POSTGRES_HOST : TextDatumGetCString(PG_GETARG_DATUM(1));
	const char *dbname = PG_ARGISNULL(2) ? get_database_name(MyDatabaseId) : PG_GETARG_CSTRING(2);
	int32 port = PG_ARGISNULL(3) ? TS_DEFAULT_POSTGRES_PORT : PG_GETARG_INT32(3);
	Oid userid = PG_ARGISNULL(4) ? GetUserId() : PG_GETARG_OID(4);
	const char *server_username =
		PG_ARGISNULL(5) ? GetUserNameFromId(userid, false) : PG_GETARG_CSTRING(5);
	const char *password = PG_ARGISNULL(6) ? NULL : TextDatumGetCString(PG_GETARG_DATUM(6));
	bool if_not_exists = PG_ARGISNULL(7) ? false : PG_GETARG_BOOL(7);
	const char *bootstrap_database = PG_ARGISNULL(8) ? NULL : PG_GETARG_CSTRING(8);
	const char *bootstrap_user = NULL;
	const char *bootstrap_password = NULL;
	ForeignServer *server;
	UserMapping *um;
	const char *username;
	Oid serverid = InvalidOid;
	bool created = false;

	/* If bootstrap_user is not set, reuse server_username and its password */
	if (PG_ARGISNULL(9))
	{
		bootstrap_user = server_username;
		bootstrap_password = password;
	}
	else
	{
		bootstrap_user = PG_GETARG_CSTRING(9);
		bootstrap_password = PG_ARGISNULL(10) ? NULL : TextDatumGetCString(PG_GETARG_DATUM(10));
	}

#if !PG96
	if (set_distid && dist_util_membership() == DIST_MEMBER_BACKEND)
		ereport(ERROR,
				(errcode(ERRCODE_TS_SERVERS_ASSIGNMENT_ALREADY_EXISTS),
				 (errmsg("unable to assign backends to an existing backend database"))));
#endif

	if (NULL == bootstrap_database)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 (errmsg("invalid bootstrap database name"))));

	if (NULL == servername)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), (errmsg("invalid server name"))));

	if (port < 1 || port > PG_UINT16_MAX)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 (errmsg("invalid port"),
				  errhint("The port number must be between 1 and %u", PG_UINT16_MAX))));

	/*
	 * Since this function creates databases on remote nodes, and CREATE DATABASE
	 * cannot run in a transaction block, we cannot run the function in a
	 * transaction block either.
	 */
	PreventInTransactionBlock(true, "add_server");

	/*
	 * First check for existing foreign server. We could rely on
	 * if_not_exists, but it is not supported in PostgreSQL 9.6 for foreign
	 * servers or user mappings. We still pass use this argument in the create
	 * statement for newer versions in case we drop support 9.6 in the future.
	 */
	server = GetForeignServerByName(servername, true);

	if (NULL == server)
	{
		serverid = create_foreign_server(servername, host, port, dbname, if_not_exists);
		created = true;
	}
	else if (if_not_exists)
		serverid = server->serverid;
	else
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_OBJECT),
				 errmsg("server \"%s\" already exists", servername)));

	/*
	 * Make the foreign server visible in current transaction so that we can
	 * reference it when adding the user mapping
	 */
	CommandCounterIncrement();

	username = GetUserNameFromId(userid, false);

	um = get_user_mapping(userid, serverid);

	if (NULL == um)
	{
		if (!created)
			elog(NOTICE, "adding user mapping for \"%s\" to server \"%s\"", username, servername);

		create_user_mapping(username, server_username, servername, password, if_not_exists);

		/* Make user mapping visible */
		CommandCounterIncrement();

		um = GetUserMapping(userid, serverid);
	}
	else if (!if_not_exists)
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_OBJECT),
				 errmsg("user mapping for user \"%s\" and server \"%s\" already exists",
						username,
						servername)));

		/* Try to create database and extension on remote server */
#if !PG96
	server_bootstrap(servername,
					 host,
					 port,
					 dbname,
					 if_not_exists,
					 bootstrap_database,
					 bootstrap_user,
					 bootstrap_password);

	if (set_distid)
	{
		if (dist_util_membership() != DIST_MEMBER_FRONTEND)
			dist_util_set_as_frontend();

		add_distributed_id_to_backend(servername,
									  host,
									  port,
									  dbname,
									  if_not_exists,
									  bootstrap_user,
									  bootstrap_password);
	}

#else
	/* Those arguments are unused in 9.6, disable compiler warning */
	(void) bootstrap_database;
	(void) bootstrap_user;
	(void) bootstrap_password;
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 (errmsg("remote server bootstrapping only supported on PG10 and above"))));
#endif

	PG_RETURN_DATUM(create_server_datum(fcinfo,
										servername,
										host,
										port,
										dbname,
										username,
										server_username,
										created));
}

Datum
server_add(PG_FUNCTION_ARGS)
{
	return server_add_internal(fcinfo, true);
}

Datum
server_add_without_dist_id(PG_FUNCTION_ARGS)
{
	return server_add_internal(fcinfo, false);
}

Datum
server_delete(PG_FUNCTION_ARGS)
{
	const char *servername = PG_ARGISNULL(0) ? NULL : PG_GETARG_CSTRING(0);
	bool if_exists = PG_ARGISNULL(1) ? false : PG_GETARG_BOOL(1);
	bool cascade = PG_ARGISNULL(2) ? false : PG_GETARG_BOOL(2);
	ForeignServer *server = GetForeignServerByName(servername, if_exists);
	bool deleted = false;

	if (NULL != server)
	{
		DropStmt stmt = {
			.type = T_DropStmt,
#if PG96
			.objects = list_make1(list_make1(makeString(pstrdup(servername)))),
#else
			.objects = list_make1(makeString(pstrdup(servername))),
#endif
			.removeType = OBJECT_FOREIGN_SERVER,
			.behavior = cascade ? DROP_CASCADE : DROP_RESTRICT,
			.missing_ok = if_exists,
		};
		ObjectAddress address;
		ObjectAddress secondaryObject = InvalidObjectAddress;
		Node *parsetree = (Node *) &stmt;

		/* Make sure event triggers are invoked so that all dropped objects
		 * are collected during a cascading drop. This ensures all dependent
		 * objects get cleaned up. */
		EventTriggerBeginCompleteQuery();

#if !PG96
		remove_distributed_id_from_backend(servername);
#endif

		PG_TRY();
		{
			EventTriggerDDLCommandStart(parsetree);
			RemoveObjects(&stmt);
			EventTriggerCollectSimpleCommand(address, secondaryObject, parsetree);
			EventTriggerSQLDrop(parsetree);
			EventTriggerDDLCommandEnd(parsetree);
		}
		PG_CATCH();
		{
			EventTriggerEndCompleteQuery();
			PG_RE_THROW();
		}
		PG_END_TRY();

#if !PG96
		/* Remove self from dist db if no longer have backends */
		if (server_get_servername_list() == NIL)
			dist_util_remove_from_db();
#endif

		EventTriggerEndCompleteQuery();
		deleted = true;
	}

	PG_RETURN_BOOL(deleted);
}

Datum
server_attach(PG_FUNCTION_ARGS)
{
	Oid table_id = PG_GETARG_OID(0);
	const char *server_name = PG_ARGISNULL(1) ? NULL : PG_GETARG_NAME(1)->data;
	bool if_not_attached = PG_ARGISNULL(2) ? false : PG_GETARG_BOOL(2);
	Cache *hcache;
	Hypertable *ht;
	List *result;
	ListCell *lc;

	if (PG_ARGISNULL(0))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid hypertable: cannot be NULL")));

	if (server_name == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid server_name: cannot be NULL")));

	hcache = ts_hypertable_cache_pin();
	ht = ts_hypertable_cache_get_entry(hcache, table_id);

	if (ht == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_TS_HYPERTABLE_NOT_EXIST),
				 errmsg("table \"%s\" is not a hypertable", get_rel_name(table_id))));

	foreach (lc, ts_hypertable_server_scan(ht->fd.id, CurrentMemoryContext))
	{
		HypertableServer *server = lfirst(lc);

		if (namestrcmp(&server->fd.server_name, server_name) == 0)
		{
			ts_cache_release(hcache);
			if (if_not_attached)
			{
				ereport(NOTICE,
						(errcode(ERRCODE_TS_TABLESPACE_ALREADY_ATTACHED),
						 errmsg("server \"%s\" is already attached to hypertable \"%s\", skipping",
								server_name,
								get_rel_name(table_id))));
				PG_RETURN_DATUM(create_hypertable_server_datum(fcinfo, server));
			}
			else
				ereport(ERROR,
						(errcode(ERRCODE_TS_TABLESPACE_ALREADY_ATTACHED),
						 errmsg("server \"%s\" is already attached to hypertable \"%s\"",
								server_name,
								get_rel_name(table_id))));
		}
	}

	result = hypertable_assign_servers(ht->fd.id, list_make1((char *) server_name));
	Assert(result->length == 1);
	ts_cache_release(hcache);
	PG_RETURN_DATUM(create_hypertable_server_datum(fcinfo, (HypertableServer *) linitial(result)));
}

List *
server_get_servername_list(void)
{
	HeapTuple tuple;
	ScanKeyData scankey[1];
	SysScanDesc scandesc;
	Relation rel;
	ForeignDataWrapper *fdw = GetForeignDataWrapperByName(TIMESCALEDB_FDW_NAME, false);
	List *servers = NIL;

	rel = heap_open(ForeignServerRelationId, AccessShareLock);

	ScanKeyInit(&scankey[0],
				Anum_pg_foreign_server_srvfdw,
				BTEqualStrategyNumber,
				F_OIDEQ,
				ObjectIdGetDatum(fdw->fdwid));

	scandesc = systable_beginscan(rel, InvalidOid, false, NULL, 1, scankey);

	while (HeapTupleIsValid(tuple = systable_getnext(scandesc)))
	{
		Form_pg_foreign_server form = (Form_pg_foreign_server) GETSTRUCT(tuple);

		servers = lappend(servers, pstrdup(NameStr(form->srvname)));
	}

	systable_endscan(scandesc);
	heap_close(rel, AccessShareLock);

	return servers;
}

Datum
server_ping(PG_FUNCTION_ARGS)
{
#if !PG96
	char *server_name = PG_ARGISNULL(0) ? NULL : PG_GETARG_CSTRING(0);
	volatile PGconn *conn = NULL;
	volatile PGresult *res = NULL;
	ForeignServer *foregin_server;
	bool success = false;
	Oid timescale_fdw_oid;

	if (server_name == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid server_name: cannot be NULL")));

	/* Make sure server is defined, throw ERROR if not */
	foregin_server = GetForeignServerByName(server_name, false);
	timescale_fdw_oid = get_foreign_data_wrapper_oid(TIMESCALEDB_FDW_NAME, false);
	if (foregin_server->fdwid != timescale_fdw_oid)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid server_name: server `%s` is not a TimescaleDB server",
						server_name)));

	PG_TRY();
	{
		conn = remote_connection_open_default(server_name);
		res = remote_connection_query_ok_result((PGconn *) conn, ping_query);
		success = true;
	}
	PG_CATCH();
	{
		if (conn == NULL)
			elog(DEBUG1, "failed to open connection to server `%s`", server_name);
		else if (res == NULL)
			elog(DEBUG1, "query `%s` failed on server `%s`", ping_query, server_name);
		FlushErrorState();
	}
	PG_END_TRY();

	if (conn)
		remote_connection_close((PGconn *) conn);

	if (res)
		remote_connection_result_close((PGresult *) res);
	PG_RETURN_DATUM(BoolGetDatum(success));
#else
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 (errmsg("server ping is only supported on PG10 and above"))));
#endif
}
