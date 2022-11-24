/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <access/htup_details.h>
#include <access/xact.h>
#include <catalog/namespace.h>
#include <catalog/pg_database.h>
#include <catalog/pg_foreign_server.h>
#include <catalog/pg_inherits.h>
#include <catalog/pg_namespace.h>
#include <commands/dbcommands.h>
#include <commands/defrem.h>
#include <commands/event_trigger.h>
#include <compat/compat.h>
#include <executor/tuptable.h>
#include <extension.h>
#include <fmgr.h>
#include <funcapi.h>
#include <libpq/crypt.h>
#include <miscadmin.h>
#include <nodes/makefuncs.h>
#include <nodes/parsenodes.h>
#include <nodes/nodes.h>
#include <nodes/value.h>
#include <utils/acl.h>
#include <utils/builtins.h>
#include <utils/array.h>
#include <utils/builtins.h>
#include <utils/guc.h>
#include <utils/inval.h>
#include <utils/palloc.h>
#include <utils/syscache.h>

#include "compat/compat.h"
#include "config.h"
#include "extension.h"
#include "cache.h"
#include "chunk.h"
#include "fdw/fdw.h"
#include "remote/async.h"
#include "remote/connection.h"
#include "remote/connection_cache.h"
#include "remote/dist_commands.h"
#include "data_node.h"
#include "remote/utils.h"
#include "hypertable_cache.h"
#include "errors.h"
#include "dist_util.h"
#include "utils/uuid.h"
#include "mb/pg_wchar.h"
#include "scan_iterator.h"
#include "ts_catalog/catalog.h"
#include "ts_catalog/chunk_data_node.h"
#include "ts_catalog/dimension_partition.h"
#include "ts_catalog/hypertable_data_node.h"

#define TS_DEFAULT_POSTGRES_PORT 5432
#define TS_DEFAULT_POSTGRES_HOST "localhost"

#define ERRCODE_DUPLICATE_DATABASE_STR "42P04"
#define ERRCODE_DUPLICATE_SCHEMA_STR "42P06"

typedef struct DbInfo
{
	NameData name;
	int32 encoding;
	const char *chartype;
	const char *collation;
} DbInfo;

/* A list of databases we try to connect to when bootstrapping a data node */
static const char *bootstrap_databases[] = { "postgres", "template1", "defaultdb" };

static bool data_node_validate_database(TSConnection *conn, const DbInfo *database);

/*
 * get_database_info - given a database OID, look up info about the database
 *
 * Returns:
 *  True if a record for the OID was found, false otherwise.
 */
static bool
get_database_info(Oid dbid, DbInfo *database)
{
	HeapTuple dbtuple;
	Form_pg_database dbrecord;

	dbtuple = SearchSysCache1(DATABASEOID, ObjectIdGetDatum(dbid));

	if (!HeapTupleIsValid(dbtuple))
		return false;

	dbrecord = (Form_pg_database) GETSTRUCT(dbtuple);

	database->encoding = dbrecord->encoding;

#if PG15_LT
	database->collation = NameStr(dbrecord->datcollate);
	database->chartype = NameStr(dbrecord->datctype);
#else
	/*
	 * Since datcollate and datctype are varlen fields in PG15+ we cannot rely
	 * on GETSTRUCT filling them in as GETSTRUCT only works for fixed-length
	 * non-NULLABLE columns.
	 */
	Datum datum;
	bool isnull;

	datum = SysCacheGetAttr(DATABASEOID, dbtuple, Anum_pg_database_datcollate, &isnull);
	Assert(!isnull);
	database->collation = TextDatumGetCString(datum);

	datum = SysCacheGetAttr(DATABASEOID, dbtuple, Anum_pg_database_datctype, &isnull);
	Assert(!isnull);
	database->chartype = TextDatumGetCString(datum);
#endif

	database->collation = pstrdup(database->collation);
	database->chartype = pstrdup(database->chartype);

	ReleaseSysCache(dbtuple);
	return true;
}

/*
 * Verify that server is TimescaleDB data node and perform optional ACL check.
 *
 * The function returns true if the server is valid TimescaleDB data node and
 * the ACL check succeeds. Otherwise, false is returned, or, an error is thrown
 * if fail_on_aclcheck is set to true.
 */
static bool
validate_foreign_server(const ForeignServer *server, AclMode const mode, bool fail_on_aclcheck)
{
	Oid const fdwid = get_foreign_data_wrapper_oid(EXTENSION_FDW_NAME, false);
	Oid curuserid = GetUserId();
	AclResult aclresult;
	bool valid;

	Assert(NULL != server);
	if (server->fdwid != fdwid)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("data node \"%s\" is not a TimescaleDB server", server->servername)));

	if (mode == ACL_NO_CHECK)
		return true;

	/* Must have permissions on the server object */
	aclresult = pg_foreign_server_aclcheck(server->serverid, curuserid, mode);

	valid = (aclresult == ACLCHECK_OK);

	if (!valid && fail_on_aclcheck)
		aclcheck_error(aclresult, OBJECT_FOREIGN_SERVER, server->servername);

	return valid;
}

/*
 * Lookup the foreign server by name
 */
ForeignServer *
data_node_get_foreign_server(const char *node_name, AclMode mode, bool fail_on_aclcheck,
							 bool missing_ok)
{
	ForeignServer *server;
	bool valid;

	if (node_name == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("data node name cannot be NULL")));

	server = GetForeignServerByName(node_name, missing_ok);
	if (NULL == server)
		return NULL;

	valid = validate_foreign_server(server, mode, fail_on_aclcheck);

	if (mode != ACL_NO_CHECK && !valid)
		return NULL;

	return server;
}

ForeignServer *
data_node_get_foreign_server_by_oid(Oid server_oid, AclMode mode)
{
	ForeignServer *server = GetForeignServer(server_oid);
	bool PG_USED_FOR_ASSERTS_ONLY valid = validate_foreign_server(server, mode, true);
	Assert(valid); /* Should always be valid since we should see error otherwise */
	return server;
}

/*
 * Create a foreign server.
 *
 * Returns true if the server was created and set the `oid` to the server oid.
 */
static bool
create_foreign_server(const char *const node_name, const char *const host, int32 port,
					  const char *const dbname, bool if_not_exists)
{
	ForeignServer *server = NULL;
	ObjectAddress objaddr;
	CreateForeignServerStmt stmt = {
		.type = T_CreateForeignServerStmt,
		.servername = (char *) node_name,
		.fdwname = EXTENSION_FDW_NAME,
		.options = list_make3(makeDefElem("host", (Node *) makeString(pstrdup(host)), -1),
							  makeDefElem("port", (Node *) makeInteger(port), -1),
							  makeDefElem("dbname", (Node *) makeString(pstrdup(dbname)), -1)),
		.if_not_exists = if_not_exists,
	};

	if (NULL == host)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 (errmsg("invalid host"),
				  (errhint("A hostname or IP address must be specified when "
						   "a data node does not already exist.")))));

	if (if_not_exists)
	{
		server = data_node_get_foreign_server(node_name, ACL_NO_CHECK, false, true);

		if (NULL != server)
		{
			ereport(NOTICE,
					(errcode(ERRCODE_DUPLICATE_OBJECT),
					 errmsg("data node \"%s\" already exists, skipping", node_name)));
			return false;
		}
	}

	/* Permissions checks done in CreateForeignServer() */
	objaddr = CreateForeignServer(&stmt);

	/* CreateForeignServer returns InvalidOid if server already exists */
	if (!OidIsValid(objaddr.objectId))
	{
		Assert(if_not_exists);
		return false;
	}

	return true;
}

TSConnection *
data_node_get_connection(const char *const data_node, RemoteTxnPrepStmtOption const ps_opt,
						 bool transactional)
{
	const ForeignServer *server;
	TSConnectionId id;

	Assert(data_node != NULL);
	server = data_node_get_foreign_server(data_node, ACL_NO_CHECK, false, false);
	id = remote_connection_id(server->serverid, GetUserId());

	if (transactional)
		return remote_dist_txn_get_connection(id, ps_opt);

	return remote_connection_cache_get_connection(id);
}

/* Attribute numbers for datum returned by create_data_node() */
enum Anum_create_data_node
{
	Anum_create_data_node_name = 1,
	Anum_create_data_node_host,
	Anum_create_data_node_port,
	Anum_create_data_node_dbname,
	Anum_create_data_node_node_created,
	Anum_create_data_node_database_created,
	Anum_create_data_node_extension_created,
	_Anum_create_data_node_max,
};

#define Natts_create_data_node (_Anum_create_data_node_max - 1)

static Datum
create_data_node_datum(FunctionCallInfo fcinfo, const char *node_name, const char *host, int32 port,
					   const char *dbname, bool node_created, bool database_created,
					   bool extension_created)
{
	TupleDesc tupdesc;
	Datum values[_Anum_create_data_node_max];
	bool nulls[_Anum_create_data_node_max] = { false };
	HeapTuple tuple;

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("function returning record called in "
						"context that cannot accept type record")));

	tupdesc = BlessTupleDesc(tupdesc);
	values[AttrNumberGetAttrOffset(Anum_create_data_node_name)] = CStringGetDatum(node_name);
	values[AttrNumberGetAttrOffset(Anum_create_data_node_host)] = CStringGetTextDatum(host);
	values[AttrNumberGetAttrOffset(Anum_create_data_node_port)] = Int32GetDatum(port);
	values[AttrNumberGetAttrOffset(Anum_create_data_node_dbname)] = CStringGetDatum(dbname);
	values[AttrNumberGetAttrOffset(Anum_create_data_node_node_created)] =
		BoolGetDatum(node_created);
	values[AttrNumberGetAttrOffset(Anum_create_data_node_database_created)] =
		BoolGetDatum(database_created);
	values[AttrNumberGetAttrOffset(Anum_create_data_node_extension_created)] =
		BoolGetDatum(extension_created);
	tuple = heap_form_tuple(tupdesc, values, nulls);

	return HeapTupleGetDatum(tuple);
}

static Datum
create_hypertable_data_node_datum(FunctionCallInfo fcinfo, HypertableDataNode *node)
{
	TupleDesc tupdesc;
	Datum values[Natts_hypertable_data_node];
	bool nulls[Natts_hypertable_data_node] = { false };
	HeapTuple tuple;

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("function returning record called in "
						"context that cannot accept type record")));

	tupdesc = BlessTupleDesc(tupdesc);
	values[AttrNumberGetAttrOffset(Anum_hypertable_data_node_hypertable_id)] =
		Int32GetDatum(node->fd.hypertable_id);
	values[AttrNumberGetAttrOffset(Anum_hypertable_data_node_node_hypertable_id)] =
		Int32GetDatum(node->fd.node_hypertable_id);
	values[AttrNumberGetAttrOffset(Anum_hypertable_data_node_node_name)] =
		NameGetDatum(&node->fd.node_name);
	tuple = heap_form_tuple(tupdesc, values, nulls);

	return HeapTupleGetDatum(tuple);
}

static List *
create_data_node_options(const char *host, int32 port, const char *dbname, const char *user,
						 const char *password)
{
	DefElem *host_elm = makeDefElem("host", (Node *) makeString(pstrdup(host)), -1);
	DefElem *port_elm = makeDefElem("port", (Node *) makeInteger(port), -1);
	DefElem *dbname_elm = makeDefElem("dbname", (Node *) makeString(pstrdup(dbname)), -1);
	DefElem *user_elm = makeDefElem("user", (Node *) makeString(pstrdup(user)), -1);

	if (NULL != password)
	{
		DefElem *password_elm = makeDefElem("password", (Node *) makeString(pstrdup(password)), -1);
		return list_make5(host_elm, port_elm, dbname_elm, user_elm, password_elm);
	}

	return list_make4(host_elm, port_elm, dbname_elm, user_elm);
}

/* Returns 'true' if the database was created. */
static bool
data_node_bootstrap_database(TSConnection *conn, const DbInfo *database)
{
	const char *const username = PQuser(remote_connection_get_pg_conn(conn));

	Assert(database);

	if (data_node_validate_database(conn, database))
	{
		/* If the database already existed on the remote node, we will log a
		 * notice and proceed since it is not an error if the database already
		 * existed on the remote node. */
		elog(NOTICE,
			 "database \"%s\" already exists on data node, skipping",
			 NameStr(database->name));
		return false;
	}

	/* Create the database with the user as owner. There is no need to
	 * validate the database after this command since it should be created
	 * correctly. */
	PGresult *res =
		remote_connection_execf(conn,
								"CREATE DATABASE %s ENCODING %s LC_COLLATE %s LC_CTYPE %s "
								"TEMPLATE template0 OWNER %s",
								quote_identifier(NameStr(database->name)),
								quote_identifier(pg_encoding_to_char(database->encoding)),
								quote_literal_cstr(database->collation),
								quote_literal_cstr(database->chartype),
								quote_identifier(username));
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
		remote_result_elog(res, ERROR);
	return true;
}

/* Validate the database.
 *
 * Errors:
 *   Will abort with errors if the database exists but is not correctly set
 *   up.
 * Returns:
 *   true if the database exists and is valid
 *   false if it does not exist.
 */
static bool
data_node_validate_database(TSConnection *conn, const DbInfo *database)
{
	PGresult *res;
	int32 actual_encoding;
	const char *actual_chartype;
	const char *actual_collation;

	res = remote_connection_execf(conn,
								  "SELECT encoding, datcollate, datctype "
								  "FROM pg_database WHERE datname = %s",
								  quote_literal_cstr(NameStr(database->name)));

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_EXCEPTION), errmsg("%s", PQresultErrorMessage(res))));

	if (PQntuples(res) == 0)
		return false;

	Assert(PQnfields(res) > 2);

	actual_encoding = atoi(PQgetvalue(res, 0, 0));
	if (actual_encoding != database->encoding)
		ereport(ERROR,
				(errcode(ERRCODE_TS_DATA_NODE_INVALID_CONFIG),
				 errmsg("database exists but has wrong encoding"),
				 errdetail("Expected database encoding to be \"%s\" (%u) but it was \"%s\" (%u).",
						   pg_encoding_to_char(database->encoding),
						   database->encoding,
						   pg_encoding_to_char(actual_encoding),
						   actual_encoding)));

	actual_collation = PQgetvalue(res, 0, 1);
	Assert(actual_collation != NULL);
	if (strcmp(actual_collation, database->collation) != 0)
		ereport(ERROR,
				(errcode(ERRCODE_TS_DATA_NODE_INVALID_CONFIG),
				 errmsg("database exists but has wrong collation"),
				 errdetail("Expected collation \"%s\" but it was \"%s\".",
						   database->collation,
						   actual_collation)));

	actual_chartype = PQgetvalue(res, 0, 2);
	Assert(actual_chartype != NULL);
	if (strcmp(actual_chartype, database->chartype) != 0)
		ereport(ERROR,
				(errcode(ERRCODE_TS_DATA_NODE_INVALID_CONFIG),
				 errmsg("database exists but has wrong LC_CTYPE"),
				 errdetail("Expected LC_CTYPE \"%s\" but it was \"%s\".",
						   database->chartype,
						   actual_chartype)));
	return true;
}

static void
data_node_validate_extension(TSConnection *conn)
{
	const char *const dbname = PQdb(remote_connection_get_pg_conn(conn));
	const char *const host = PQhost(remote_connection_get_pg_conn(conn));
	const char *const port = PQport(remote_connection_get_pg_conn(conn));

	if (!remote_connection_check_extension(conn))
		ereport(ERROR,
				(errcode(ERRCODE_TS_DATA_NODE_INVALID_CONFIG),
				 errmsg("database does not have TimescaleDB extension loaded"),
				 errdetail("The TimescaleDB extension is not loaded in database %s on node at "
						   "%s:%s.",
						   dbname,
						   host,
						   port)));
}

static void
data_node_validate_as_data_node(TSConnection *conn)
{
	PGresult *res =
		remote_connection_exec(conn, "SELECT _timescaledb_internal.validate_as_data_node()");

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
		ereport(ERROR,
				(errcode(ERRCODE_TS_DATA_NODE_INVALID_CONFIG),
				 (errmsg("cannot add \"%s\" as a data node", remote_connection_node_name(conn)),
				  errdetail("%s", PQresultErrorMessage(res)))));

	remote_result_close(res);
}

/*
 * Bootstrap the extension and associated objects.
 */
static bool
data_node_bootstrap_extension(TSConnection *conn)
{
	const char *const username = PQuser(remote_connection_get_pg_conn(conn));
	const char *schema_name = ts_extension_schema_name();
	const char *schema_name_quoted = quote_identifier(schema_name);
	Oid schema_oid = get_namespace_oid(schema_name, true);

	/* We only count the number of tuples in the code below, but having the
	 * name and version are useful for debugging purposes. */
	PGresult *res =
		remote_connection_execf(conn,
								"SELECT extname, extversion FROM pg_extension WHERE extname = %s",
								quote_literal_cstr(EXTENSION_NAME));

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_EXCEPTION), errmsg("%s", PQresultErrorMessage(res))));

	if (PQntuples(res) == 0)
	{
		if (schema_oid != PG_PUBLIC_NAMESPACE)
		{
			PGresult *res = remote_connection_execf(conn,
													"CREATE SCHEMA %s AUTHORIZATION %s",
													schema_name_quoted,
													quote_identifier(username));
			if (PQresultStatus(res) != PGRES_COMMAND_OK)
			{
				const char *const sqlstate = PQresultErrorField(res, PG_DIAG_SQLSTATE);
				bool schema_exists =
					(sqlstate && strcmp(sqlstate, ERRCODE_DUPLICATE_SCHEMA_STR) == 0);
				if (!schema_exists)
					remote_result_elog(res, ERROR);
				/* If the schema already existed on the remote node, we got a
				 * duplicate schema error and the schema was not created. In
				 * that case, we log an error with a hint on how to fix the
				 * issue. */
				ereport(ERROR,
						(errcode(ERRCODE_DUPLICATE_SCHEMA),
						 errmsg("schema \"%s\" already exists in database, aborting", schema_name),
						 errhint("Make sure that the data node does not contain any "
								 "existing objects prior to adding it.")));
			}
		}

		remote_connection_cmdf_ok(conn,
								  "CREATE EXTENSION " EXTENSION_NAME
								  " WITH SCHEMA %s VERSION %s CASCADE",
								  schema_name_quoted,
								  quote_literal_cstr(ts_extension_get_version()));
		return true;
	}
	else
	{
		ereport(NOTICE,
				(errmsg("extension \"%s\" already exists on data node, skipping",
						PQgetvalue(res, 0, 0)),
				 errdetail("TimescaleDB extension version on %s:%s was %s.",
						   PQhost(remote_connection_get_pg_conn(conn)),
						   PQport(remote_connection_get_pg_conn(conn)),
						   PQgetvalue(res, 0, 1))));
		data_node_validate_extension(conn);
		return false;
	}
}

/* Add dist_uuid on the remote node.
 *
 * If the remote node is set to use the current database, `set_dist_id` will report an error and not
 * set it. */
static void
add_distributed_id_to_data_node(TSConnection *conn)
{
	Datum id_string = DirectFunctionCall1(uuid_out, dist_util_get_id());
	PGresult *res = remote_connection_queryf_ok(conn,
												"SELECT _timescaledb_internal.set_dist_id('%s')",
												DatumGetCString(id_string));
	remote_result_close(res);
}

/*
 * Connect to do bootstrapping.
 *
 * We iterate through the list of databases and try to connect to so we can
 * bootstrap the data node.
 */
static TSConnection *
connect_for_bootstrapping(const char *node_name, const char *const host, int32 port,
						  const char *username, const char *password)
{
	TSConnection *conn = NULL;
	char *err = NULL;

	for (size_t i = 0; i < lengthof(bootstrap_databases); i++)
	{
		List *node_options =
			create_data_node_options(host, port, bootstrap_databases[i], username, password);
		conn = remote_connection_open_with_options_nothrow(node_name, node_options, &err);

		if (conn)
			return conn;
	}

	ereport(ERROR,
			(errcode(ERRCODE_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION),
			 errmsg("could not connect to \"%s\"", node_name),
			 err == NULL ? 0 : errdetail("%s", err)));

	pg_unreachable();

	return NULL;
}

/*
 * Validate that compatible extension is available on the data node.
 *
 * We check all available extension versions. Since we are connected to
 * template DB when performing this check, it means we can't
 * really tell if a compatible extension is installed in the database we
 * are trying to add to the cluster. However we can make sure that a user
 * will be able to manually upgrade the extension on the data node if needed.
 *
 * Will abort with error if there is no compatible version available, otherwise do nothing.
 */
static void
data_node_validate_extension_availability(TSConnection *conn)
{
	StringInfo concat_versions = makeStringInfo();
	bool compatible = false;
	PGresult *res;
	int i;

	res =
		remote_connection_execf(conn,
								"SELECT version FROM pg_available_extension_versions WHERE name = "
								"%s AND version ~ '\\d+.\\d+.\\d+.*' ORDER BY version DESC",
								quote_literal_cstr(EXTENSION_NAME));

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_EXCEPTION), errmsg("%s", PQresultErrorMessage(res))));

	if (PQntuples(res) == 0)
		ereport(ERROR,
				(errcode(ERRCODE_TS_DATA_NODE_INVALID_CONFIG),
				 errmsg("TimescaleDB extension not available on remote PostgreSQL instance"),
				 errhint("Install the TimescaleDB extension on the remote PostgresSQL instance.")));

	Assert(PQnfields(res) == 1);

	for (i = 0; i < PQntuples(res); i++)
	{
		appendStringInfo(concat_versions, "%s, ", PQgetvalue(res, i, 0));
		compatible = dist_util_is_compatible_version(PQgetvalue(res, i, 0), TIMESCALEDB_VERSION);
		if (compatible)
			break;
	}

	if (!compatible)
		ereport(ERROR,
				(errcode(ERRCODE_TS_DATA_NODE_INVALID_CONFIG),
				 errmsg("remote PostgreSQL instance has an incompatible timescaledb extension "
						"version"),
				 errdetail_internal("Access node version: %s, available remote versions: %s.",
									TIMESCALEDB_VERSION_MOD,
									concat_versions->data)));
}

/**
 * Get the configured server port for the server as an integer.
 *
 * Returns:
 *   Port number if a port is configured, -1 if it is not able to get
 *   the port number.
 *
 * Note:
 *   We cannot use `inet_server_port()` since that will return NULL if
 *   connecting to a server on localhost since a UNIX socket will be
 *   used. This is the case even if explicitly using a port when
 *   connecting. Regardless of how the user connected, we want to use the same
 *   port as the one that the server listens on.
 */
static int32
get_server_port()
{
	const char *const portstr =
		GetConfigOption("port", /* missing_ok= */ false, /* restrict_privileged= */ false);
	return pg_strtoint32(portstr);
}

static void
validate_data_node_port(int port)
{
	if (port < 1 || port > PG_UINT16_MAX)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 (errmsg("invalid port number %d", port),
				  errhint("The port number must be between 1 and %u.", PG_UINT16_MAX))));
}

/* set_distid may need to be false for some otherwise invalid configurations
 * that are useful for testing */
static Datum
data_node_add_internal(PG_FUNCTION_ARGS, bool set_distid)
{
	Oid userid = GetUserId();
	const char *username = GetUserNameFromId(userid, false);
	const char *node_name = PG_ARGISNULL(0) ? NULL : PG_GETARG_CSTRING(0);
	const char *host = PG_ARGISNULL(1) ? NULL : TextDatumGetCString(PG_GETARG_DATUM(1));
	const char *dbname = PG_ARGISNULL(2) ? get_database_name(MyDatabaseId) : PG_GETARG_CSTRING(2);
	int32 port = PG_ARGISNULL(3) ? get_server_port() : PG_GETARG_INT32(3);
	bool if_not_exists = PG_ARGISNULL(4) ? false : PG_GETARG_BOOL(4);
	bool bootstrap = PG_ARGISNULL(5) ? true : PG_GETARG_BOOL(5);
	const char *password = PG_ARGISNULL(6) ? NULL : TextDatumGetCString(PG_GETARG_DATUM(6));
	bool server_created = false;
	bool database_created = false;
	bool extension_created = false;
	bool PG_USED_FOR_ASSERTS_ONLY result;
	DbInfo database;

	TS_PREVENT_FUNC_IF_READ_ONLY();

	namestrcpy(&database.name, dbname);

	if (host == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 (errmsg("a host needs to be specified"),
				  errhint("Provide a host name or IP address of a data node to add."))));

	if (set_distid && dist_util_membership() == DIST_MEMBER_DATA_NODE)
		ereport(ERROR,
				(errcode(ERRCODE_TS_DATA_NODE_ASSIGNMENT_ALREADY_EXISTS),
				 (errmsg("unable to assign data nodes from an existing distributed database"))));

	if (NULL == node_name)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 (errmsg("data node name cannot be NULL"))));

	validate_data_node_port(port);

	result = get_database_info(MyDatabaseId, &database);
	Assert(result);

	/*
	 * Since this function creates databases on remote nodes, and CREATE DATABASE
	 * cannot run in a transaction block, we cannot run the function in a
	 * transaction block either.
	 */
	TS_PREVENT_IN_TRANSACTION_BLOCK(true);

	/* Try to create the foreign server, or get the existing one in case of
	 * if_not_exists true. */
	if (create_foreign_server(node_name, host, port, dbname, if_not_exists))
	{
		List *node_options;
		TSConnection *conn;

		server_created = true;

		/* Make the foreign server visible in current transaction. */
		CommandCounterIncrement();

		/* If bootstrapping, we check the extension availability here and
		 * abort if the extension is not available. We should not start
		 * creating databases and other cruft on the datanode unless we know
		 * that the extension is installed.
		 *
		 * We ensure that there is a database if we are bootstrapping. This is
		 * done using a separate connection since the database that is going
		 * to be used for the data node does not exist yet, so we cannot
		 * connect to it. */
		if (bootstrap)
		{
			TSConnection *conn =
				connect_for_bootstrapping(node_name, host, port, username, password);
			Assert(NULL != conn);
			data_node_validate_extension_availability(conn);
			database_created = data_node_bootstrap_database(conn, &database);
			remote_connection_close(conn);
		}

		/* Connect to the database we are bootstrapping and either install the
		 * extension or validate that the extension is installed. The
		 * following statements are executed inside a transaction so that they
		 * can be rolled back in the event of a failure.
		 *
		 * We could use `remote_dist_txn_get_connection` here, but it is
		 * comparably heavy and make the code more complicated than
		 * necessary. Instead using a more straightforward approach here since
		 * we do not need 2PC support. */
		node_options = create_data_node_options(host, port, dbname, username, password);
		conn = remote_connection_open_with_options(node_name, node_options, false);
		Assert(NULL != conn);
		remote_connection_cmd_ok(conn, "BEGIN");

		if (bootstrap)
			extension_created = data_node_bootstrap_extension(conn);

		if (!database_created)
		{
			data_node_validate_database(conn, &database);
			data_node_validate_as_data_node(conn);
		}

		if (!extension_created)
			data_node_validate_extension(conn);

		/* After the node is verified or bootstrapped, we set the `dist_uuid`
		 * using the same connection. We skip this if clustering checks are
		 * disabled, which means that the `dist_uuid` is neither set nor
		 * checked.
		 *
		 * This is done inside a transaction so that we can roll it back if
		 * there are any failures. Note that any failure at this point will
		 * not rollback the creates above. */
		if (set_distid)
		{
			if (dist_util_membership() != DIST_MEMBER_ACCESS_NODE)
				dist_util_set_as_access_node();
			add_distributed_id_to_data_node(conn);
		}

		/* If there were an error before, we will not reach this point to the
		 * transaction will be aborted when the connection is closed. */
		remote_connection_cmd_ok(conn, "COMMIT");
		remote_connection_close(conn);
	}

	PG_RETURN_DATUM(create_data_node_datum(fcinfo,
										   node_name,
										   host,
										   port,
										   dbname,
										   server_created,
										   database_created,
										   extension_created));
}

Datum
data_node_add(PG_FUNCTION_ARGS)
{
	return data_node_add_internal(fcinfo, true);
}

Datum
data_node_add_without_dist_id(PG_FUNCTION_ARGS)
{
	return data_node_add_internal(fcinfo, false);
}

Datum
data_node_attach(PG_FUNCTION_ARGS)
{
	const char *node_name = PG_ARGISNULL(0) ? NULL : PG_GETARG_CSTRING(0);
	Oid table_id = PG_GETARG_OID(1);
	bool if_not_attached = PG_ARGISNULL(2) ? false : PG_GETARG_BOOL(2);
	bool repartition = PG_ARGISNULL(3) ? false : PG_GETARG_BOOL(3);
	ForeignServer *fserver;
	HypertableDataNode *node;
	Cache *hcache;
	Hypertable *ht;
	Dimension *space_dim;
	List *result;
	int num_nodes;
	ListCell *lc;
	Oid uid, saved_uid;
	int sec_ctx;
	Relation rel;

	TS_PREVENT_FUNC_IF_READ_ONLY();

	if (PG_ARGISNULL(1))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("hypertable cannot be NULL")));
	Assert(get_rel_name(table_id));

	ht = ts_hypertable_cache_get_cache_and_entry(table_id, CACHE_FLAG_NONE, &hcache);
	Assert(ht != NULL);

	if (!hypertable_is_distributed(ht))
		ereport(ERROR,
				(errcode(ERRCODE_TS_HYPERTABLE_NOT_DISTRIBUTED),
				 errmsg("hypertable \"%s\" is not distributed", get_rel_name(table_id))));

	/* Must have owner permissions on the hypertable to attach a new data
	   node. Must also have USAGE on the foreign server.  */
	ts_hypertable_permissions_check(table_id, GetUserId());
	fserver = data_node_get_foreign_server(node_name, ACL_USAGE, true, false);

	Assert(NULL != fserver);

	foreach (lc, ht->data_nodes)
	{
		node = lfirst(lc);

		if (node->foreign_server_oid == fserver->serverid)
		{
			ts_cache_release(hcache);

			if (if_not_attached)
			{
				ereport(NOTICE,
						(errcode(ERRCODE_TS_DATA_NODE_ALREADY_ATTACHED),
						 errmsg("data node \"%s\" is already attached to hypertable \"%s\", "
								"skipping",
								node_name,
								get_rel_name(table_id))));
				PG_RETURN_DATUM(create_hypertable_data_node_datum(fcinfo, node));
			}
			else
				ereport(ERROR,
						(errcode(ERRCODE_TS_DATA_NODE_ALREADY_ATTACHED),
						 errmsg("data node \"%s\" is already attached to hypertable \"%s\"",
								node_name,
								get_rel_name(table_id))));
		}
	}

	/*
	 * Change to the hypertable owner so that the same permissions will be set up on the
	 * datanode being attached to as well. We need to do this explicitly because the
	 * caller of this function could be a superuser and we definitely don't want to create
	 * this hypertable with superuser ownership on the datanode being attached to!
	 *
	 * We retain the lock on the hypertable till the end of the traction to avoid any
	 * possibility of a concurrent "ALTER TABLE OWNER TO" changing the owner underneath
	 * us.
	 */
	rel = table_open(ht->main_table_relid, AccessShareLock);
	uid = rel->rd_rel->relowner;
	table_close(rel, NoLock);
	GetUserIdAndSecContext(&saved_uid, &sec_ctx);

	if (uid != saved_uid)
		SetUserIdAndSecContext(uid, sec_ctx | SECURITY_LOCAL_USERID_CHANGE);

	result = hypertable_assign_data_nodes(ht->fd.id, list_make1((char *) node_name));
	Assert(result->length == 1);

	/* Refresh the cached hypertable entry to get the attached node */
	ts_cache_release(hcache);
	hcache = ts_hypertable_cache_pin();
	ht = ts_hypertable_cache_get_entry(hcache, table_id, CACHE_FLAG_NONE);

	/* Get the first closed (space) dimension, which is the one along which we
	 * partition across data nodes. */
	space_dim = ts_hyperspace_get_mutable_dimension(ht->space, DIMENSION_TYPE_CLOSED, 0);
	num_nodes = list_length(ht->data_nodes);

	if (num_nodes > MAX_NUM_HYPERTABLE_DATA_NODES)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("max number of data nodes already attached"),
				 errdetail("The number of data nodes in a hypertable cannot exceed %d.",
						   MAX_NUM_HYPERTABLE_DATA_NODES)));

	/* If there are less slices (partitions) in the space dimension than there
	 * are data nodes, we'd like to expand the number of slices to be able to
	 * make use of the new data node. */
	if (NULL != space_dim)
	{
		List *data_node_names = NIL;

		if (num_nodes > space_dim->fd.num_slices)
		{
			if (repartition)
			{
				ts_dimension_set_number_of_slices(space_dim, num_nodes & 0xFFFF);

				ereport(NOTICE,
						(errmsg("the number of partitions in dimension \"%s\" was increased to %u",
								NameStr(space_dim->fd.column_name),
								num_nodes),
						 errdetail("To make use of all attached data nodes, a distributed "
								   "hypertable needs at least as many partitions in the first "
								   "closed (space) dimension as there are attached data nodes.")));
			}
			else
			{
				/* Raise a warning if the number of partitions are too few to make
				 * use of all data nodes. Need to refresh cache first to get the
				 * updated data node list. */
				int dimension_id = space_dim->fd.id;
				ts_hypertable_check_partitioning(ht, dimension_id);
			}
		}

		data_node_names = ts_hypertable_get_available_data_node_names(ht, true);
		ts_dimension_partition_info_recreate(space_dim->fd.id,
											 num_nodes,
											 data_node_names,
											 ht->fd.replication_factor);
	}

	node = linitial(result);
	ts_cache_release(hcache);

	/* Need to restore security context */
	if (uid != saved_uid)
		SetUserIdAndSecContext(saved_uid, sec_ctx);

	PG_RETURN_DATUM(create_hypertable_data_node_datum(fcinfo, node));
}

/* Only used for generating proper error message */
typedef enum OperationType
{
	OP_BLOCK,
	OP_DETACH,
	OP_DELETE
} OperationType;

static void
check_replication_for_new_data(const char *node_name, Hypertable *ht, bool force)
{
	List *available_nodes = ts_hypertable_get_available_data_nodes(ht, false);

	if (ht->fd.replication_factor < list_length(available_nodes))
		return;

	ereport(force ? WARNING : ERROR,
			(errcode(ERRCODE_TS_INSUFFICIENT_NUM_DATA_NODES),
			 errmsg("insufficient number of data nodes for distributed hypertable \"%s\"",
					NameStr(ht->fd.table_name)),
			 errdetail("Reducing the number of available data nodes on distributed"
					   " hypertable \"%s\" prevents full replication of new chunks.",
					   NameStr(ht->fd.table_name)),
			 force ? 0 : errhint("Use force => true to force this operation.")));
}

static bool
data_node_contains_non_replicated_chunks(List *chunk_data_nodes)
{
	ListCell *lc;

	foreach (lc, chunk_data_nodes)
	{
		ChunkDataNode *cdn = lfirst(lc);
		List *replicas =
			ts_chunk_data_node_scan_by_chunk_id(cdn->fd.chunk_id, CurrentMemoryContext);

		if (list_length(replicas) < 2)
			return true;
	}

	return false;
}

static List *
data_node_detach_or_delete_validate(const char *node_name, Hypertable *ht, bool force,
									OperationType op_type)
{
	List *chunk_data_nodes =
		ts_chunk_data_node_scan_by_node_name_and_hypertable_id(node_name,
															   ht->fd.id,
															   CurrentMemoryContext);
	bool has_non_replicated_chunks = data_node_contains_non_replicated_chunks(chunk_data_nodes);

	Assert(op_type == OP_DELETE || op_type == OP_DETACH);

	if (has_non_replicated_chunks)
		ereport(ERROR,
				(errcode(ERRCODE_TS_INSUFFICIENT_NUM_DATA_NODES),
				 errmsg("insufficient number of data nodes"),
				 errdetail("Distributed hypertable \"%s\" would lose data if"
						   " data node \"%s\" is %s.",
						   NameStr(ht->fd.table_name),
						   node_name,
						   (op_type == OP_DELETE) ? "deleted" : "detached"),
				 errhint("Ensure all chunks on the data node are fully replicated before %s it.",
						 (op_type == OP_DELETE) ? "deleting" : "detaching")));

	if (list_length(chunk_data_nodes) > 0)
	{
		if (force)
			ereport(WARNING,
					(errcode(ERRCODE_TS_INSUFFICIENT_NUM_DATA_NODES),
					 errmsg("distributed hypertable \"%s\" is under-replicated",
							NameStr(ht->fd.table_name)),
					 errdetail("Some chunks no longer meet the replication target"
							   " after %s data node \"%s\".",
							   (op_type == OP_DELETE) ? "deleting" : "detaching",
							   node_name)));
		else
			ereport(ERROR,
					(errcode(ERRCODE_TS_DATA_NODE_IN_USE),
					 errmsg("data node \"%s\" still holds data for distributed hypertable \"%s\"",
							node_name,
							NameStr(ht->fd.table_name))));
	}

	check_replication_for_new_data(node_name, ht, force);

	return chunk_data_nodes;
}

static int
data_node_modify_hypertable_data_nodes(const char *node_name, List *hypertable_data_nodes,
									   bool all_hypertables, OperationType op_type,
									   bool block_chunks, bool force, bool repartition,
									   bool drop_remote_data)
{
	Cache *hcache = ts_hypertable_cache_pin();
	ListCell *lc;
	int removed = 0;

	foreach (lc, hypertable_data_nodes)
	{
		HypertableDataNode *node = lfirst(lc);
		Oid relid = ts_hypertable_id_to_relid(node->fd.hypertable_id);
		Hypertable *ht = ts_hypertable_cache_get_entry(hcache, relid, CACHE_FLAG_NONE);
		bool has_privs = ts_hypertable_has_privs_of(relid, GetUserId());
		bool update_dimension_partitions = false;
		Dimension *space_dim;

		Assert(ht != NULL);
		space_dim = ts_hyperspace_get_mutable_dimension(ht->space, DIMENSION_TYPE_CLOSED, 0);

		if (!has_privs)
		{
			/* If the operation is OP_DELETE, we MUST be able to detach the data
			 * node from ALL tables since the foreign server object will be
			 * deleted. Therefore, we fail the operation if we find a table
			 * that we don't have owner permissions on in this case. */
			if (all_hypertables && op_type != OP_DELETE)
				ereport(NOTICE,
						(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
						 errmsg("skipping hypertable \"%s\" due to missing permissions",
								get_rel_name(relid))));
			else
				ereport(ERROR,
						(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
						 errmsg("permission denied for hypertable \"%s\"", get_rel_name(relid)),
						 errdetail("The data node is attached to hypertables that the current "
								   "user lacks permissions for.")));
		}
		else if (op_type == OP_DETACH || op_type == OP_DELETE)
		{
			/* we have permissions to detach */
			List *chunk_data_nodes =
				data_node_detach_or_delete_validate(NameStr(node->fd.node_name),
													ht,
													force,
													op_type);
			ListCell *cs_lc;

			/* update chunk foreign table server and delete chunk mapping */
			foreach (cs_lc, chunk_data_nodes)
			{
				ChunkDataNode *cdn = lfirst(cs_lc);
				const Chunk *chunk = ts_chunk_get_by_id(cdn->fd.chunk_id, true);
				chunk_update_foreign_server_if_needed(chunk, cdn->foreign_server_oid, false);
				ts_chunk_data_node_delete_by_chunk_id_and_node_name(cdn->fd.chunk_id,
																	NameStr(cdn->fd.node_name));
			}

			/* delete hypertable mapping */
			removed +=
				ts_hypertable_data_node_delete_by_node_name_and_hypertable_id(node_name, ht->fd.id);

			if (repartition)
			{
				int num_nodes = list_length(ht->data_nodes) - 1;

				if (space_dim != NULL && num_nodes < space_dim->fd.num_slices && num_nodes > 0)
				{
					ts_dimension_set_number_of_slices(space_dim, num_nodes & 0xFFFF);

					ereport(NOTICE,
							(errmsg("the number of partitions in dimension \"%s\" of hypertable "
									"\"%s\" was decreased to %u",
									NameStr(space_dim->fd.column_name),
									get_rel_name(ht->main_table_relid),
									num_nodes),
							 errdetail(
								 "To make efficient use of all attached data nodes, the number of "
								 "space partitions was set to match the number of data nodes.")));
				}
			}

			/* Update dimension partitions. First remove the detach/deleted
			 * data node from the list of remaining nodes so that it is not
			 * used in the new partitioning scheme.
			 *
			 * Note that the cached dimension partition info in the Dimension
			 * object is not updated. The cache will be invalidated and
			 * released at the end of this function.
			 */
			update_dimension_partitions = NULL != space_dim;

			if (op_type == OP_DETACH && drop_remote_data)
			{
				/* Drop the hypertable on the data node */
				ts_dist_cmd_run_on_data_nodes(
					psprintf("DROP TABLE IF EXISTS %s",
							 quote_qualified_identifier(NameStr(ht->fd.schema_name),
														NameStr(ht->fd.table_name))),
					list_make1(NameStr(node->fd.node_name)),
					true);
			}
		}
		else
		{
			/*  set block new chunks */
			if (block_chunks)
			{
				if (node->fd.block_chunks)
				{
					elog(NOTICE,
						 "new chunks already blocked on data node \"%s\" for"
						 " hypertable \"%s\"",
						 NameStr(node->fd.node_name),
						 get_rel_name(relid));
					continue;
				}

				check_replication_for_new_data(node_name, ht, force);
			}
			node->fd.block_chunks = block_chunks;
			removed += ts_hypertable_data_node_update(node);
			update_dimension_partitions = NULL != space_dim;
		}

		if (update_dimension_partitions)
		{
			/* Refresh the cached hypertable to get the updated list of data nodes */
			ts_cache_release(hcache);
			hcache = ts_hypertable_cache_pin();
			ht = ts_hypertable_cache_get_entry(hcache, relid, CACHE_FLAG_NONE);
			ts_hypertable_update_dimension_partitions(ht);
		}
	}

	ts_cache_release(hcache);
	return removed;
}

static int
data_node_block_hypertable_data_nodes(const char *node_name, List *hypertable_data_nodes,
									  bool all_hypertables, bool block_chunks, bool force)
{
	return data_node_modify_hypertable_data_nodes(node_name,
												  hypertable_data_nodes,
												  all_hypertables,
												  OP_BLOCK,
												  block_chunks,
												  force,
												  false,
												  false);
}

static int
data_node_detach_hypertable_data_nodes(const char *node_name, List *hypertable_data_nodes,
									   bool all_hypertables, bool force, bool repartition,
									   bool drop_remote_data, OperationType op_type)
{
	return data_node_modify_hypertable_data_nodes(node_name,
												  hypertable_data_nodes,
												  all_hypertables,
												  op_type,
												  false,
												  force,
												  repartition,
												  drop_remote_data);
}

HypertableDataNode *
data_node_hypertable_get_by_node_name(const Hypertable *ht, const char *node_name,
									  bool attach_check)
{
	HypertableDataNode *hdn = NULL;
	ListCell *lc;

	if (!hypertable_is_distributed(ht))
		ereport(ERROR,
				(errcode(ERRCODE_TS_HYPERTABLE_NOT_DISTRIBUTED),
				 errmsg("hypertable \"%s\" is not distributed",
						get_rel_name(ht->main_table_relid))));

	foreach (lc, ht->data_nodes)
	{
		hdn = lfirst(lc);
		if (namestrcmp(&hdn->fd.node_name, node_name) == 0)
			break;
		else
			hdn = NULL;
	}

	if (hdn == NULL)
	{
		if (attach_check)
			ereport(ERROR,
					(errcode(ERRCODE_TS_DATA_NODE_NOT_ATTACHED),
					 errmsg("data node \"%s\" is not attached to hypertable \"%s\"",
							node_name,
							get_rel_name(ht->main_table_relid))));
		else
			ereport(NOTICE,
					(errcode(ERRCODE_TS_DATA_NODE_NOT_ATTACHED),
					 errmsg("data node \"%s\" is not attached to hypertable \"%s\", "
							"skipping",
							node_name,
							get_rel_name(ht->main_table_relid))));
	}

	return hdn;
}

static HypertableDataNode *
get_hypertable_data_node(Oid table_id, const char *node_name, bool owner_check, bool attach_check)
{
	HypertableDataNode *hdn = NULL;
	Cache *hcache = ts_hypertable_cache_pin();
	const Hypertable *ht = ts_hypertable_cache_get_entry(hcache, table_id, CACHE_FLAG_NONE);

	if (owner_check)
		ts_hypertable_permissions_check(table_id, GetUserId());

	hdn = data_node_hypertable_get_by_node_name(ht, node_name, attach_check);

	ts_cache_release(hcache);

	return hdn;
}

static Datum
data_node_block_or_allow_new_chunks(const char *node_name, Oid const table_id, bool force,
									bool block_chunks)
{
	int affected = 0;
	bool all_hypertables = !OidIsValid(table_id);
	List *hypertable_data_nodes = NIL;
	ForeignServer *server = data_node_get_foreign_server(node_name, ACL_USAGE, true, false);

	Assert(NULL != server);

	if (OidIsValid(table_id))
	{
		/* Early abort on missing hypertable permissions */
		ts_hypertable_permissions_check(table_id, GetUserId());
		hypertable_data_nodes =
			list_make1(get_hypertable_data_node(table_id, server->servername, true, true));
	}
	else
	{
		/* block or allow for all hypertables */
		hypertable_data_nodes =
			ts_hypertable_data_node_scan_by_node_name(server->servername, CurrentMemoryContext);
	}

	affected = data_node_block_hypertable_data_nodes(server->servername,
													 hypertable_data_nodes,
													 all_hypertables,
													 block_chunks,
													 force);
	return Int32GetDatum(affected);
}

Datum
data_node_allow_new_chunks(PG_FUNCTION_ARGS)
{
	const char *node_name = PG_ARGISNULL(0) ? NULL : NameStr(*PG_GETARG_NAME(0));
	Oid table_id = PG_ARGISNULL(1) ? InvalidOid : PG_GETARG_OID(1);

	TS_PREVENT_FUNC_IF_READ_ONLY();

	return data_node_block_or_allow_new_chunks(node_name, table_id, false, false);
}

Datum
data_node_block_new_chunks(PG_FUNCTION_ARGS)
{
	const char *node_name = PG_ARGISNULL(0) ? NULL : NameStr(*PG_GETARG_NAME(0));
	Oid table_id = PG_ARGISNULL(1) ? InvalidOid : PG_GETARG_OID(1);
	bool force = PG_ARGISNULL(2) ? false : PG_GETARG_BOOL(2);

	TS_PREVENT_FUNC_IF_READ_ONLY();

	return data_node_block_or_allow_new_chunks(node_name, table_id, force, true);
}

Datum
data_node_detach(PG_FUNCTION_ARGS)
{
	const char *node_name = PG_ARGISNULL(0) ? NULL : NameStr(*PG_GETARG_NAME(0));
	Oid table_id = PG_ARGISNULL(1) ? InvalidOid : PG_GETARG_OID(1);
	bool all_hypertables = PG_ARGISNULL(1);
	bool if_attached = PG_ARGISNULL(2) ? false : PG_GETARG_BOOL(2);
	bool force = PG_ARGISNULL(3) ? InvalidOid : PG_GETARG_OID(3);
	bool repartition = PG_ARGISNULL(4) ? false : PG_GETARG_BOOL(4);
	bool drop_remote_data = PG_ARGISNULL(5) ? false : PG_GETARG_BOOL(5);
	int removed = 0;
	List *hypertable_data_nodes = NIL;
	ForeignServer *server;

	TS_PREVENT_FUNC_IF_READ_ONLY();

	server = data_node_get_foreign_server(node_name, ACL_USAGE, true, false);
	Assert(NULL != server);

	if (OidIsValid(table_id))
	{
		HypertableDataNode *node;

		/* Early abort on missing hypertable permissions */
		ts_hypertable_permissions_check(table_id, GetUserId());

		node = get_hypertable_data_node(table_id, server->servername, true, !if_attached);
		if (node)
			hypertable_data_nodes = list_make1(node);
	}
	else
	{
		/* Detach data node for all hypertables where user has
		 * permissions. Permissions checks done in
		 * data_node_detach_hypertable_data_nodes().  */
		hypertable_data_nodes =
			ts_hypertable_data_node_scan_by_node_name(server->servername, CurrentMemoryContext);
	}

	removed = data_node_detach_hypertable_data_nodes(server->servername,
													 hypertable_data_nodes,
													 all_hypertables,
													 force,
													 repartition,
													 drop_remote_data,
													 OP_DETACH);

	PG_RETURN_INT32(removed);
}

enum Anum_show_conn
{
	Anum_alter_data_node_node_name = 1,
	Anum_alter_data_node_host,
	Anum_alter_data_node_port,
	Anum_alter_data_node_database,
	Anum_alter_data_node_available,
	_Anum_alter_data_node_max,
};

#define Natts_alter_data_node (_Anum_alter_data_node_max - 1)

static HeapTuple
create_alter_data_node_tuple(TupleDesc tupdesc, const char *node_name, List *options)
{
	Datum values[Natts_alter_data_node];
	bool nulls[Natts_alter_data_node] = { false };
	ListCell *lc;

	MemSet(nulls, false, sizeof(nulls));

	values[AttrNumberGetAttrOffset(Anum_alter_data_node_node_name)] = CStringGetDatum(node_name);

	foreach (lc, options)
	{
		DefElem *elem = lfirst(lc);

		if (strcmp("host", elem->defname) == 0)
		{
			values[AttrNumberGetAttrOffset(Anum_alter_data_node_host)] =
				CStringGetTextDatum(defGetString(elem));
		}
		else if (strcmp("port", elem->defname) == 0)
		{
			int port = atoi(defGetString(elem));
			values[AttrNumberGetAttrOffset(Anum_alter_data_node_port)] = Int32GetDatum(port);
		}
		else if (strcmp("dbname", elem->defname) == 0)
		{
			values[AttrNumberGetAttrOffset(Anum_alter_data_node_database)] =
				CStringGetDatum(defGetString(elem));
		}
		else if (strcmp("available", elem->defname) == 0)
		{
			values[AttrNumberGetAttrOffset(Anum_alter_data_node_available)] =
				BoolGetDatum(defGetBoolean(elem));
		}
	}

	return heap_form_tuple(tupdesc, values, nulls);
}

/*
 * Switch data node to use for queries on chunks.
 *
 * When available=false it will switch from the given data node to another
 * one, but only if the data node is currently used for queries on the chunk.
 *
 * When available=true it will switch to the given data node, if it is
 * "primary" for the chunk (according to the current partitioning
 * configuration).
 */
static void
switch_data_node_on_chunks(const ForeignServer *datanode, bool available)
{
	unsigned int failed_update_count = 0;
	ScanIterator it = ts_chunk_data_nodes_scan_iterator_create(CurrentMemoryContext);
	ts_chunk_data_nodes_scan_iterator_set_node_name(&it, datanode->servername);

	/* Scan for chunks that reference the given data node */
	ts_scanner_foreach(&it)
	{
		TupleTableSlot *slot = ts_scan_iterator_slot(&it);
		bool PG_USED_FOR_ASSERTS_ONLY isnull = false;
		Datum chunk_id = slot_getattr(slot, Anum_chunk_data_node_chunk_id, &isnull);

		Assert(!isnull);

		const Chunk *chunk = ts_chunk_get_by_id(DatumGetInt32(chunk_id), true);
		if (!chunk_update_foreign_server_if_needed(chunk, datanode->serverid, available))
			failed_update_count++;
	}

	if (!available && failed_update_count > 0)
		elog(WARNING, "could not switch data node on %u chunks", failed_update_count);

	ts_scan_iterator_close(&it);
}

/*
 * Append new data node options.
 *
 * When setting options via AlterForeignServer(), the defelem list must
 * account for whether the an option already exists (is set) in the current
 * options or it is newly added. These are different operations on a foreign
 * server.
 *
 * Any options that already exist are purged from the current_options list so
 * that only the options not set or added remains. This list can be merged
 * with the new options to produce the full list of options (new and old).
 */
static List *
append_data_node_option(List *new_options, List **current_options, const char *name, Node *value)
{
	DefElem *elem;
	ListCell *lc;
	bool option_found = false;
#if PG13_LT
	ListCell *prev_lc = NULL;
#endif

	foreach (lc, *current_options)
	{
		elem = lfirst(lc);

		if (strcmp(elem->defname, name) == 0)
		{
			option_found = true;
			/* Remove the option which is replaced so that the remaining
			 * options can be merged later into an updated list */
#if PG13_GE
			*current_options = list_delete_cell(*current_options, lc);
#else
			*current_options = list_delete_cell(*current_options, lc, prev_lc);
#endif
			break;
		}
#if PG13_LT
		prev_lc = lc;
#endif
	}

	elem = makeDefElemExtended(NULL,
							   pstrdup(name),
							   value,
							   option_found ? DEFELEM_SET : DEFELEM_ADD,
							   -1);
	return lappend(new_options, elem);
}

/*
 * Alter a data node.
 *
 * Change the configuration of a data node, including host, port, and
 * database.
 *
 * Can also be used to mark a data node "unavailable", which ensures it is no
 * longer used for reads as long as there are replica chunks on other data
 * nodes to use for reads instead. If it is not possible to fail over all
 * chunks, a warning will be raised.
 */
Datum
data_node_alter(PG_FUNCTION_ARGS)
{
	const char *node_name = PG_ARGISNULL(0) ? NULL : NameStr(*PG_GETARG_NAME(0));
	const char *host = PG_ARGISNULL(1) ? NULL : TextDatumGetCString(PG_GETARG_TEXT_P(1));
	const char *database = PG_ARGISNULL(2) ? NULL : NameStr(*PG_GETARG_NAME(2));
	int port = PG_ARGISNULL(3) ? -1 : PG_GETARG_INT32(3);
	bool available_is_null = PG_ARGISNULL(4);
	bool available = available_is_null ? true : PG_GETARG_BOOL(4);
	ForeignServer *server = NULL;
	List *current_options = NIL;
	List *options = NIL;
	TupleDesc tupdesc;
	AlterForeignServerStmt alter_server_stmt = {
		.type = T_AlterForeignServerStmt,
		.servername = node_name ? pstrdup(node_name) : NULL,
		.has_version = false,
		.version = NULL,
		.options = NIL,
	};

	TS_PREVENT_FUNC_IF_READ_ONLY();

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("function returning record called in context "
						"that cannot accept type record")));

	tupdesc = BlessTupleDesc(tupdesc);

	/* Check if a data node with the given name actually exists, or raise an error. */
	server = data_node_get_foreign_server(node_name, ACL_NO_CHECK, false, false /* missing_ok */);

	if (host == NULL && database == NULL && port == -1 && available_is_null)
		PG_RETURN_DATUM(
			HeapTupleGetDatum(create_alter_data_node_tuple(tupdesc, node_name, server->options)));

	current_options = list_copy(server->options);

	if (host != NULL)
		options = append_data_node_option(options,
										  &current_options,
										  "host",
										  (Node *) makeString((char *) host));

	if (database != NULL)
		options = append_data_node_option(options,
										  &current_options,
										  "dbname",
										  (Node *) makeString((char *) database));

	if (port != -1)
	{
		validate_data_node_port(port);
		options =
			append_data_node_option(options, &current_options, "port", (Node *) makeInteger(port));
	}

	if (!available_is_null)
		options = append_data_node_option(options,
										  &current_options,
										  "available",
										  (Node *) makeString(available ? "true" : "false"));

	alter_server_stmt.options = options;
	AlterForeignServer(&alter_server_stmt);

	/* Drop stale chunks on the unavailable data node, if we are going to
	 * make it available again */
	if (!available_is_null && available && !ts_data_node_is_available_by_server(server))
		ts_chunk_drop_stale_chunks(node_name, NULL);

	/* Make changes to the data node (foreign server object) visible so that
	 * the changes are present when we switch "primary" data node on chunks */
	CommandCounterIncrement();

	/* Update the currently used query data node on all affected chunks to
	 * reflect the new status of the data node */
	switch_data_node_on_chunks(server, available);

	/* Add updated options last as they will take precedence over old options
	 * when creating the result tuple. */
	options = list_concat(current_options, options);

	PG_RETURN_DATUM(HeapTupleGetDatum(create_alter_data_node_tuple(tupdesc, node_name, options)));
}

/*
 * Drop a data node's database.
 *
 * To drop the database on the data node, a connection must be made to another
 * database since one cannot drop the database currently connected
 * to. Therefore, we bypass the connection cache and use a "raw" connection to
 * a standard database (e.g., template0 or postgres), similar to how
 * bootstrapping does it.
 *
 * Note that no password is provided on the command line as is done when
 * bootstrapping. Instead, it is assumed that the current user already has a
 * method to authenticate with the remote data node (e.g., via a password
 * file, certificate, or user mapping). This should normally be the case or
 * otherwise the user wouldn't have been able to use the data node.
 *
 * Note that the user that deletes a data node also must be the database owner
 * on the data node. The database will only be dropped if there are no other
 * concurrent connections so all connections must be closed before being able
 * to drop the database.
 */
static void
drop_data_node_database(const ForeignServer *server)
{
	ListCell *lc;
	TSConnection *conn = NULL;
	Oid userid = GetUserId();
	TSConnectionId connid = {
		.server_id = server->serverid,
		.user_id = userid,
	};
	/* Make a copy of the node name since the server pointer will be
	 * updated */
	char *nodename = pstrdup(server->servername);
	char *dbname = NULL;
	char *err = NULL;

	/* Figure out the name of the database that should be dropped */
	foreach (lc, server->options)
	{
		DefElem *d = lfirst(lc);

		if (strcmp(d->defname, "dbname") == 0)
		{
			dbname = defGetString(d);
			break;
		}
	}

	if (NULL == dbname)
	{
		/* This should not happen unless the configuration is messed up */
		ereport(ERROR,
				(errcode(ERRCODE_TS_DATA_NODE_INVALID_CONFIG),
				 errmsg("could not drop the database on data node \"%s\"", nodename),
				 errdetail("The data node configuration lacks the \"dbname\" option.")));
		pg_unreachable();
		return;
	}

	/* Clear potentially cached connection to the data node for the current
	 * session so that it won't block dropping the database */
	remote_connection_cache_remove(connid);

	/* Cannot connect to the database that is being dropped, so try to connect
	 * to a "standard" bootstrap database that we expect to exist on the data
	 * node */
	for (size_t i = 0; i < lengthof(bootstrap_databases); i++)
	{
		List *conn_options;
		DefElem dbname_elem = {
			.type = T_DefElem,
			.defaction = DEFELEM_SET,
			.defname = "dbname",
			.arg = (Node *) makeString(pstrdup(bootstrap_databases[i])),
		};
		AlterForeignServerStmt stmt = {
			.type = T_AlterForeignServerStmt,
			.servername = nodename,
			.has_version = false,
			.options = list_make1(&dbname_elem),
		};

		/*
		 * We assume that the user already has credentials configured to
		 * connect to the data node, e.g., via a user mapping, password file,
		 * or certificate. But in order to easily make use of those
		 * authentication methods, we need to establish the connection using
		 * the standard connection functions to pick up the foreign server
		 * options and associated user mapping (if such a mapping
		 * exists). However, the foreign server configuration references the
		 * database we are trying to drop, so we first need to update the
		 * foreign server definition to use the bootstrap database. */
		AlterForeignServer(&stmt);

		/* Make changes to foreign server database visible */
		CommandCounterIncrement();

		/* Get the updated server definition */
		server = data_node_get_foreign_server(nodename, ACL_USAGE, true, false);
		/* Open a connection to the bootstrap database using the new server options */
		conn_options = remote_connection_prepare_auth_options(server, userid);
		conn = remote_connection_open_with_options_nothrow(nodename, conn_options, &err);

		if (NULL != conn)
			break;
	}

	if (NULL != conn)
	{
		/* Do not include FORCE or IF EXISTS options when dropping the
		 * database. Instead, we expect the database to exist, or the user
		 * has to rerun the command without drop_database=>true set. We
		 * don't force removal if there are other connections to the
		 * database out of caution. If the user wants to forcefully remove
		 * the database, they can do it manually. From PG15, the backend
		 * executing the DROP forces all other backends to close all smgr
		 * fds using the ProcSignalBarrier mechanism. To allow this backend
		 * to handle that interrupt, send the DROP request using the async
		 * API. */
		char *cmd;
		AsyncRequest *req;

		cmd = psprintf("DROP DATABASE %s", quote_identifier(dbname));
		req = async_request_send(conn, cmd);
		Assert(NULL != req);
		async_request_wait_ok_result(req);
		remote_connection_close(conn);
		pfree(req);
		pfree(cmd);
	}
	else
		ereport(ERROR,
				(errcode(ERRCODE_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION),
				 errmsg("could not connect to data node \"%s\"", nodename),
				 err == NULL ? 0 : errdetail("%s", err)));
}

Datum
data_node_delete(PG_FUNCTION_ARGS)
{
	const char *node_name = PG_ARGISNULL(0) ? NULL : PG_GETARG_CSTRING(0);
	bool if_exists = PG_ARGISNULL(1) ? false : PG_GETARG_BOOL(1);
	bool force = PG_ARGISNULL(2) ? false : PG_GETARG_BOOL(2);
	bool repartition = PG_ARGISNULL(3) ? false : PG_GETARG_BOOL(3);
	bool drop_database = PG_ARGISNULL(4) ? false : PG_GETARG_BOOL(4);
	List *hypertable_data_nodes = NIL;
	DropStmt stmt;
	ObjectAddress address;
	ObjectAddress secondary_object = {
		.classId = InvalidOid,
		.objectId = InvalidOid,
		.objectSubId = 0,
	};
	Node *parsetree = NULL;
	TSConnectionId cid;
	ForeignServer *server;

	TS_PREVENT_FUNC_IF_READ_ONLY();

	/* Need USAGE to detach. Further owner check done when executing the DROP
	 * statement. */
	server = data_node_get_foreign_server(node_name, ACL_USAGE, true, if_exists);

	Assert(server == NULL ? if_exists : true);

	if (NULL == server)
	{
		elog(NOTICE, "data node \"%s\" does not exist, skipping", node_name);
		PG_RETURN_BOOL(false);
	}

	if (drop_database)
	{
		TS_PREVENT_IN_TRANSACTION_BLOCK(true);
		drop_data_node_database(server);
	}

	/* close any pending connections */
	remote_connection_id_set(&cid, server->serverid, GetUserId());
	remote_connection_cache_remove(cid);

	/* detach data node */
	hypertable_data_nodes =
		ts_hypertable_data_node_scan_by_node_name(node_name, CurrentMemoryContext);

	data_node_detach_hypertable_data_nodes(node_name,
										   hypertable_data_nodes,
										   true,
										   force,
										   repartition,
										   false,
										   OP_DELETE);

	/* clean up persistent transaction records */
	remote_txn_persistent_record_delete_for_data_node(server->serverid, NULL);

	stmt = (DropStmt){
		.type = T_DropStmt,
		.objects = list_make1(makeString(pstrdup(node_name))),
		.removeType = OBJECT_FOREIGN_SERVER,
		.behavior = DROP_RESTRICT,
		.missing_ok = if_exists,
	};

	parsetree = (Node *) &stmt;

	/* Make sure event triggers are invoked so that all dropped objects
	 * are collected during a cascading drop. This ensures all dependent
	 * objects get cleaned up. */
	EventTriggerBeginCompleteQuery();

	PG_TRY();
	{
		ObjectAddressSet(address, ForeignServerRelationId, server->serverid);
		EventTriggerDDLCommandStart(parsetree);
		RemoveObjects(&stmt);
		EventTriggerCollectSimpleCommand(address, secondary_object, parsetree);
		EventTriggerSQLDrop(parsetree);
		EventTriggerDDLCommandEnd(parsetree);
	}
	PG_CATCH();
	{
		EventTriggerEndCompleteQuery();
		PG_RE_THROW();
	}
	PG_END_TRY();

	/* Remove self from dist db if no longer have data_nodes */
	if (data_node_get_node_name_list() == NIL)
		dist_util_remove_from_db();

	EventTriggerEndCompleteQuery();
	CommandCounterIncrement();
	CacheInvalidateRelcacheByRelid(ForeignServerRelationId);

	PG_RETURN_BOOL(true);
}

/*
 * Get server list, performing an ACL check on each of them in the process.
 */
List *
data_node_get_node_name_list_with_aclcheck(AclMode mode, bool fail_on_aclcheck)
{
	HeapTuple tuple;
	ScanKeyData scankey[1];
	SysScanDesc scandesc;
	Relation rel;
	ForeignDataWrapper *fdw = GetForeignDataWrapperByName(EXTENSION_FDW_NAME, false);
	List *nodes = NIL;

	rel = table_open(ForeignServerRelationId, AccessShareLock);

	ScanKeyInit(&scankey[0],
				Anum_pg_foreign_server_srvfdw,
				BTEqualStrategyNumber,
				F_OIDEQ,
				ObjectIdGetDatum(fdw->fdwid));

	scandesc = systable_beginscan(rel, InvalidOid, false, NULL, 1, scankey);

	while (HeapTupleIsValid(tuple = systable_getnext(scandesc)))
	{
		Form_pg_foreign_server form = (Form_pg_foreign_server) GETSTRUCT(tuple);
		ForeignServer *server;

		server =
			data_node_get_foreign_server(NameStr(form->srvname), mode, fail_on_aclcheck, false);

		if (server != NULL)
			nodes = lappend(nodes, pstrdup(NameStr(form->srvname)));
	}

	systable_endscan(scandesc);
	table_close(rel, AccessShareLock);

	return nodes;
}

void
data_node_fail_if_nodes_are_unavailable(void)
{
	/* Get a list of data nodes and ensure all of them are available */
	List *data_node_list = data_node_get_node_name_list_with_aclcheck(ACL_NO_CHECK, false);
	ListCell *lc;

	foreach (lc, data_node_list)
	{
		const char *node_name = lfirst(lc);
		const ForeignServer *server;

		server = data_node_get_foreign_server(node_name, ACL_NO_CHECK, false, false);
		if (!ts_data_node_is_available_by_server(server))
			ereport(ERROR, (errmsg("some data nodes are not available")));
	}
}

/*
 * Get server list with optional ACL check.
 *
 * Returns:
 *
 * If nodearr is NULL, returns all system-configured data nodes that fulfill
 * the ACL check.
 *
 * If nodearr is non-NULL, returns all the data nodes in the specified array
 * subject to ACL checks.
 */
List *
data_node_get_filtered_node_name_list(ArrayType *nodearr, AclMode mode, bool fail_on_aclcheck)
{
	ArrayIterator it;
	Datum node_datum;
	bool isnull;
	List *nodes = NIL;

	if (NULL == nodearr)
		return data_node_get_node_name_list_with_aclcheck(mode, fail_on_aclcheck);

	it = array_create_iterator(nodearr, 0, NULL);

	while (array_iterate(it, &node_datum, &isnull))
	{
		if (!isnull)
		{
			const char *node_name = DatumGetCString(node_datum);
			ForeignServer *server =
				data_node_get_foreign_server(node_name, mode, fail_on_aclcheck, false);

			if (NULL != server)
				nodes = lappend(nodes, server->servername);
		}
	}

	array_free_iterator(it);

	return nodes;
}

List *
data_node_get_node_name_list(void)
{
	return data_node_get_node_name_list_with_aclcheck(ACL_NO_CHECK, false);
}

/*
 * Turn an array of data nodes into a list of names.
 *
 * The function will verify that all the servers in the list belong to the
 * TimescaleDB foreign data wrapper. Optionally, perform ACL check on each
 * data node's foreign server. Checks are skipped when specificing
 * ACL_NO_CHECK. If fail_on_aclcheck is false, then no errors will be thrown
 * on ACL check failures. Instead, data nodes that fail ACL checks will simply
 * be filtered.
 */
List *
data_node_array_to_node_name_list_with_aclcheck(ArrayType *nodearr, AclMode mode,
												bool fail_on_aclcheck)
{
	if (NULL == nodearr)
		return NIL;

	Assert(ARR_NDIM(nodearr) <= 1);

	return data_node_get_filtered_node_name_list(nodearr, mode, fail_on_aclcheck);
}

List *
data_node_array_to_node_name_list(ArrayType *nodearr)
{
	return data_node_array_to_node_name_list_with_aclcheck(nodearr, ACL_NO_CHECK, false);
}

Datum
data_node_ping(PG_FUNCTION_ARGS)
{
	const char *node_name = PG_ARGISNULL(0) ? NULL : PG_GETARG_CSTRING(0);
	/* Allow anyone to ping a data node. Otherwise the
	 * timescaledb_information.data_node view won't work for those users. */
	ForeignServer *server = data_node_get_foreign_server(node_name, ACL_NO_CHECK, false, false);
	bool success;

	Assert(NULL != server);

	success = remote_connection_ping(server->servername);

	PG_RETURN_DATUM(BoolGetDatum(success));
}

List *
data_node_oids_to_node_name_list(List *data_node_oids, AclMode mode)
{
	List *node_names = NIL;
	ListCell *lc;
	ForeignServer *fs;

	foreach (lc, data_node_oids)
	{
		Oid foreign_server_oid = lfirst_oid(lc);
		fs = data_node_get_foreign_server_by_oid(foreign_server_oid, mode);
		node_names = lappend(node_names, pstrdup(fs->servername));
	}

	return node_names;
}

void
data_node_name_list_check_acl(List *data_node_names, AclMode mode)
{
	AclResult aclresult;
	Oid curuserid;
	ListCell *lc;

	if (data_node_names == NIL)
		return;

	curuserid = GetUserId();

	foreach (lc, data_node_names)
	{
		/* Validate the servers, but privilege check is optional */
		ForeignServer *server = GetForeignServerByName(lfirst(lc), false);

		if (mode != ACL_NO_CHECK)
		{
			/* Must have permissions on the server object */
			aclresult = pg_foreign_server_aclcheck(server->serverid, curuserid, mode);

			if (aclresult != ACLCHECK_OK)
				aclcheck_error(aclresult, OBJECT_FOREIGN_SERVER, server->servername);
		}
	}
}
