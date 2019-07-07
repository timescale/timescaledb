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
#include <utils/syscache.h>
#include <utils/acl.h>
#include <utils/inval.h>
#include <libpq/crypt.h>
#include <miscadmin.h>
#include <funcapi.h>

#include <hypertable_data_node.h>
#include <extension.h>
#include <compat.h>
#include <catalog.h>
#include <chunk_data_node.h>

#include "fdw/fdw.h"
#include "remote/async.h"
#include "remote/connection.h"
#include "remote/connection_cache.h"
#include "data_node.h"
#include "remote/utils.h"
#include "hypertable.h"
#include "hypertable_cache.h"
#include "errors.h"
#include "dist_util.h"
#include "utils/uuid.h"
#include "chunk.h"

#define TS_DEFAULT_POSTGRES_PORT 5432
#define TS_DEFAULT_POSTGRES_HOST "localhost"

/*
 * Lookup the foreign server by name and verify that it is a TimescaleDB
 * server. Optionally, do ACL checks.
 */
ForeignServer *
data_node_get_foreign_server(const char *node_name, AclMode mode, bool missing_ok)
{
	ForeignServer *server;
	ForeignDataWrapper *fdw;

	if (node_name == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid node_name: cannot be NULL")));

	fdw = GetForeignDataWrapperByName(TIMESCALEDB_FDW_NAME, false);
	server = GetForeignServerByName(node_name, missing_ok);

	Assert(NULL != fdw);

	if (NULL == server)
		return NULL;

	if (server->fdwid != fdw->fdwid)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("data node \"%s\" is not a TimescaleDB server", node_name)));

	if (mode != ACL_NO_CHECK)
	{
		AclResult aclresult;
		Oid curuserid = GetUserId();

		/* Must have permissions on the server object */
		aclresult = pg_foreign_server_aclcheck(server->serverid, curuserid, mode);

		if (aclresult != ACLCHECK_OK)
			aclcheck_error(aclresult, OBJECT_FOREIGN_SERVER, node_name);
	}

	return server;
}

/*
 * Create a user mapping.
 *
 * Returns the OID of the created user mapping.
 *
 * Non-superusers must provide a password.
 */
static Oid
create_user_mapping(const char *username, const char *node_name, const char *password,
					bool if_not_exists)
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
		.user = &rolespec,
		.if_not_exists = if_not_exists,
		.servername = (char *) node_name,
		.options = NIL,
	};

	Assert(NULL != username && NULL != node_name);

	stmt.options =
		list_make1(makeDefElemCompat("user", (Node *) makeString(pstrdup(username)), -1));

	/* Non-superusers must provide a password */
	if (!superuser() && (NULL == password || password[0] == '\0'))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_PARAMETER),
				 errmsg("no password specified for user \"%s\"", username),
				 errhint("Specify a password to use when connecting to node \"%s\"", node_name)));

	if (NULL != password)
		stmt.options =
			lappend(stmt.options,
					makeDefElemCompat("password", (Node *) makeString(pstrdup(password)), -1));

	/* Permissions checks done in CreateUserMapping() */
	objaddr = CreateUserMapping(&stmt);

	return objaddr.objectId;
}

/*
 * Create a foreign server.
 *
 * Returns the OID of the created foreign server.
 */
static Oid
create_foreign_server(const char *node_name, const char *host, int32 port, const char *dbname,
					  bool if_not_exists, bool *created)
{
	ForeignServer *server;
	ObjectAddress objaddr;
	CreateForeignServerStmt stmt = {
		.type = T_CreateForeignServerStmt,
		.servername = (char *) node_name,
		.fdwname = TIMESCALEDB_FDW_NAME,
		.options =
			list_make3(makeDefElemCompat("host", (Node *) makeString(pstrdup(host)), -1),
					   makeDefElemCompat("port", (Node *) makeInteger(port), -1),
					   makeDefElemCompat("dbname", (Node *) makeString(pstrdup(dbname)), -1)),
		.if_not_exists = if_not_exists,
	};

	if (NULL == host)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 (errmsg("invalid host"),
				  (errhint("A hostname or IP address must be specified when "
						   "a data node does not already exist.")))));

	if (NULL != created)
		*created = false;

	if (if_not_exists)
	{
		server = data_node_get_foreign_server(node_name, ACL_USAGE, true);

		if (NULL != server)
		{
			ereport(NOTICE,
					(errcode(ERRCODE_DUPLICATE_OBJECT),
					 errmsg("data node \"%s\" already exists, skipping", node_name)));

			return server->serverid;
		}
	}

	/* Permissions checks done in CreateForeignServer() */
	objaddr = CreateForeignServer(&stmt);

	/* CreateForeignServer returns InvalidOid if server already exists */
	if (!OidIsValid(objaddr.objectId))
	{
		Assert(if_not_exists);

		server = data_node_get_foreign_server(node_name, ACL_USAGE, false);

		return server->serverid;
	}

	if (NULL != created)
		*created = true;

	return objaddr.objectId;
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
	List *node_options;
	DefElem *host_elm = makeDefElemCompat("host", (Node *) makeString(pstrdup(host)), -1);
	DefElem *port_elm = makeDefElemCompat("port", (Node *) makeInteger(port), -1);
	DefElem *dbname_elm = makeDefElemCompat("dbname", (Node *) makeString(pstrdup(dbname)), -1);
	DefElem *user_elm = makeDefElemCompat("user", (Node *) makeString(pstrdup(user)), -1);
	DefElem *password_elm;

	node_options = list_make4(host_elm, port_elm, dbname_elm, user_elm);
	if (password)
	{
		password_elm = makeDefElemCompat("password", (Node *) makeString(pstrdup(password)), -1);
		lappend(node_options, password_elm);
	}
	return node_options;
}

static bool
data_node_bootstrap_database(const char *node_name, const char *host, int32 port,
							 const char *dbname, const char *username, bool if_not_exists,
							 const char *bootstrap_database, const char *bootstrap_user,
							 const char *bootstrap_password)
{
	TSConnection *conn;
	List *node_options;
	bool created = false;

	Assert(NULL != node_name);
	Assert(NULL != dbname);
	Assert(NULL != username);
	Assert(NULL != host);
	Assert(NULL != bootstrap_database);
	Assert(NULL != bootstrap_user);

	node_options = create_data_node_options(host,
											port,
											bootstrap_database,
											bootstrap_user,
											bootstrap_password);

	conn = remote_connection_open(node_name, node_options, NULL, false);

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
						 errmsg("database \"%s\" already exists on the remote node", dbname),
						 errhint("Set if_not_exists => TRUE to add the node to an existing "
								 "database.")));
			else
				elog(NOTICE, "database \"%s\" already exists on data node, skipping", dbname);
		}
		else
		{
			/* Create the database with the user as owner */
			request = psprintf("CREATE DATABASE %s OWNER %s",
							   quote_identifier(dbname),
							   quote_identifier(username));
			res = remote_connection_query_ok_result(conn, request);
			remote_connection_result_close(res);
			created = true;
		}
	}
	PG_CATCH();
	{
		remote_connection_close(conn);
		PG_RE_THROW();
	}
	PG_END_TRY();

	remote_connection_close(conn);

	return created;
}

static bool
data_node_bootstrap_extension(const char *node_name, const char *host, int32 port,
							  const char *dbname, const char *username, bool if_not_exists,
							  const char *bootstrap_user, const char *bootstrap_password)
{
	TSConnection *conn;
	List *node_options;
	bool created = false;

	node_options = create_data_node_options(host, port, dbname, bootstrap_user, bootstrap_password);
	conn = remote_connection_open(node_name, node_options, NULL, false);

	PG_TRY();
	{
		PGresult *res;
		char *request;
		const char *schema_name = ts_extension_schema_name();
		const char *schema_name_quoted = quote_identifier(schema_name);
		Oid schema_oid = get_namespace_oid(schema_name, true);
		bool extension_exists = false;

		request = psprintf("SELECT 1 FROM pg_extension WHERE extname = %s",
						   quote_literal_cstr(EXTENSION_NAME));
		res = remote_connection_query_any_result(conn, request);

		if (PQntuples(res) > 0)
			extension_exists = true;

		if (!extension_exists)
		{
			if (schema_oid != PG_PUBLIC_NAMESPACE)
			{
				request = psprintf("CREATE SCHEMA %s%s AUTHORIZATION %s",
								   if_not_exists ? "IF NOT EXISTS " : "",
								   schema_name_quoted,
								   quote_identifier(username));
				res = remote_connection_query_ok_result(conn, request);
				remote_connection_result_close(res);
			}
			request = psprintf("CREATE EXTENSION %s " EXTENSION_NAME " WITH SCHEMA %s CASCADE",
							   if_not_exists ? "IF NOT EXISTS" : "",
							   schema_name_quoted);
			res = remote_connection_query_ok_result(conn, request);
			remote_connection_result_close(res);

			created = true;
		}
	}
	PG_CATCH();
	{
		remote_connection_close(conn);
		PG_RE_THROW();
	}
	PG_END_TRY();

	remote_connection_close(conn);

	return created;
}

static void
data_node_bootstrap(const char *node_name, const char *host, int32 port, const char *dbname,
					const char *username, bool if_not_exists, const char *bootstrap_database,
					const char *bootstrap_user, const char *bootstrap_password,
					bool *database_created, bool *extension_created)
{
	bool created;

	created = data_node_bootstrap_database(node_name,
										   host,
										   port,
										   dbname,
										   username,
										   if_not_exists,
										   bootstrap_database,
										   bootstrap_user,
										   bootstrap_password);

	if (NULL != database_created)
		*database_created = created;

	/* Always use "if_not_exists" when the database was created since the
	 * extension could have been pre-installed in the template database and
	 * thus created with the database. */
	if (created)
		if_not_exists = true;

	created = data_node_bootstrap_extension(node_name,
											host,
											port,
											dbname,
											username,
											if_not_exists,
											bootstrap_user,
											bootstrap_password);

	if (NULL != extension_created)
		*extension_created = created;
}

static void
add_distributed_id_to_data_node(const char *node_name, const char *host, int32 port,
								const char *dbname, bool if_not_exists, const char *user,
								const char *user_password)
{
	TSConnection *conn;
	List *node_options;

	node_options = create_data_node_options(host, port, dbname, user, user_password);
	conn = remote_connection_open(node_name, node_options, NULL, false);

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
remove_distributed_id_from_backend(ForeignServer *fs, UserMapping *um)
{
	TSConnection *conn;

	conn = remote_connection_open(fs->servername, fs->options, um ? um->options : NULL, true);

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

/* set_distid may need to be false for some otherwise invalid configurations that are useful for
 * testing */
static Datum
data_node_add_internal(PG_FUNCTION_ARGS, bool set_distid)
{
	Oid userid = GetUserId();
	const char *username = GetUserNameFromId(userid, false);
	const char *node_name = PG_ARGISNULL(0) ? NULL : PG_GETARG_CSTRING(0);
	const char *host =
		PG_ARGISNULL(1) ? TS_DEFAULT_POSTGRES_HOST : TextDatumGetCString(PG_GETARG_DATUM(1));
	const char *dbname = PG_ARGISNULL(2) ? get_database_name(MyDatabaseId) : PG_GETARG_CSTRING(2);
	int32 port = PG_ARGISNULL(3) ? TS_DEFAULT_POSTGRES_PORT : PG_GETARG_INT32(3);
	const char *password = PG_ARGISNULL(4) ? NULL : TextDatumGetCString(PG_GETARG_DATUM(4));
	bool if_not_exists = PG_ARGISNULL(5) ? false : PG_GETARG_BOOL(5);
	const char *bootstrap_database = PG_ARGISNULL(6) ? dbname : PG_GETARG_CSTRING(6);
	const char *bootstrap_user = PG_ARGISNULL(7) ? username : PG_GETARG_CSTRING(7);
	const char *bootstrap_password =
		PG_ARGISNULL(8) ? password : TextDatumGetCString(PG_GETARG_DATUM(8));
	UserMapping *um;
	Oid serverid = InvalidOid;
	bool server_created = false;
	bool database_created = false;
	bool extension_created = false;

	if (set_distid && dist_util_membership() == DIST_MEMBER_BACKEND)
		ereport(ERROR,
				(errcode(ERRCODE_TS_DATA_NODE_ASSIGNMENT_ALREADY_EXISTS),
				 (errmsg("unable to assign data nodes from an existing distributed database"))));

	if (NULL == bootstrap_database)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 (errmsg("invalid bootstrap database name"))));

	if (NULL == node_name)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE), (errmsg("invalid data node name"))));

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
	PreventInTransactionBlock(true, "add_data_node");

	/* Try to create the foreign server, or get the existing one in case of
	 * if_not_exists = true. */
	serverid = create_foreign_server(node_name, host, port, dbname, if_not_exists, &server_created);

	/* Make the foreign server visible in current transaction. */
	CommandCounterIncrement();

	um = get_user_mapping(userid, serverid, true);

	if (NULL == um)
	{
		elog(NOTICE, "adding user mapping for \"%s\" to connect to \"%s\"", username, node_name);

		create_user_mapping(username, node_name, password, if_not_exists);

		/* Make user mapping visible */
		CommandCounterIncrement();

		um = GetUserMapping(userid, serverid);
		Assert(NULL != um);
	}
	else if (!if_not_exists)
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_OBJECT),
				 errmsg("user mapping for user \"%s\" and data node \"%s\" already exists",
						username,
						node_name)));

	/* Try to create database and extension on remote server */
	data_node_bootstrap(node_name,
						host,
						port,
						dbname,
						username,
						if_not_exists,
						bootstrap_database,
						bootstrap_user,
						bootstrap_password,
						&database_created,
						&extension_created);

	if (set_distid)
	{
		if (dist_util_membership() != DIST_MEMBER_FRONTEND)
			dist_util_set_as_frontend();

		add_distributed_id_to_data_node(node_name,
										host,
										port,
										dbname,
										if_not_exists,
										bootstrap_user,
										bootstrap_password);
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
	Oid table_id = PG_GETARG_OID(0);
	const char *node_name = PG_ARGISNULL(1) ? NULL : PG_GETARG_NAME(1)->data;
	bool if_not_attached = PG_ARGISNULL(2) ? false : PG_GETARG_BOOL(2);
	ForeignServer *fserver = data_node_get_foreign_server(node_name, ACL_USAGE, false);
	Cache *hcache;
	Hypertable *ht;
	List *result;
	ListCell *lc;

	Assert(NULL != fserver);

	if (PG_ARGISNULL(0))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid hypertable: cannot be NULL")));

	hcache = ts_hypertable_cache_pin();
	ht = ts_hypertable_cache_get_entry(hcache, table_id);

	if (ht == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_TS_HYPERTABLE_NOT_EXIST),
				 errmsg("table \"%s\" is not a hypertable", get_rel_name(table_id))));

	/* Must have owner permissions on the hypertable to attach a new server.
	   Must also have USAGE on the foreign server.  */
	ts_hypertable_permissions_check(table_id, GetUserId());

	foreach (lc, ts_hypertable_data_node_scan(ht->fd.id, CurrentMemoryContext))
	{
		HypertableDataNode *node = lfirst(lc);

		if (node->foreign_server_oid == fserver->serverid)
		{
			ts_cache_release(hcache);
			if (if_not_attached)
			{
				ereport(NOTICE,
						(errcode(ERRCODE_TS_TABLESPACE_ALREADY_ATTACHED),
						 errmsg("data node \"%s\" is already attached to hypertable \"%s\", "
								"skipping",
								node_name,
								get_rel_name(table_id))));
				PG_RETURN_DATUM(create_hypertable_data_node_datum(fcinfo, node));
			}
			else
				ereport(ERROR,
						(errcode(ERRCODE_TS_TABLESPACE_ALREADY_ATTACHED),
						 errmsg("data node \"%s\" is already attached to hypertable \"%s\"",
								node_name,
								get_rel_name(table_id))));
		}
	}

	result = hypertable_assign_data_nodes(ht->fd.id, list_make1((char *) node_name));
	Assert(result->length == 1);
	ts_cache_release(hcache);
	PG_RETURN_DATUM(
		create_hypertable_data_node_datum(fcinfo, (HypertableDataNode *) linitial(result)));
}

/* Only used for generating proper error message */
typedef enum OperationType
{
	BLOCK,
	DETACH,
	DELETE
} OperationType;

static char *
get_operation_type_message(OperationType op_type)
{
	switch (op_type)
	{
		case BLOCK:
			return "blocking new chunks on";
		case DETACH:
			return "detaching";
		case DELETE:
			return "deleting";
		default:
			return NULL;
	}
}

static void
check_replication_for_new_data(const char *node_name, Hypertable *ht, bool force,
							   OperationType op_type)
{
	List *available_nodes = ts_hypertable_get_available_data_nodes(ht, false);
	char *operation = get_operation_type_message(op_type);

	if (ht->fd.replication_factor < list_length(available_nodes))
		return;

	if (!force)
		ereport(ERROR,
				(errcode(ERRCODE_TS_INTERNAL_ERROR),
				 errmsg("%s data node \"%s\" risks making new data for hypertable \"%s\" "
						"under-replicated",
						operation,
						node_name,
						NameStr(ht->fd.table_name)),
				 errhint("Call function with force => true to force this operation.")));

	ereport(WARNING,
			(errcode(ERRCODE_TS_INTERNAL_ERROR),
			 errmsg("new data for hypertable \"%s\" will be under-replicated due to %s data node "
					"\"%s\"",
					NameStr(ht->fd.table_name),
					operation,
					node_name)));
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
data_node_detach_validate(const char *node_name, Hypertable *ht, bool force, OperationType op_type)
{
	List *chunk_data_nodes =
		ts_chunk_data_node_scan_by_node_name_and_hypertable_id(node_name,
															   ht->fd.id,
															   CurrentMemoryContext);
	bool has_non_replicated_chunks = data_node_contains_non_replicated_chunks(chunk_data_nodes);
	char *operation = get_operation_type_message(op_type);

	if (has_non_replicated_chunks)
		ereport(ERROR,
				(errcode(ERRCODE_TS_INTERNAL_ERROR),
				 errmsg("%s data node \"%s\" would mean a data-loss for hypertable "
						"\"%s\" since data node has the only data replica",
						operation,
						node_name,
						NameStr(ht->fd.table_name)),
				 errhint("Ensure the data node \"%s\" has no non-replicated data before %s it.",
						 node_name,
						 operation)));

	if (list_length(chunk_data_nodes) > 0)
	{
		if (force)
			ereport(WARNING,
					(errcode(ERRCODE_WARNING),
					 errmsg("hypertable \"%s\" has under-replicated chunks due to %s "
							"data node \"%s\"",
							NameStr(ht->fd.table_name),
							operation,
							node_name)));
		else
			ereport(ERROR,
					(errcode(ERRCODE_TS_DATA_NODE_IN_USE),
					 errmsg("%s data node \"%s\" failed because it contains chunks "
							"for hypertable \"%s\"",
							operation,
							node_name,
							NameStr(ht->fd.table_name))));
	}

	check_replication_for_new_data(node_name, ht, force, op_type);

	return chunk_data_nodes;
}

static int
data_node_modify_hypertable_data_nodes(const char *node_name, List *hypertable_data_nodes,
									   bool all_hypertables, OperationType op_type,
									   bool block_chunks, bool force)
{
	Cache *hcache = ts_hypertable_cache_pin();
	ListCell *lc;
	int removed = 0;

	foreach (lc, hypertable_data_nodes)
	{
		HypertableDataNode *node = lfirst(lc);
		Oid relid = ts_hypertable_id_to_relid(node->fd.hypertable_id);
		Hypertable *ht = ts_hypertable_cache_get_entry_by_id(hcache, node->fd.hypertable_id);
		bool has_privs = ts_hypertable_has_privs_of(relid, GetUserId());

		Assert(ht != NULL);

		if (!has_privs)
		{
			/* If the operation is DELETE, we MUST be able to detach the data
			 * node from ALL tables since the foreign server object will be
			 * deleted. Therefore, we fail the operation if we find a table
			 * that we don't have owner permissions on in this case. */
			if (all_hypertables && op_type != DELETE)
				ereport(NOTICE,
						(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
						 errmsg("skipping hypertable \"%s\" due to missing permissions",
								get_rel_name(relid))));
			else
				ereport(ERROR,
						(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
						 errmsg("permission denied for hypertable \"%s\"", get_rel_name(relid)),
						 errdetail("The server is attached to hypertables that the current "
								   "user lacks permissions for.")));
		}
		else if (op_type == DETACH || op_type == DELETE)
		{
			/* we have permissions to detach */
			List *chunk_data_nodes =
				data_node_detach_validate(NameStr(node->fd.node_name), ht, force, op_type);
			ListCell *cs_lc;

			/* update chunk foreign table server and delete chunk mapping */
			foreach (cs_lc, chunk_data_nodes)
			{
				ChunkDataNode *cdn = lfirst(cs_lc);

				chunk_update_foreign_server_if_needed(cdn->fd.chunk_id, cdn->foreign_server_oid);
				ts_chunk_data_node_delete_by_chunk_id_and_node_name(cdn->fd.chunk_id,
																	NameStr(cdn->fd.node_name));
			}

			/* delete hypertable mapping */
			removed +=
				ts_hypertable_data_node_delete_by_node_name_and_hypertable_id(node_name, ht->fd.id);
		}
		else
		{
			/*  set block new chunks */
			if (block_chunks)
			{
				if (node->fd.block_chunks)
				{
					ereport(NOTICE,
							(errcode(ERRCODE_TS_INTERNAL_ERROR),
							 errmsg("new chunks already blocked on data node \"%s\" for hypertable "
									"\"%s\"",
									NameStr(node->fd.node_name),
									get_rel_name(relid))));
					continue;
				}

				check_replication_for_new_data(node_name, ht, force, BLOCK);
			}
			node->fd.block_chunks = block_chunks;
			removed += ts_hypertable_data_node_update(node);
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
												  BLOCK,
												  block_chunks,
												  force);
}

static int
data_node_detach_hypertable_data_nodes(const char *node_name, List *hypertable_data_nodes,
									   bool all_hypertables, bool force, OperationType op_type)
{
	return data_node_modify_hypertable_data_nodes(node_name,
												  hypertable_data_nodes,
												  all_hypertables,
												  op_type,
												  false,
												  force);
}

static HypertableDataNode *
get_hypertable_data_node(Oid table_id, const char *node_name, bool ownercheck)
{
	HypertableDataNode *hdn = NULL;
	Cache *hcache = ts_hypertable_cache_pin();
	Hypertable *ht = ts_hypertable_cache_get_entry(hcache, table_id);
	ListCell *lc;

	if (ownercheck)
		ts_hypertable_permissions_check(table_id, GetUserId());

	if (ht == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("relation \"%s\" is not a hypertable", get_rel_name(table_id))));

	foreach (lc, ht->data_nodes)
	{
		hdn = lfirst(lc);
		if (namestrcmp(&hdn->fd.node_name, node_name) == 0)
			break;
		else
			hdn = NULL;
	}
	if (hdn == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_TS_DATA_NODE_NOT_ATTACHED),
				 errmsg("data node \"%s\" is not attached to hypertable \"%s\"",
						node_name,
						get_rel_name(table_id))));

	ts_cache_release(hcache);
	return hdn;
}

static Datum
data_node_block_or_allow_new_chunks(PG_FUNCTION_ARGS, bool block_chunks)
{
	const char *node_name = PG_ARGISNULL(0) ? NULL : NameStr(*PG_GETARG_NAME(0));
	Oid table_id = PG_ARGISNULL(1) ? InvalidOid : PG_GETARG_OID(1);
	bool force = PG_ARGISNULL(2) ? false : PG_GETARG_BOOL(2);
	int affected = 0;
	bool all_hypertables = table_id == InvalidOid ? true : false;
	List *hypertable_data_nodes = NIL;
	ForeignServer *server = data_node_get_foreign_server(node_name, ACL_USAGE, false);

	Assert(NULL != server);

	if (OidIsValid(table_id))
	{
		/* Early abort on missing hypertable permissions */
		ts_hypertable_permissions_check(table_id, GetUserId());
		hypertable_data_nodes =
			list_make1(get_hypertable_data_node(table_id, server->servername, true));
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
data_node_set_block_new_chunks(PG_FUNCTION_ARGS, bool block)
{
	return data_node_block_or_allow_new_chunks(fcinfo, block);
}

Datum
data_node_detach(PG_FUNCTION_ARGS)
{
	const char *node_name = PG_ARGISNULL(0) ? NULL : NameStr(*PG_GETARG_NAME(0));
	Oid table_id = PG_ARGISNULL(1) ? InvalidOid : PG_GETARG_OID(1);
	bool all_hypertables = PG_ARGISNULL(1);
	bool force = PG_ARGISNULL(2) ? InvalidOid : PG_GETARG_OID(2);
	int removed = 0;
	List *hypertable_data_nodes = NIL;
	ForeignServer *server = data_node_get_foreign_server(node_name, ACL_USAGE, false);

	Assert(NULL != server);

	if (OidIsValid(table_id))
	{
		/* Early abort on missing hypertable permissions */
		ts_hypertable_permissions_check(table_id, GetUserId());
		hypertable_data_nodes =
			list_make1(get_hypertable_data_node(table_id, server->servername, true));
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
													 DETACH);
	PG_RETURN_INT32(removed);
}

Datum
data_node_delete(PG_FUNCTION_ARGS)
{
	const char *node_name = PG_ARGISNULL(0) ? NULL : PG_GETARG_CSTRING(0);
	bool if_exists = PG_ARGISNULL(1) ? false : PG_GETARG_BOOL(1);
	bool cascade = PG_ARGISNULL(2) ? false : PG_GETARG_BOOL(2);
	bool force = PG_ARGISNULL(3) ? false : PG_GETARG_BOOL(3);
	List *hypertable_data_nodes = NIL;
	DropStmt stmt;
	ObjectAddress address;
	ObjectAddress secondaryObject = InvalidObjectAddress;
	Node *parsetree = NULL;
	UserMapping *um = NULL;
	Cache *conn_cache;
	ForeignServer *server;

	/* Need USAGE to detach. Further owner check done when executing the DROP
	 * statement. */
	server = data_node_get_foreign_server(node_name, ACL_USAGE, if_exists);

	if (NULL == server)
	{
		elog(NOTICE, "data node \"%s\" does not exist, skipping", node_name);
		PG_RETURN_BOOL(false);
	}

	um = get_user_mapping(GetUserId(), server->serverid, true);

	if (um != NULL)
	{
		conn_cache = remote_connection_cache_pin();
		remote_connection_cache_remove(conn_cache, um);
		ts_cache_release(conn_cache);
	}

	/* detach data node */
	hypertable_data_nodes =
		ts_hypertable_data_node_scan_by_node_name(node_name, CurrentMemoryContext);

	data_node_detach_hypertable_data_nodes(node_name, hypertable_data_nodes, true, force, DELETE);

	stmt = (DropStmt){
		.type = T_DropStmt,
		.objects = list_make1(makeString(pstrdup(node_name))),
		.removeType = OBJECT_FOREIGN_SERVER,
		.behavior = cascade ? DROP_CASCADE : DROP_RESTRICT,
		.missing_ok = if_exists,
	};

	parsetree = (Node *) &stmt;

	remove_distributed_id_from_backend(server, um);

	/* Make sure event triggers are invoked so that all dropped objects
	 * are collected during a cascading drop. This ensures all dependent
	 * objects get cleaned up. */
	EventTriggerBeginCompleteQuery();

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
data_node_get_node_name_list_with_aclcheck(AclMode mode)
{
	HeapTuple tuple;
	ScanKeyData scankey[1];
	SysScanDesc scandesc;
	Relation rel;
	ForeignDataWrapper *fdw = GetForeignDataWrapperByName(TIMESCALEDB_FDW_NAME, false);
	List *nodes = NIL;

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

		if (mode != ACL_NO_CHECK)
			data_node_get_foreign_server(NameStr(form->srvname), mode, false);

		nodes = lappend(nodes, pstrdup(NameStr(form->srvname)));
	}

	systable_endscan(scandesc);
	heap_close(rel, AccessShareLock);

	return nodes;
}

/*
 * Get server list without an ACL check.
 */
List *
data_node_get_node_name_list(void)
{
	return data_node_get_node_name_list_with_aclcheck(ACL_NO_CHECK);
}

/*
 * Turn an array of server names into a list of names.
 *
 * The function will verify that all the servers in the list belong to the
 * TimescaleDB foreign data wrapper. Optionally, perform ACL check on each
 * data node's foreign server. Checks are skipped when specificing
 * ACL_NO_CHECK.
 */
List *
data_node_array_to_node_name_list_with_aclcheck(ArrayType *serverarr, AclMode mode)
{
	ArrayIterator it = array_create_iterator(serverarr, 0, NULL);
	Datum node_datum;
	bool isnull;
	List *nodes = NIL;

	while (array_iterate(it, &node_datum, &isnull))
	{
		if (!isnull)
		{
			const char *node_name = DatumGetCString(node_datum);
			ForeignServer *server = data_node_get_foreign_server(node_name, mode, false);

			nodes = lappend(nodes, server->servername);
		}
	}

	array_free_iterator(it);

	return nodes;
}

List *
data_node_array_to_node_name_list(ArrayType *nodearr)
{
	return data_node_array_to_node_name_list_with_aclcheck(nodearr, ACL_NO_CHECK);
}

Datum
data_node_ping(PG_FUNCTION_ARGS)
{
	const char *node_name = PG_ARGISNULL(0) ? NULL : PG_GETARG_CSTRING(0);
	ForeignServer *server = data_node_get_foreign_server(node_name, ACL_USAGE, false);
	bool success;

	Assert(NULL != server);

	success = remote_connection_ping(server->servername);

	PG_RETURN_DATUM(BoolGetDatum(success));
}
