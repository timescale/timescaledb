/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <postgres.h>
#include <access/twophase.h>
#include <catalog/pg_type.h>
#include <miscadmin.h>
#include <utils/builtins.h>
#include <utils/fmgrprotos.h>

#include "catalog.h"
#include "dist_util.h"
#include "errors.h"
#include "funcapi.h"
#include "loader/seclabel.h"
#include "metadata.h"
#include "remote/dist_commands.h"
#include "telemetry/telemetry_metadata.h"
#include "utils/uuid.h"

/*
 * When added to a distributed database, this key in the metadata table will be set to match the
 * uuid (from ts_metadata_get_uuid()) of the frontend.  Therefore we can check if a database is the
 * frontend or not simply by comparing the results of dist_util_get_id() and ts_metadata_get_uuid().
 */
#define METADATA_DISTRIBUTED_UUID_KEY_NAME "dist_uuid"

/* UUID associated with remote connection */
static pg_uuid_t *peer_dist_id = NULL;

/* Requires non-null arguments */
static bool
uuid_matches(Datum a, Datum b)
{
	Assert(DatumGetUUIDP(a) != NULL && DatumGetUUIDP(b) != NULL);
	return DatumGetBool(DirectFunctionCall2(uuid_eq, a, b));
}

static Datum
local_get_dist_id(bool *isnull)
{
	return ts_metadata_get_value(CStringGetDatum(METADATA_DISTRIBUTED_UUID_KEY_NAME),
								 CSTRINGOID,
								 UUIDOID,
								 isnull);
}

DistUtilMembershipStatus
dist_util_membership(void)
{
	bool isnull;
	Datum dist_id = local_get_dist_id(&isnull);

	if (isnull)
		return DIST_MEMBER_NONE;
	else if (uuid_matches(dist_id, ts_telemetry_metadata_get_uuid()))
		return DIST_MEMBER_ACCESS_NODE;
	else
		return DIST_MEMBER_DATA_NODE;
}

const char *
dist_util_membership_str(DistUtilMembershipStatus status)
{
	static const char *dist_util_membership_status_str[] = { [DIST_MEMBER_NONE] = "none",
															 [DIST_MEMBER_DATA_NODE] = "data node",
															 [DIST_MEMBER_ACCESS_NODE] =
																 "access node" };
	return dist_util_membership_status_str[status];
}

static void
seclabel_set_dist_uuid(Oid dbid, Datum dist_uuid)
{
	ObjectAddress dbobj;
	Datum uuid_string = DirectFunctionCall1(uuid_out, dist_uuid);
	const char *label = psprintf("%s%c%s",
								 SECLABEL_DIST_TAG,
								 SECLABEL_DIST_TAG_SEPARATOR,
								 DatumGetCString(uuid_string));

	ObjectAddressSet(dbobj, DatabaseRelationId, dbid);
	SetSecurityLabel(&dbobj, SECLABEL_DIST_PROVIDER, label);
}

void
dist_util_set_as_frontend()
{
	dist_util_set_id(ts_telemetry_metadata_get_uuid());

	/*
	 * Set security label to mark current database as the access node database.
	 *
	 * Presence of this label is used as a flag to send NOTICE messsage
	 * after a DROP DATABASE operation completion.
	 */
	seclabel_set_dist_uuid(MyDatabaseId, local_get_dist_id(NULL));
}

bool
dist_util_set_id(Datum dist_id)
{
	if (dist_util_membership() != DIST_MEMBER_NONE)
	{
		if (uuid_matches(dist_id, dist_util_get_id()))
			return false;
		else
			ereport(ERROR,
					(errcode(ERRCODE_TS_DATA_NODE_ASSIGNMENT_ALREADY_EXISTS),
					 (errmsg("database is already a member of a distributed database"))));
	}

	ts_metadata_insert(CStringGetDatum(METADATA_DISTRIBUTED_UUID_KEY_NAME),
					   CSTRINGOID,
					   dist_id,
					   UUIDOID,
					   true);
	return true;
}

Datum
dist_util_get_id()
{
	return local_get_dist_id(NULL);
}

const char *
dist_util_internal_key_name()
{
	return METADATA_DISTRIBUTED_UUID_KEY_NAME;
}

bool
dist_util_remove_from_db()
{
	if (dist_util_membership() != DIST_MEMBER_NONE)
	{
		CatalogSecurityContext sec_ctx;

		ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);
		ts_metadata_drop(CStringGetDatum(METADATA_DISTRIBUTED_UUID_KEY_NAME), CSTRINGOID);
		ts_catalog_restore_user(&sec_ctx);

		return true;
	}

	return false;
}

void
dist_util_set_peer_id(Datum dist_id)
{
	pg_uuid_t *uuid = DatumGetUUIDP(dist_id);
	static pg_uuid_t id;

	if (peer_dist_id != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_TS_INTERNAL_ERROR), (errmsg("distributed peer ID already set"))));

	memcpy(id.data, uuid->data, UUID_LEN);
	peer_dist_id = &id;
}

bool
dist_util_is_frontend_session(void)
{
	Datum dist_id;

	if (dist_util_membership() == DIST_MEMBER_NONE)
		return false;

	if (!peer_dist_id)
		return false;

	dist_id = local_get_dist_id(NULL);
	return uuid_matches(UUIDPGetDatum(peer_dist_id), dist_id);
}

Datum
dist_util_remote_hypertable_info(PG_FUNCTION_ARGS)
{
	char *node_name;
	FuncCallContext *funcctx;
	PGresult *result;

	Assert(!PG_ARGISNULL(0)); /* Strict function */
	node_name = PG_GETARG_NAME(0)->data;

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;
		TupleDesc tupdesc;

		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("function returning record called in context "
							"that cannot accept type record")));

		funcctx->user_fctx =
			ts_dist_cmd_invoke_on_data_nodes("SELECT * FROM "
											 "timescaledb_information.hypertable_size_info;",
											 list_make1((void *) node_name),
											 true);
		funcctx->attinmeta = TupleDescGetAttInMetadata(tupdesc);

		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();
	result = ts_dist_cmd_get_result_by_node_name(funcctx->user_fctx, node_name);

	if (funcctx->call_cntr < PQntuples(result))
	{
		HeapTuple tuple;
		char **fields = palloc(sizeof(char *) * PQnfields(result));
		int i;

		for (i = 0; i < PQnfields(result); ++i)
		{
			if (PQgetisnull(result, funcctx->call_cntr, i) != 1)
			{
				fields[i] = PQgetvalue(result, funcctx->call_cntr, i);

				if (fields[i][0] == '\0')
					fields[i] = NULL;
			}
			else
				fields[i] = NULL;
		}

		tuple = BuildTupleFromCStrings(funcctx->attinmeta, fields);

		SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
	}

	ts_dist_cmd_close_response(funcctx->user_fctx);
	SRF_RETURN_DONE(funcctx);
}

void
validate_data_node_settings(void)
{
	switch (dist_util_membership())
	{
		case DIST_MEMBER_DATA_NODE:
			ereport(ERROR,
					(errcode(ERRCODE_TS_DATA_NODE_INVALID_CONFIG),
					 errmsg("node is already a data node")));
			break;

		case DIST_MEMBER_ACCESS_NODE:
			ereport(ERROR,
					(errcode(ERRCODE_TS_DATA_NODE_INVALID_CONFIG),
					 errmsg("node is already an access node")));
			break;

		default:
			/* Nothing to do */
			break;
	}

	/*
	 * We skip printing the warning if we have already printed the error.
	 */
	if (max_prepared_xacts == 0)
		ereport(ERROR,
				(errcode(ERRCODE_TS_DATA_NODE_INVALID_CONFIG),
				 errmsg("prepared transactions need to be enabled"),
				 errhint("Configuration parameter max_prepared_transactions must be set >0 "
						 "(changes will require restart)."),
				 errdetail("Parameter max_prepared_transactions=%d.", max_prepared_xacts)));
	else if (max_prepared_xacts < MaxConnections)
		ereport(WARNING,
				(errcode(ERRCODE_TS_DATA_NODE_INVALID_CONFIG),
				 errmsg("max_prepared_transactions is set low"),
				 errhint("It is recommended that max_prepared_transactions >= max_connections "
						 "(changes will require restart)."),
				 errdetail("Parameters max_prepared_transactions=%d, max_connections=%d.",
						   max_prepared_xacts,
						   MaxConnections)));
}

/*
 * Check that the data node version is compatible with the version on this
 * node by checking that all of the following are true:
 *
 * - The major version is identical on the data node and the access node.
 * - The minor version on the data node is before or the same as on the access
 *   node.
 *
 * We explicitly do *not* check the patch version since changes between patch
 * versions will only fix bugs and there should be no problem using an older
 * patch version of the extension on the data node.
 *
 * We also check if the version on the data node is older and set
 * `old_version` to `true` or `false` so that caller can print a warning.
 */
bool
dist_util_is_compatible_version(const char *data_node_version, const char *access_node_version,
								bool *is_old_version)
{
	unsigned int data_node_major, data_node_minor, data_node_patch;
	unsigned int access_node_major, access_node_minor, access_node_patch;

	Assert(is_old_version);

	if (sscanf(data_node_version,
			   "%u.%u.%u",
			   &data_node_major,
			   &data_node_minor,
			   &data_node_patch) != 3)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("invalid data node version %s", data_node_version)));
	if (sscanf(access_node_version,
			   "%u.%u.%u",
			   &access_node_major,
			   &access_node_minor,
			   &access_node_patch) != 3)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("invalid access node version %s", access_node_version)));

	if (data_node_major == access_node_major)
		if (data_node_minor == access_node_minor)
			*is_old_version = (data_node_patch < access_node_patch);
		else
			*is_old_version = (data_node_minor < access_node_minor);
	else
		*is_old_version = (data_node_major < access_node_major);

	return (data_node_major == access_node_major) && (data_node_minor <= access_node_minor);
}
