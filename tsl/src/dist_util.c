/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <postgres.h>
#include "dist_util.h"
#include "metadata.h"
#include "telemetry/telemetry_metadata.h"
#include <catalog/pg_type.h>
#include "catalog.h"
#include "utils/uuid.h"
#include "errors.h"
#include <utils/fmgrprotos.h>
#include "remote/dist_commands.h"
#include "funcapi.h"
#include <access/twophase.h>
#include <miscadmin.h>
#include "compat.h"
#include <utils/builtins.h>

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

void
dist_util_set_as_frontend()
{
	dist_util_set_id(ts_telemetry_metadata_get_uuid());
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
											 list_make1((void *) node_name));
		funcctx->attinmeta = TupleDescGetAttInMetadata(tupdesc);

		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();
	result = ts_dist_cmd_get_data_node_result(funcctx->user_fctx, node_name);

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
#if !PG_VERSION_SUPPORTS_MULTINODE
	/* This should really be enforced by the access node since we might
	 * deprecate PostgreSQL versions that previously were supported for
	 * multinode setups. This is just extra safety. */
	ereport(ERROR,
			(errcode(ERRCODE_TS_DATA_NODE_INVALID_CONFIG),
			 errmsg("unsupported PostgreSQL version %s", PG_VERSION)));
#endif

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
