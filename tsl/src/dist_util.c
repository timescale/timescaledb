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

/*
 * When added to a distributed database, this key in the metadata table will be set to match the
 * uuid (from ts_metadata_get_uuid()) of the frontend.  Therefore we can check if a database is the
 * frontend or not simply by comparing the results of dist_util_get_id() and ts_metadata_get_uuid().
 */
#define METADATA_DISTRIBUTED_UUID_KEY_NAME "dist_uuid"

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
		return DIST_MEMBER_FRONTEND;
	else
		return DIST_MEMBER_BACKEND;
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
					(errcode(ERRCODE_TS_SERVERS_ASSIGNMENT_ALREADY_EXISTS),
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
