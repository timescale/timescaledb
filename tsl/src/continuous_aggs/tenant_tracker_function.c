/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

/*
 * tsl_hypertable_get_tenant_tracking_info(regclass) returns the current tracker state present in
 * shared memory for a hypertable
 *
 * tsl_tenant_tracking_map() lists the hypertables the tracker map knows about, in
 * every database
 */

#include <postgres.h>
#include <access/htup_details.h>
#include <fmgr.h>
#include <funcapi.h>
#include <miscadmin.h>
#include <utils/tuplestore.h>

#include "hypertable.h"
#include "tenant_tracker.h"
#include "tenant_tracker_function.h"

Datum
tsl_hypertable_get_tenant_tracking_info(PG_FUNCTION_ARGS)
{
	Oid relid = PG_ARGISNULL(0) ? InvalidOid : PG_GETARG_OID(0);
	int32 hypertable_id =
		OidIsValid(relid) ? ts_hypertable_relid_to_id(relid) : INVALID_HYPERTABLE_ID;
	TenantTracking *tracking = NULL;
	TupleDesc tupdesc;
	Datum values[6];
	bool nulls[6];

	if (hypertable_id != INVALID_HYPERTABLE_ID)
	{
		/* Tracker state is per-hypertable data: restrict it to the owner. */
		ts_hypertable_permissions_check(relid, GetUserId());
		tracking = ts_tenant_tracker_lookup(hypertable_id);
	}

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("function returning record called in context that cannot accept type "
						"record")));
	}
	tupdesc = BlessTupleDesc(tupdesc);

	if (tracking == NULL)
	{
		/* Not a hypertable, or no tracker for it yet -> all-NULL row. */
		memset(nulls, true, sizeof(nulls));
		memset(values, 0, sizeof(values));
	}
	else
	{
		TenantTrackerInfo info;

		ts_tenant_tracker_get_info(tracking, &info);
		memset(nulls, false, sizeof(nulls));
		values[0] = Int32GetDatum(info.seqnum);
		values[1] = Int32GetDatum((int32) info.active_generation);
		values[2] = Int32GetDatum((int32) info.nentries);
		values[3] = Int32GetDatum((int32) info.status);
		values[4] = Int64GetDatum(info.late_threshold_start);
		values[5] = Int64GetDatum(info.late_threshold_end);
	}

	PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(tupdesc, values, nulls)));
}

#define TENANT_TRACKING_MAP_NCOLS 3

/*
 * One row per (database, hypertable) in the tracker map.  is_tracked is false
 * when the map has an entry but its tracker could not be allocated (out of
 * shared memory), i.e. tracking is off for that hypertable until restart.
 *
 * ts_tenant_tracker_map_get_entries palloc's `entries` in the current memory
 * context and we own it from there on.  tuplestore_putvalues copies each row into
 * the tuplestore (which InitMaterializedSRF created in the per-query context), so
 * nothing points into the array once the loop is done.  With no entries at all
 * (nothing tracked yet, or no loader) the array is NULL and the result set is
 * simply empty.
 */
Datum
tsl_tenant_tracking_map(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TenantTrackerMapEntry *entries;
	int nentries;

	if (!superuser())
	{
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("insufficient privilege to list hypertables in the per-tenant "
						"invalidation tracker"),
				 errdetail("Only superusers can call this function.")));
	}

	InitMaterializedSRF(fcinfo, 0);

	nentries = ts_tenant_tracker_map_get_entries(&entries);

	for (int i = 0; i < nentries; i++)
	{
		Datum values[TENANT_TRACKING_MAP_NCOLS];
		bool nulls[TENANT_TRACKING_MAP_NCOLS];

		memset(nulls, false, sizeof(nulls));
		values[0] = ObjectIdGetDatum(entries[i].database_id);
		values[1] = Int32GetDatum(entries[i].hypertable_id);
		values[2] = BoolGetDatum(entries[i].is_tracked);

		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
	}

	if (entries != NULL)
	{
		pfree(entries);
	}

	PG_RETURN_VOID();
}
