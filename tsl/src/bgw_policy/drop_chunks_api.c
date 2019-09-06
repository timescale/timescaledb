/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <postgres.h>
#include <catalog/pg_type.h>
#include <utils/builtins.h>
#include <utils/lsyscache.h>
#include <utils/syscache.h>
#include <miscadmin.h>

#include <hypertable_cache.h>
#include <access/xact.h>

#include "bgw/job.h"
#include "bgw_policy/drop_chunks.h"
#include "drop_chunks_api.h"
#include "errors.h"
#include "hypertable.h"
#include "dimension.h"
#include "license.h"
#include "utils.h"
#include "interval.h"

/* Default scheduled interval for drop_chunks jobs is currently 1 day (24 hours) */
#define DEFAULT_SCHEDULE_INTERVAL                                                                  \
	DatumGetIntervalP(DirectFunctionCall3(interval_in, CStringGetDatum("1 day"), InvalidOid, -1))
/* Default max runtime for a drop_chunks job should not be very long. Right now set to 5 minutes */
#define DEFAULT_MAX_RUNTIME                                                                        \
	DatumGetIntervalP(DirectFunctionCall3(interval_in, CStringGetDatum("5 min"), InvalidOid, -1))
/* Right now, there is an infinite number of retries for drop_chunks jobs */
#define DEFAULT_MAX_RETRIES -1
/* Default retry period for drop_chunks_jobs is currently 5 minutes */
#define DEFAULT_RETRY_PERIOD                                                                       \
	DatumGetIntervalP(DirectFunctionCall3(interval_in, CStringGetDatum("5 min"), InvalidOid, -1))

Datum
drop_chunks_add_policy(PG_FUNCTION_ARGS)
{
	NameData application_name;
	NameData drop_chunks_name;
	int32 job_id;
	BgwPolicyDropChunks *existing;
	Oid ht_oid = PG_GETARG_OID(0);
	Datum older_than_datum = PG_GETARG_DATUM(1);
	bool cascade = PG_GETARG_BOOL(2);
	bool if_not_exists = PG_GETARG_BOOL(3);
	bool cascade_to_materializations = PG_GETARG_BOOL(4);
	Oid older_than_type = PG_ARGISNULL(1) ? InvalidOid : get_fn_expr_argtype(fcinfo->flinfo, 1);
	BgwPolicyDropChunks policy;
	Hypertable *hypertable;
	Cache *hcache;
	FormData_ts_interval *older_than;
	license_enforce_enterprise_enabled();
	license_print_expiration_warning_if_needed();
	ts_hypertable_permissions_check(ht_oid, GetUserId());

	/* Make sure that an existing policy doesn't exist on this hypertable */
	hcache = ts_hypertable_cache_pin();
	hypertable = ts_hypertable_cache_get_entry(hcache, ht_oid);

	if (NULL == hypertable)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("\"%s\" is not a hypertable", get_rel_name(ht_oid)),
				 errhint("add_drop_chunk_policy can only be used with hypertables.")));

	if (hypertable->fd.compressed)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot add drop chunks policy to hypertable \"%s\" which contains "
						"compressed data",
						get_rel_name(ht_oid)),
				 errhint("Please add the policy to the corresponding uncompressed hypertable "
						 "instead.")));

	older_than = ts_interval_from_sql_input(ht_oid,
											older_than_datum,
											older_than_type,
											"older_than",
											"add_drop_chunks_policy");

	existing = ts_bgw_policy_drop_chunks_find_by_hypertable(hypertable->fd.id);

	if (existing != NULL)
	{
		if (!if_not_exists)
		{
			ts_cache_release(hcache);
			ereport(ERROR,
					(errcode(ERRCODE_DUPLICATE_OBJECT),
					 errmsg("drop chunks policy already exists for hypertable \"%s\"",
							get_rel_name(ht_oid))));
		}

		if (ts_interval_equal(&existing->fd.older_than, older_than) &&
			existing->fd.cascade == cascade &&
			existing->fd.cascade_to_materializations == cascade_to_materializations)
		{
			/* If all arguments are the same, do nothing */
			ts_cache_release(hcache);
			ereport(NOTICE,
					(errmsg("drop chunks policy already exists on hypertable \"%s\", skipping",
							get_rel_name(ht_oid))));
			PG_RETURN_INT32(-1);
		}
		else
		{
			ts_cache_release(hcache);
			elog(WARNING,
				 "could not add drop_chunks policy due to existing policy on hypertable with "
				 "different arguments");
			PG_RETURN_INT32(-1);
		}
	}

	ts_cache_release(hcache);

	/* Next, insert a new job into jobs table */
	namestrcpy(&application_name, "Drop Chunks Background Job");
	namestrcpy(&drop_chunks_name, "drop_chunks");
	job_id = ts_bgw_job_insert_relation(&application_name,
										&drop_chunks_name,
										DEFAULT_SCHEDULE_INTERVAL,
										DEFAULT_MAX_RUNTIME,
										DEFAULT_MAX_RETRIES,
										DEFAULT_RETRY_PERIOD);

	policy = (BgwPolicyDropChunks){ .fd = {
										.job_id = job_id,
										.hypertable_id = ts_hypertable_relid_to_id(ht_oid),
										.older_than = *older_than,
										.cascade = cascade,
										.cascade_to_materializations = cascade_to_materializations,
									} };

	/* Now, insert a new row in the drop_chunks args table */
	ts_bgw_policy_drop_chunks_insert(&policy);

	PG_RETURN_INT32(job_id);
}

Datum
drop_chunks_remove_policy(PG_FUNCTION_ARGS)
{
	Oid hypertable_oid = PG_GETARG_OID(0);
	bool if_exists = PG_GETARG_BOOL(1);

	/* Remove the job, then remove the policy */
	int ht_id = ts_hypertable_relid_to_id(hypertable_oid);
	BgwPolicyDropChunks *policy = ts_bgw_policy_drop_chunks_find_by_hypertable(ht_id);

	license_enforce_enterprise_enabled();
	license_print_expiration_warning_if_needed();
	ts_hypertable_permissions_check(hypertable_oid, GetUserId());

	if (policy == NULL)
	{
		if (!if_exists)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("cannot remove drop chunks policy, no such policy exists")));
		else
		{
			ereport(NOTICE,
					(errmsg("drop chunks policy does not exist on hypertable \"%s\", skipping",
							get_rel_name(hypertable_oid))));
			PG_RETURN_NULL();
		}
	}

	ts_bgw_job_delete_by_id(policy->fd.job_id);

	PG_RETURN_NULL();
}
