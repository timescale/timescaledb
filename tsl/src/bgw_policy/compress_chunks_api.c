/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <postgres.h>
#include <access/xact.h>
#include <miscadmin.h>
#include <utils/builtins.h>

#include "bgw_policy/compress_chunks.h"
#include "bgw/job.h"
#include "compress_chunks_api.h"
#include "errors.h"
#include "hypertable.h"
#include "hypertable_cache.h"
#include "interval.h"
#include "license.h"
#include "utils.h"

/*
 * Default scheduled interval for compress jobs = default chunk length.
 * If this is non-timestamp based hypertable, then default is 1 day
 */
#define DEFAULT_SCHEDULE_INTERVAL                                                                  \
	DatumGetIntervalP(DirectFunctionCall7(make_interval,                                           \
										  Int32GetDatum(0),                                        \
										  Int32GetDatum(0),                                        \
										  Int32GetDatum(0),                                        \
										  Int32GetDatum(1),                                        \
										  Int32GetDatum(0),                                        \
										  Int32GetDatum(0),                                        \
										  Float8GetDatum(0)))
/* Default max runtime is unlimited for compress chunks */
#define DEFAULT_MAX_RUNTIME                                                                        \
	DatumGetIntervalP(DirectFunctionCall7(make_interval,                                           \
										  Int32GetDatum(0),                                        \
										  Int32GetDatum(0),                                        \
										  Int32GetDatum(0),                                        \
										  Int32GetDatum(0),                                        \
										  Int32GetDatum(0),                                        \
										  Int32GetDatum(0),                                        \
										  Float8GetDatum(0)))
/* Right now, there is an infinite number of retries for compress_chunks jobs */
#define DEFAULT_MAX_RETRIES -1
/* Default retry period for reorder_jobs is currently 1 hour */
#define DEFAULT_RETRY_PERIOD                                                                       \
	DatumGetIntervalP(DirectFunctionCall7(make_interval,                                           \
										  Int32GetDatum(0),                                        \
										  Int32GetDatum(0),                                        \
										  Int32GetDatum(0),                                        \
										  Int32GetDatum(0),                                        \
										  Int32GetDatum(1),                                        \
										  Int32GetDatum(0),                                        \
										  Float8GetDatum(0)))

Datum
compress_chunks_add_policy(PG_FUNCTION_ARGS)
{
	NameData application_name;
	NameData compress_chunks_name;
	int32 job_id;
	BgwPolicyCompressChunks *existing;
	Oid ht_oid = PG_GETARG_OID(0);
	Datum older_than_datum = PG_GETARG_DATUM(1);
	Oid older_than_type = PG_ARGISNULL(1) ? InvalidOid : get_fn_expr_argtype(fcinfo->flinfo, 1);
	bool if_not_exists = PG_GETARG_BOOL(2);
	Interval *default_schedule_interval = DEFAULT_SCHEDULE_INTERVAL;

	BgwPolicyCompressChunks policy;
	Hypertable *hypertable;
	Cache *hcache;
	Dimension *dim;
	FormData_ts_interval *older_than;
	ts_hypertable_permissions_check(ht_oid, GetUserId());
	Oid owner_id = ts_hypertable_permissions_check(ht_oid, GetUserId());

	older_than = ts_interval_from_sql_input(ht_oid,
											older_than_datum,
											older_than_type,
											"older_than",
											"compress_chunks_add_policy");

	/* check if this is a table with compression enabled */
	hypertable = ts_hypertable_cache_get_cache_and_entry(ht_oid, CACHE_FLAG_NONE, &hcache);
	if (!TS_HYPERTABLE_HAS_COMPRESSION(hypertable))
	{
		ts_cache_release(hcache);
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("can add compress_chunks policy only on hypertables with compression "
						"enabled")));
	}

	ts_bgw_job_validate_job_owner(owner_id, JOB_TYPE_COMPRESS_CHUNKS);

	/* Make sure that an existing policy doesn't exist on this hypertable */
	existing = ts_bgw_policy_compress_chunks_find_by_hypertable(hypertable->fd.id);

	if (existing != NULL)
	{
		if (!if_not_exists)
		{
			ts_cache_release(hcache);
			ereport(ERROR,
					(errcode(ERRCODE_DUPLICATE_OBJECT),
					 errmsg("compress chunks policy already exists for hypertable \"%s\"",
							get_rel_name(ht_oid))));
		}
		if (ts_interval_equal(&existing->fd.older_than, older_than))
		{
			/* If all arguments are the same, do nothing */
			ts_cache_release(hcache);
			ereport(NOTICE,
					(errmsg("compress chunks policy already exists on hypertable \"%s\", skipping",
							get_rel_name(ht_oid))));
			PG_RETURN_INT32(-1);
		}
		else
		{
			ts_cache_release(hcache);
			elog(WARNING,
				 "could not add compress_chunks policy due to existing policy on hypertable with "
				 "different arguments");
			PG_RETURN_INT32(-1);
		}
	}
	dim = hyperspace_get_open_dimension(hypertable->space, 0);

	if (dim && IS_TIMESTAMP_TYPE(ts_dimension_get_partition_type(dim)))
	{
		default_schedule_interval = DatumGetIntervalP(
			ts_internal_to_interval_value(dim->fd.interval_length / 2, INTERVALOID));
	}

	/* insert a new job into jobs table */
	namestrcpy(&application_name, "Compress Chunks Background Job");
	namestrcpy(&compress_chunks_name, "compress_chunks");
	job_id = ts_bgw_job_insert_relation(&application_name,
										&compress_chunks_name,
										default_schedule_interval,
										DEFAULT_MAX_RUNTIME,
										DEFAULT_MAX_RETRIES,
										DEFAULT_RETRY_PERIOD);

	policy = (BgwPolicyCompressChunks){ .fd = {
											.job_id = job_id,
											.hypertable_id = ts_hypertable_relid_to_id(ht_oid),
											.older_than = *older_than,
										} };

	/* Now, insert a new row in the compress_chunks args table */
	ts_bgw_policy_compress_chunks_insert(&policy);
	ts_cache_release(hcache);

	PG_RETURN_INT32(job_id);
}

Datum
compress_chunks_remove_policy(PG_FUNCTION_ARGS)
{
	Oid hypertable_oid = PG_GETARG_OID(0);
	bool if_exists = PG_GETARG_BOOL(1);

	/* Remove the job, then remove the policy */
	int ht_id = ts_hypertable_relid_to_id(hypertable_oid);
	BgwPolicyCompressChunks *policy = ts_bgw_policy_compress_chunks_find_by_hypertable(ht_id);

	ts_hypertable_permissions_check(hypertable_oid, GetUserId());

	if (policy == NULL)
	{
		if (!if_exists)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("cannot remove compress chunks policy, no such policy exists")));
		else
		{
			ereport(NOTICE,
					(errmsg("compress chunks policy does not exist on hypertable \"%s\", skipping",
							get_rel_name(hypertable_oid))));
			PG_RETURN_BOOL(false);
		}
	}

	ts_bgw_job_delete_by_id(policy->fd.job_id);

	PG_RETURN_BOOL(true);
}
