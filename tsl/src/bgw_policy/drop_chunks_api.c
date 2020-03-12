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
#include "continuous_agg.h"
#include "chunk.h"
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

typedef struct DropChunksMeta
{
	Hypertable *ht;
	Oid ht_oid;
	FormData_ts_interval *older_than;
} DropChunksMeta;

static void
validate_drop_chunks_hypertable(Cache *hcache, Oid user_htoid, Oid older_than_type,
								Datum older_than_datum, DropChunksMeta *meta)
{
	FormData_ts_interval *older_than;
	ContinuousAggHypertableStatus status;

	meta->ht = NULL;
	meta->ht_oid = user_htoid;
	meta->ht = ts_hypertable_cache_get_entry(hcache, meta->ht_oid, true /* missing_ok */);
	if (meta->ht != NULL)
	{
		if (meta->ht->fd.compressed)
		{
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("cannot add drop chunks policy to compressed hypertable \"%s\"",
							get_rel_name(user_htoid)),
					 errhint("Please add the policy to the corresponding uncompressed hypertable "
							 "instead.")));
		}
		status = ts_continuous_agg_hypertable_status(meta->ht->fd.id);
		if ((status == HypertableIsMaterialization || status == HypertableIsMaterializationAndRaw))
		{
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("cannot add drop chunks policy to materialized hypertable \"%s\" ",
							get_rel_name(user_htoid)),
					 errhint("Please add the policy to the corresponding continuous aggregate "
							 "instead.")));
		}
		older_than = ts_interval_from_sql_input(meta->ht_oid,
												older_than_datum,
												older_than_type,
												"older_than",
												"add_drop_chunks_policy");
	}
	else
	{
		/*check if this is a cont aggregate view */
		int32 mat_id;
		Dimension *open_dim;
		Oid partitioning_type;
		char *schema = get_namespace_name(get_rel_namespace(user_htoid));
		char *view_name = get_rel_name(user_htoid);
		ContinuousAgg *ca = NULL;
		ca = ts_continuous_agg_find_by_view_name(schema, view_name);
		if (ca == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_TS_HYPERTABLE_NOT_EXIST),
					 errmsg("\"%s\" is not a hypertable or a continuous aggregate view",
							view_name)));
		mat_id = ca->data.mat_hypertable_id;
		meta->ht = ts_hypertable_get_by_id(mat_id);
		Assert(meta->ht != NULL);
		open_dim = hyperspace_get_open_dimension(meta->ht->space, 0);
		partitioning_type = ts_dimension_get_partition_type(open_dim);
		if (IS_INTEGER_TYPE(partitioning_type))
		{
			open_dim = ts_continuous_agg_find_integer_now_func_by_materialization_id(mat_id);
		}
		older_than = ts_interval_from_sql_input_internal(open_dim,
														 older_than_datum,
														 older_than_type,
														 "older_than",
														 "add_drop_chunks_policy");
	}
	Assert(meta->ht != NULL);
	meta->older_than = older_than;
	return;
}

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
	CascadeToMaterializationOption cascade_to_materializations =
		(PG_ARGISNULL(4) ? CASCADE_TO_MATERIALIZATION_UNKNOWN :
						   (PG_GETARG_BOOL(4) ? CASCADE_TO_MATERIALIZATION_TRUE :
												CASCADE_TO_MATERIALIZATION_FALSE));
	Oid older_than_type = PG_ARGISNULL(1) ? InvalidOid : get_fn_expr_argtype(fcinfo->flinfo, 1);
	BgwPolicyDropChunks policy;
	Hypertable *hypertable;
	Cache *hcache;
	FormData_ts_interval *older_than;
	DropChunksMeta meta;
	Oid mapped_oid;
	Oid owner_id = ts_hypertable_permissions_check(ht_oid, GetUserId());

	/* Verify that the hypertable owner can create a background worker */
	ts_bgw_job_validate_job_owner(owner_id, JOB_TYPE_DROP_CHUNKS);

	/* Make sure that an existing policy doesn't exist on this hypertable */
	hcache = ts_hypertable_cache_pin();
	validate_drop_chunks_hypertable(hcache, ht_oid, older_than_type, older_than_datum, &meta);
	older_than = meta.older_than;
	hypertable = meta.ht;
	mapped_oid = meta.ht->main_table_relid;

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

		if (ts_interval_equal(&existing->older_than, older_than) && existing->cascade == cascade &&
			existing->cascade_to_materializations == cascade_to_materializations)
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
				 "could not add drop chunks policy due to existing policy on hypertable with "
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

	policy = (BgwPolicyDropChunks){
		.job_id = job_id,
		.hypertable_id = ts_hypertable_relid_to_id(mapped_oid),
		.older_than = *older_than,
		.cascade = cascade,
		.cascade_to_materializations = cascade_to_materializations,
	};

	/* Now, insert a new row in the drop_chunks args table */
	ts_bgw_policy_drop_chunks_insert(&policy);

	PG_RETURN_INT32(job_id);
}

Datum
drop_chunks_remove_policy(PG_FUNCTION_ARGS)
{
	Oid table_oid = PG_GETARG_OID(0);
	bool if_exists = PG_GETARG_BOOL(1);
	Cache *hcache;

	Hypertable *hypertable = ts_hypertable_cache_get_cache_and_entry(table_oid, true, &hcache);
	if (!hypertable)
	{
		char *view_name = get_rel_name(table_oid);
		if (!view_name)
		{
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("OID %d does not refer to a hypertable or continuous aggregate",
							table_oid)));
		}
		else
		{
			char *schema_name = get_namespace_name(get_rel_namespace(table_oid));
			ContinuousAgg *ca = ts_continuous_agg_find_by_view_name(schema_name, view_name);
			if (!ca)
				ereport(ERROR,
						(errcode(ERRCODE_UNDEFINED_OBJECT),
						 errmsg("no hypertable or continuous aggregate by the name \"%s\" exists",
								view_name)));
			hypertable = ts_hypertable_get_by_id(ca->data.mat_hypertable_id);
		}
	}

	Assert(hypertable != NULL);

	/* Remove the job, then remove the policy */
	BgwPolicyDropChunks *policy = ts_bgw_policy_drop_chunks_find_by_hypertable(hypertable->fd.id);

	ts_cache_release(hcache);

	ts_hypertable_permissions_check(table_oid, GetUserId());

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
							get_rel_name(table_oid))));
			PG_RETURN_NULL();
		}
	}

	ts_bgw_job_delete_by_id(policy->job_id);

	PG_RETURN_NULL();
}
