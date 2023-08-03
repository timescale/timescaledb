/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <postgres.h>
#include <catalog/namespace.h>
#include <catalog/pg_collation.h>
#include <catalog/pg_type.h>
#include <utils/builtins.h>
#include <utils/timestamp.h>
#include <utils/lsyscache.h>
#include <utils/syscache.h>
#include <miscadmin.h>

#include <compat/compat.h>
#include <dimension.h>
#include <hypertable_cache.h>
#include <jsonb_utils.h>

#include "bgw/job.h"
#include "bgw_policy/job.h"
#include "bgw_policy/reorder_api.h"
#include "errors.h"
#include "hypertable.h"
#include "reorder.h"
#include "utils.h"
#include "guc.h"
#include "bgw/job_stat.h"
#include "bgw/timer.h"

/*
 * Default scheduled interval for reorder jobs should be 1/2 of the default chunk length.
 * If no such length is specified for the hypertable, then
 * the default is 4 days, which is approximately 1/2 of the default chunk size, 7 days.
 */
#define DEFAULT_SCHEDULE_INTERVAL                                                                  \
	{                                                                                              \
		.day = 4                                                                                   \
	}

/* Default max runtime for a reorder job is unlimited for now */
#define DEFAULT_MAX_RUNTIME                                                                        \
	DatumGetIntervalP(DirectFunctionCall3(interval_in, CStringGetDatum("0"), InvalidOid, -1))
/* Right now, there is an infinite number of retries for reorder jobs */
#define DEFAULT_MAX_RETRIES (-1)
/* Default retry period for reorder_jobs is currently 5 minutes */
#define DEFAULT_RETRY_PERIOD                                                                       \
	DatumGetIntervalP(DirectFunctionCall3(interval_in, CStringGetDatum("5 min"), InvalidOid, -1))

#define CONFIG_KEY_HYPERTABLE_ID "hypertable_id"
#define CONFIG_KEY_INDEX_NAME "index_name"

#define POLICY_REORDER_PROC_NAME "policy_reorder"
#define POLICY_REORDER_CHECK_NAME "policy_reorder_check"

int32
policy_reorder_get_hypertable_id(const Jsonb *config)
{
	bool found;
	int32 hypertable_id = ts_jsonb_get_int32_field(config, CONFIG_KEY_HYPERTABLE_ID, &found);

	if (!found)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("could not find hypertable_id in config for job")));

	return hypertable_id;
}

char *
policy_reorder_get_index_name(const Jsonb *config)
{
	char *index_name = NULL;

	if (config != NULL)
		index_name = ts_jsonb_get_str_field(config, CONFIG_KEY_INDEX_NAME);

	if (index_name == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("could not find index_name in config for job")));

	return index_name;
}

static void
check_valid_index(Hypertable *ht, Name index_name)
{
	Oid index_oid;
	HeapTuple idxtuple;
	Form_pg_index indexForm;

	index_oid = ts_get_relation_relid(NameStr(ht->fd.schema_name), NameStr(*index_name), true);
	idxtuple = SearchSysCache1(INDEXRELID, ObjectIdGetDatum(index_oid));
	if (!HeapTupleIsValid(idxtuple))
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("invalid reorder index")));

	indexForm = (Form_pg_index) GETSTRUCT(idxtuple);
	if (indexForm->indrelid != ht->main_table_relid)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid reorder index"),
				 errhint("The reorder index must by an index on hypertable \"%s\".",
						 NameStr(ht->fd.table_name))));

	ReleaseSysCache(idxtuple);
}

Datum
policy_reorder_check(PG_FUNCTION_ARGS)
{
	TS_PREVENT_FUNC_IF_READ_ONLY();

	if (PG_ARGISNULL(0))
	{
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("config must not be NULL")));
	}

	policy_reorder_read_and_validate_config(PG_GETARG_JSONB_P(0), NULL);

	PG_RETURN_VOID();
}

Datum
policy_reorder_proc(PG_FUNCTION_ARGS)
{
	if (PG_NARGS() != 2 || PG_ARGISNULL(0) || PG_ARGISNULL(1))
		PG_RETURN_VOID();

	ts_feature_flag_check(FEATURE_POLICY);
	TS_PREVENT_FUNC_IF_READ_ONLY();

	policy_reorder_execute(PG_GETARG_INT32(0), PG_GETARG_JSONB_P(1));

	PG_RETURN_VOID();
}

Datum
policy_reorder_add(PG_FUNCTION_ARGS)
{
	/* behave like a strict function */
	if (PG_ARGISNULL(0) || PG_ARGISNULL(1) || PG_ARGISNULL(2))
		PG_RETURN_NULL();

	NameData application_name;
	NameData proc_name, proc_schema, check_name, check_schema, owner;
	int32 job_id;
	const Dimension *dim;
	Interval schedule_interval = DEFAULT_SCHEDULE_INTERVAL;
	Oid ht_oid = PG_GETARG_OID(0);
	Name index_name = PG_GETARG_NAME(1);
	bool if_not_exists = PG_GETARG_BOOL(2);
	Cache *hcache;
	Hypertable *ht;
	int32 hypertable_id;
	Oid partitioning_type;
	Oid owner_id;
	List *jobs;
	TimestampTz initial_start = PG_ARGISNULL(3) ? DT_NOBEGIN : PG_GETARG_TIMESTAMPTZ(3);
	bool fixed_schedule = !PG_ARGISNULL(3);
	text *timezone = PG_ARGISNULL(4) ? NULL : PG_GETARG_TEXT_PP(4);
	char *valid_timezone = NULL;

	ts_feature_flag_check(FEATURE_POLICY);
	TS_PREVENT_FUNC_IF_READ_ONLY();

	if (timezone != NULL)
		valid_timezone = ts_bgw_job_validate_timezone(PG_GETARG_DATUM(4));

	ht = ts_hypertable_cache_get_cache_and_entry(ht_oid, CACHE_FLAG_NONE, &hcache);
	Assert(ht != NULL);
	hypertable_id = ht->fd.id;

	/* First verify that the hypertable corresponds to a valid table */
	owner_id = ts_hypertable_permissions_check(ht_oid, GetUserId());

	if (TS_HYPERTABLE_IS_INTERNAL_COMPRESSION_TABLE(ht))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot add reorder policy to compressed hypertable \"%s\"",
						get_rel_name(ht_oid)),
				 errhint("Please add the policy to the corresponding uncompressed hypertable "
						 "instead.")));

	if (hypertable_is_distributed(ht))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("reorder policies not supported on a distributed hypertables")));

	/* Now verify that the index is an actual index on that hypertable */
	check_valid_index(ht, index_name);

	/* Verify that the hypertable owner can create a background worker */
	ts_bgw_job_validate_job_owner(owner_id);

	/* Make sure that an existing reorder policy doesn't exist on this hypertable */
	jobs = ts_bgw_job_find_by_proc_and_hypertable_id(POLICY_REORDER_PROC_NAME,
													 INTERNAL_SCHEMA_NAME,
													 ht->fd.id);

	/*
	 * Try to see if the hypertable has a specified chunk length for the
	 * default schedule interval
	 */
	dim = hyperspace_get_open_dimension(ht->space, 0);
	Assert(dim);

	partitioning_type = ts_dimension_get_partition_type(dim);
	if (IS_TIMESTAMP_TYPE(partitioning_type))
	{
		schedule_interval.time = dim->fd.interval_length / 2;
		schedule_interval.day = 0;
		schedule_interval.month = 0;
	}

	ts_cache_release(hcache);

	if (jobs != NIL)
	{
		BgwJob *existing = linitial(jobs);
		Assert(list_length(jobs) == 1);

		if (!if_not_exists)
			ereport(ERROR,
					(errcode(ERRCODE_DUPLICATE_OBJECT),
					 errmsg("reorder policy already exists for hypertable \"%s\"",
							get_rel_name(ht_oid))));

		if (!DatumGetBool(DirectFunctionCall2Coll(nameeq,
												  C_COLLATION_OID,
												  CStringGetDatum(policy_reorder_get_index_name(
													  existing->fd.config)),
												  NameGetDatum(index_name))))
		{
			ereport(WARNING,
					(errmsg("reorder policy already exists for hypertable \"%s\"",
							get_rel_name(ht_oid)),
					 errdetail("A policy already exists with different arguments."),
					 errhint("Remove the existing policy before adding a new one.")));
			PG_RETURN_INT32(-1);
		}
		/* If all arguments are the same, do nothing */
		ereport(NOTICE,
				(errmsg("reorder policy already exists on hypertable \"%s\", skipping",
						get_rel_name(ht_oid))));
		PG_RETURN_INT32(-1);
	}

	/* if users pass in -infinity for initial_start, then use the current_timestamp instead */
	if (fixed_schedule)
	{
		ts_bgw_job_validate_schedule_interval(&schedule_interval);
		if (TIMESTAMP_NOT_FINITE(initial_start))
			initial_start = ts_timer_get_current_timestamp();
	}

	/* Next, insert a new job into jobs table */
	namestrcpy(&application_name, "Reorder Policy");
	namestrcpy(&proc_name, POLICY_REORDER_PROC_NAME);
	namestrcpy(&proc_schema, INTERNAL_SCHEMA_NAME);
	namestrcpy(&check_name, POLICY_REORDER_CHECK_NAME);
	namestrcpy(&check_schema, INTERNAL_SCHEMA_NAME);
	namestrcpy(&owner, GetUserNameFromId(owner_id, false));

	JsonbParseState *parse_state = NULL;

	pushJsonbValue(&parse_state, WJB_BEGIN_OBJECT, NULL);
	ts_jsonb_add_int32(parse_state, CONFIG_KEY_HYPERTABLE_ID, hypertable_id);
	ts_jsonb_add_str(parse_state, CONFIG_KEY_INDEX_NAME, NameStr(*index_name));
	JsonbValue *result = pushJsonbValue(&parse_state, WJB_END_OBJECT, NULL);
	Jsonb *config = JsonbValueToJsonb(result);

	/* for the reorder policy, we choose a drifting schedule
	 since the user does not control the schedule interval either */
	job_id = ts_bgw_job_insert_relation(&application_name,
										&schedule_interval,
										DEFAULT_MAX_RUNTIME,
										DEFAULT_MAX_RETRIES,
										DEFAULT_RETRY_PERIOD,
										&proc_schema,
										&proc_name,
										&check_schema,
										&check_name,
										owner_id,
										true,
										fixed_schedule,
										hypertable_id,
										config,
										initial_start,
										valid_timezone);

	if (!TIMESTAMP_NOT_FINITE(initial_start))
		ts_bgw_job_stat_upsert_next_start(job_id, initial_start);

	PG_RETURN_INT32(job_id);
}

Datum
policy_reorder_remove(PG_FUNCTION_ARGS)
{
	Oid hypertable_oid = PG_GETARG_OID(0);
	bool if_exists = PG_GETARG_BOOL(1);
	Hypertable *ht;
	Cache *hcache;

	ts_feature_flag_check(FEATURE_POLICY);
	TS_PREVENT_FUNC_IF_READ_ONLY();

	ht = ts_hypertable_cache_get_cache_and_entry(hypertable_oid, CACHE_FLAG_NONE, &hcache);

	List *jobs = ts_bgw_job_find_by_proc_and_hypertable_id(POLICY_REORDER_PROC_NAME,
														   INTERNAL_SCHEMA_NAME,
														   ht->fd.id);
	ts_cache_release(hcache);

	if (jobs == NIL)
	{
		if (!if_exists)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("reorder policy not found for hypertable \"%s\"",
							get_rel_name(hypertable_oid))));
		else
		{
			ereport(NOTICE,
					(errmsg("reorder policy not found for hypertable \"%s\", skipping",
							get_rel_name(hypertable_oid))));
			PG_RETURN_NULL();
		}
	}
	Assert(list_length(jobs) == 1);
	BgwJob *job = linitial(jobs);

	ts_hypertable_permissions_check(hypertable_oid, GetUserId());

	ts_bgw_job_delete_by_id(job->fd.id);

	PG_RETURN_NULL();
}
