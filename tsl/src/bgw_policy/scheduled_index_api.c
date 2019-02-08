/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <postgres.h>
#include <catalog/namespace.h>
#include <catalog/pg_type.h>
#include <catalog/pg_proc.h>
#include <utils/builtins.h>
#include <utils/timestamp.h>
#include <utils/lsyscache.h>
#include <utils/syscache.h>

#include <dimension.h>

#include "bgw/job.h"
#include "bgw_policy/scheduled_index.h"
#include "errors.h"
#include "hypertable.h"
#include "indexing.h"
#include "license.h"
#include "scheduled_index_api.h"
#include "utils.h"
#include "hypertable.h"
#include "bgw/job.h"
#include <bgw_policy/scheduled_index.h>

/*
 * Default scheduled interval for scheduled_index jobs should be 1/2 of the default chunk length.
 * If no such length is specified for the hypertable, then
 * the default is 4 days, which is approximately 1/2 of the default chunk size, 7 days.
 */
#define DEFAULT_SCHEDULE_INTERVAL                                                                  \
	DatumGetIntervalP(DirectFunctionCall7(make_interval,                                           \
										  Int32GetDatum(0),                                        \
										  Int32GetDatum(0),                                        \
										  Int32GetDatum(0),                                        \
										  Int32GetDatum(4),                                        \
										  Int32GetDatum(0),                                        \
										  Int32GetDatum(0),                                        \
										  Float8GetDatum(0)))
/* Default max runtime for a scheduled_index job is unlimited for now */
#define DEFAULT_MAX_RUNTIME                                                                        \
	DatumGetIntervalP(DirectFunctionCall7(make_interval,                                           \
										  Int32GetDatum(0),                                        \
										  Int32GetDatum(0),                                        \
										  Int32GetDatum(0),                                        \
										  Int32GetDatum(0),                                        \
										  Int32GetDatum(0),                                        \
										  Int32GetDatum(0),                                        \
										  Float8GetDatum(0)))
/* Right now, there is an infinite number of retries for scheduled_index jobs */
#define DEFAULT_MAX_RETRIES -1
/* Default retry period for scheduled_index_jobs is currently 1 day */
#define DEFAULT_RETRY_PERIOD                                                                       \
	DatumGetIntervalP(DirectFunctionCall7(make_interval,                                           \
										  Int32GetDatum(0),                                        \
										  Int32GetDatum(0),                                        \
										  Int32GetDatum(0),                                        \
										  Int32GetDatum(1),                                        \
										  Int32GetDatum(0),                                        \
										  Int32GetDatum(0),                                        \
										  Float8GetDatum(0)))

static void
check_valid_index(Hypertable *ht, Name index_name)
{
	Oid index_oid;
	HeapTuple idxtuple;
	Form_pg_index indexForm;

	index_oid = get_relname_relid(NameStr(*index_name),
								  get_namespace_oid(NameStr(ht->fd.schema_name), false));
	idxtuple = SearchSysCache1(INDEXRELID, ObjectIdGetDatum(index_oid));
	if (!HeapTupleIsValid(idxtuple))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("could not add scheduled_index policy because the provided index is not a "
						"valid "
						"relation")));

	indexForm = (Form_pg_index) GETSTRUCT(idxtuple);
	if (indexForm->indrelid != ht->main_table_relid)
		elog(ERROR,
			 "could not add scheduled_index policy because the provided index is not a valid index "
			 "on the "
			 "hypertable");
	ReleaseSysCache(idxtuple);
}

// extern Datum nameeq(PG_FUNCTION_ARGS);

Datum
scheduled_index_add_policy(PG_FUNCTION_ARGS)
{
	NameData application_name;
	NameData scheduled_index_name;
	int32 job_id;
	BgwPolicyScheduledIndex *existing;
	Dimension *dim;

	Interval *default_schedule_interval = DEFAULT_SCHEDULE_INTERVAL;
	Oid ht_oid = PG_GETARG_OID(0);
	Name index_name = PG_GETARG_NAME(1);
	bool if_not_exists = PG_GETARG_BOOL(2);
	int32 hypertable_id = ts_hypertable_relid_to_id(ht_oid);
	Hypertable *ht = ts_hypertable_get_by_id(hypertable_id);

	BgwPolicyScheduledIndex policy = { .fd = {
										   .hypertable_id = hypertable_id,
										   .hypertable_index_name = *index_name,
									   } };
	OptionalIndexInfo *optional_index_info;

	license_enforce_enterprise_enabled();
	license_print_expiration_warning_if_needed();

	/* First verify that the hypertable corresponds to a valid table */
	if (!ts_is_hypertable(ht_oid))
		ereport(ERROR,
				(errcode(ERRCODE_TS_HYPERTABLE_NOT_EXIST),
				 errmsg("could not add scheduled_index policy because \"%s\" is not a hypertable",
						get_rel_name(ht_oid))));

	/* Now verify that the index is an actual index on that hypertable */
	check_valid_index(ht, index_name);

	/* Make sure that an existing policy doesn't exist on this hypertable */
	existing = ts_bgw_policy_scheduled_index_find_by_hypertable(ts_hypertable_relid_to_id(ht_oid));

	if (existing != NULL)
	{
		if (!if_not_exists)
			ereport(ERROR,
					(errcode(ERRCODE_DUPLICATE_OBJECT),
					 errmsg("scheduled_index policy already exists for hypertable \"%s\"",
							get_rel_name(ht_oid))));

		if (!DatumGetBool(DirectFunctionCall2(nameeq,
											  NameGetDatum(&existing->fd.hypertable_index_name),
											  NameGetDatum(index_name))))
		{
			elog(WARNING,
				 "could not add scheduled_index policy due to existing policy on hypertable with "
				 "different "
				 "arguments");
			return -1;
		}
		/* If all arguments are the same, do nothing */
		ereport(NOTICE,
				(errmsg("scheduled_index policy already exists on hypertable \"%s\", skipping",
						get_rel_name(ht_oid))));
		return -1;
	}

	/* Next, insert a new job into jobs table */
	namestrcpy(&application_name, "ScheduledIndex Background Job");
	namestrcpy(&scheduled_index_name, "scheduled_index");

	/*
	 * Try to see if the hypertable has a specified chunk length for the
	 * default schedule interval
	 */
	dim = hyperspace_get_open_dimension(ht->space, 0);

	if (dim && IS_TIMESTAMP_TYPE(dim->fd.column_type))
		default_schedule_interval = DatumGetIntervalP(
			DirectFunctionCall7(make_interval,
								Int32GetDatum(0),
								Int32GetDatum(0),
								Int32GetDatum(0),
								Int32GetDatum(0),
								Int32GetDatum(0),
								Int32GetDatum(0),
								Float8GetDatum(dim->fd.interval_length / 2000000)));

	job_id = ts_bgw_job_insert_relation(&application_name,
										&scheduled_index_name,
										default_schedule_interval,
										DEFAULT_MAX_RUNTIME,
										DEFAULT_MAX_RETRIES,
										DEFAULT_RETRY_PERIOD);

	/* Now, insert a new row in the scheduled_index args table */
	policy.fd.job_id = job_id;
	ts_bgw_policy_scheduled_index_insert(&policy);

	optional_index_info = ts_indexing_optional_info_find_by_index_name(index_name);
	if (optional_index_info == NULL)
		optional_index_info = ts_optional_index_info_alloc(NameStr(*index_name));

	optional_index_info->fd.is_scheduled = true;
	Assert(DirectFunctionCall2(nameeq,
							   NameGetDatum(index_name),
							   NameGetDatum(&optional_index_info->fd.hypertable_index_name)));

	// TODO: use update when optional_index_info already exists
	ts_indexing_optional_info_insert(optional_index_info);

	PG_RETURN_INT32(job_id);
}

Datum
scheduled_index_remove_policy(PG_FUNCTION_ARGS)
{
	Oid hypertable_oid = PG_GETARG_OID(0);
	bool if_exists = PG_GETARG_BOOL(1);

	/* Remove the job, then remove the policy */
	int ht_id = ts_hypertable_relid_to_id(hypertable_oid);
	BgwPolicyScheduledIndex *policy = ts_bgw_policy_scheduled_index_find_by_hypertable(ht_id);

	license_enforce_enterprise_enabled();
	license_print_expiration_warning_if_needed();

	if (policy == NULL)
	{
		if (!if_exists)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("cannot remove scheduled_index policy, no such policy exists")));
		else
		{
			char *hypertable_name = get_rel_name(hypertable_oid);

			if (hypertable_name != NULL)
				ereport(NOTICE,
						(errmsg("scheduled_index policy does not exist on hypertable \"%s\", "
								"skipping",
								hypertable_name)));
			else
				ereport(NOTICE,
						(errmsg("scheduled_index policy does not exist on unnamed hypertable, "
								"skipping")));
			PG_RETURN_VOID();
		}
	}

	ts_bgw_job_delete_by_id(policy->fd.job_id);
	ts_indexing_optional_info_delete_by_index_name(&policy->fd.hypertable_index_name);

	PG_RETURN_VOID();
}
