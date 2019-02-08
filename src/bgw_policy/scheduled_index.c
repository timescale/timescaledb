/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

#include <postgres.h>
#include <utils/builtins.h>
#include <utils/timestamp.h>
#include <utils/lsyscache.h>
#include <utils/syscache.h>

#include "catalog.h"
#include "policy.h"
#include "scheduled_index.h"
#include "scanner.h"
#include "utils.h"
#include "hypertable.h"
#include "bgw/job.h"

static ScanTupleResult
bgw_policy_scheduled_index_tuple_found(TupleInfo *ti, void *const data)
{
	BgwPolicyScheduledIndex **policy = data;

	*policy = STRUCT_FROM_TUPLE(ti->tuple,
								ti->mctx,
								BgwPolicyScheduledIndex,
								FormData_bgw_policy_scheduled_index);

	return SCAN_CONTINUE;
}

/*
 * To prevent infinite recursive calls from the job <-> policy tables, we do not cascade deletes in
 * this function. Instead, the caller must be responsible for making sure that the delete cascades
 * to the job corresponding to this policy.
 */
bool
ts_bgw_policy_scheduled_index_delete_row_only_by_job_id(int32 job_id)
{
	ScanKeyData scankey[1];

	ScanKeyInit(&scankey[0],
				Anum_bgw_policy_scheduled_index_pkey_idx_job_id,
				BTEqualStrategyNumber,
				F_INT4EQ,
				Int32GetDatum(job_id));

	return ts_catalog_scan_one(BGW_POLICY_SCHEDULED_INDEX,
							   BGW_POLICY_SCHEDULED_INDEX_PKEY_IDX,
							   scankey,
							   1,
							   ts_bgw_policy_delete_row_only_tuple_found,
							   RowExclusiveLock,
							   BGW_POLICY_SCHEDULED_INDEX_TABLE_NAME,
							   NULL);
}

BgwPolicyScheduledIndex *
ts_bgw_policy_scheduled_index_find_by_job(int32 job_id)
{
	ScanKeyData scankey[1];
	BgwPolicyScheduledIndex *ret = NULL;

	ScanKeyInit(&scankey[0],
				Anum_bgw_policy_scheduled_index_pkey_idx_job_id,
				BTEqualStrategyNumber,
				F_INT4EQ,
				Int32GetDatum(job_id));

	ts_catalog_scan_one(BGW_POLICY_SCHEDULED_INDEX,
						BGW_POLICY_SCHEDULED_INDEX_PKEY_IDX,
						scankey,
						1,
						bgw_policy_scheduled_index_tuple_found,
						AccessShareLock,
						BGW_POLICY_SCHEDULED_INDEX_TABLE_NAME,
						(void *) &ret);

	return ret;
}

BgwPolicyScheduledIndex *
ts_bgw_policy_scheduled_index_find_by_hypertable(int32 hypertable_id)
{
	ScanKeyData scankey[1];
	BgwPolicyScheduledIndex *ret = NULL;

	ScanKeyInit(&scankey[0],
				Anum_bgw_policy_scheduled_index_hypertable_id_idx_hypertable_id,
				BTEqualStrategyNumber,
				F_INT4EQ,
				Int32GetDatum(hypertable_id));

	ts_catalog_scan_one(BGW_POLICY_SCHEDULED_INDEX,
						BGW_POLICY_SCHEDULED_INDEX_HYPERTABLE_ID_IDX,
						scankey,
						1,
						bgw_policy_scheduled_index_tuple_found,
						AccessShareLock,
						BGW_POLICY_SCHEDULED_INDEX_TABLE_NAME,
						(void *) &ret);

	return ret;
}

static void
ts_bgw_policy_scheduled_index_insert_with_relation(Relation rel, BgwPolicyScheduledIndex *policy)
{
	TupleDesc tupdesc;
	CatalogSecurityContext sec_ctx;
	Datum values[Natts_bgw_policy_scheduled_index];
	bool nulls[Natts_bgw_policy_scheduled_index] = { false };

	tupdesc = RelationGetDescr(rel);

	values[AttrNumberGetAttrOffset(Anum_bgw_policy_scheduled_index_job_id)] =
		Int32GetDatum(policy->fd.job_id);
	values[AttrNumberGetAttrOffset(Anum_bgw_policy_scheduled_index_hypertable_id)] =
		Int32GetDatum(policy->fd.hypertable_id);
	values[AttrNumberGetAttrOffset(Anum_bgw_policy_scheduled_index_hypertable_index_name)] =
		NameGetDatum(&policy->fd.hypertable_index_name);

	ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);
	ts_catalog_insert_values(rel, tupdesc, values, nulls);
	ts_catalog_restore_user(&sec_ctx);
}

void
ts_bgw_policy_scheduled_index_insert(BgwPolicyScheduledIndex *policy)
{
	Catalog *catalog = ts_catalog_get();
	Relation rel =
		heap_open(catalog_get_table_id(catalog, BGW_POLICY_SCHEDULED_INDEX), RowExclusiveLock);

	ts_bgw_policy_scheduled_index_insert_with_relation(rel, policy);
	heap_close(rel, RowExclusiveLock);
}
