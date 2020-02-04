/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

#include <postgres.h>

#include "bgw/job.h"
#include "catalog.h"
#include "chunk_stats.h"
#include "utils.h"
#include "policy.h"

#include "compat.h"

static ScanTupleResult
bgw_policy_chunk_stats_tuple_found(TupleInfo *ti, void *const data)
{
	BgwPolicyChunkStats **chunk_stats = data;

	*chunk_stats = STRUCT_FROM_TUPLE(ti->tuple,
									 ti->mctx,
									 BgwPolicyChunkStats,
									 FormData_bgw_policy_chunk_stats);
	return SCAN_CONTINUE;
}

/* Cascades deletes via the job delete function */
static ScanTupleResult
bgw_policy_chunk_stats_delete_via_job_tuple_found(TupleInfo *ti, void *const data)
{
	FormData_bgw_policy_chunk_stats *fd = (FormData_bgw_policy_chunk_stats *) GETSTRUCT(ti->tuple);

	/* This call will actually delete the row for us */
	ts_bgw_job_delete_by_id(fd->job_id);
	return SCAN_CONTINUE;
}

/*
 * Delete all chunk_stat rows associated with this job_id.
 * To prevent infinite recursive calls from the job <-> policy tables, we do not cascade deletes in
 * this function. Instead, the caller must be responsible for making sure that the delete cascades
 * to the job corresponding to this policy.
 */
void
ts_bgw_policy_chunk_stats_delete_row_only_by_job_id(int32 job_id)
{
	ScanKeyData scankey[1];

	ScanKeyInit(&scankey[0],
				Anum_bgw_policy_chunk_stats_job_id_chunk_id_idx_job_id,
				BTEqualStrategyNumber,
				F_INT4EQ,
				Int32GetDatum(job_id));

	ts_catalog_scan_all(BGW_POLICY_CHUNK_STATS,
						BGW_POLICY_CHUNK_STATS_JOB_ID_CHUNK_ID_IDX,
						scankey,
						1,
						ts_bgw_policy_delete_row_only_tuple_found,
						RowExclusiveLock,
						NULL);
}

/*
 * Delete all chunk_stat rows associated with this chunk_id.
 * Deletes are cascaded via ...delete_via_job_tuple_found.
 */
void
ts_bgw_policy_chunk_stats_delete_by_chunk_id(int32 chunk_id)
{
	ScanKeyData scankey[1];

	ScanKeyInit(&scankey[0],
				Anum_bgw_policy_chunk_stats_chunk_id,
				BTEqualStrategyNumber,
				F_INT4EQ,
				Int32GetDatum(chunk_id));

	ts_catalog_scan_all(BGW_POLICY_CHUNK_STATS,
						InvalidOid,
						scankey,
						1,
						bgw_policy_chunk_stats_delete_via_job_tuple_found,
						RowExclusiveLock,
						NULL);
}

static void
ts_bgw_policy_chunk_stats_insert_with_relation(Relation rel, BgwPolicyChunkStats *chunk_stats)
{
	TupleDesc tupdesc;
	CatalogSecurityContext sec_ctx;
	Datum values[Natts_bgw_policy_chunk_stats];
	bool nulls[Natts_bgw_policy_chunk_stats] = { false };

	tupdesc = RelationGetDescr(rel);

	values[AttrNumberGetAttrOffset(Anum_bgw_policy_chunk_stats_job_id)] =
		Int32GetDatum(chunk_stats->fd.job_id);
	values[AttrNumberGetAttrOffset(Anum_bgw_policy_chunk_stats_chunk_id)] =
		Int32GetDatum(chunk_stats->fd.chunk_id);
	values[AttrNumberGetAttrOffset(Anum_bgw_policy_chunk_stats_num_times_job_run)] =
		Int32GetDatum(chunk_stats->fd.num_times_job_run);
	values[AttrNumberGetAttrOffset(Anum_bgw_policy_chunk_stats_last_time_job_run)] =
		TimestampTzGetDatum(chunk_stats->fd.last_time_job_run);

	ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);
	ts_catalog_insert_values(rel, tupdesc, values, nulls);
	ts_catalog_restore_user(&sec_ctx);
}

void
ts_bgw_policy_chunk_stats_insert(BgwPolicyChunkStats *chunk_stats)
{
	Catalog *catalog = ts_catalog_get();
	Relation rel =
		table_open(catalog_get_table_id(catalog, BGW_POLICY_CHUNK_STATS), RowExclusiveLock);

	ts_bgw_policy_chunk_stats_insert_with_relation(rel, chunk_stats);
	table_close(rel, RowExclusiveLock);
}

BgwPolicyChunkStats *
ts_bgw_policy_chunk_stats_find(int32 job_id, int32 chunk_id)
{
	ScanKeyData scankeys[2];
	BgwPolicyChunkStats *stats = NULL;

	ScanKeyInit(&scankeys[0],
				Anum_bgw_policy_chunk_stats_job_id_chunk_id_idx_job_id,
				BTEqualStrategyNumber,
				F_INT4EQ,
				Int32GetDatum(job_id));
	ScanKeyInit(&scankeys[1],
				Anum_bgw_policy_chunk_stats_job_id_chunk_id_idx_chunk_id,
				BTEqualStrategyNumber,
				F_INT4EQ,
				Int32GetDatum(chunk_id));

	ts_catalog_scan_one(BGW_POLICY_CHUNK_STATS,
						BGW_POLICY_CHUNK_STATS_JOB_ID_CHUNK_ID_IDX,
						scankeys,
						2,
						bgw_policy_chunk_stats_tuple_found,
						AccessShareLock,
						BGW_POLICY_CHUNK_STATS_TABLE_NAME,
						&stats);
	return stats;
}

static ScanTupleResult
bgw_policy_chunk_stats_update_tuple_found(TupleInfo *ti, void *const data)
{
	TimestampTz *updated_last_time_job_run = data;
	HeapTuple tuple = heap_copytuple(ti->tuple);
	BgwPolicyChunkStats *chunk_stats = STRUCT_FROM_TUPLE(ti->tuple,
														 ti->mctx,
														 BgwPolicyChunkStats,
														 FormData_bgw_policy_chunk_stats);

	chunk_stats->fd.num_times_job_run++;
	chunk_stats->fd.last_time_job_run = *updated_last_time_job_run;

	ts_catalog_update(ti->scanrel, tuple);
	heap_freetuple(tuple);

	return SCAN_CONTINUE;
}

/* This function also increments num_times_job_run by 1. */
void
ts_bgw_policy_chunk_stats_record_job_run(int32 job_id, int32 chunk_id,
										 TimestampTz last_time_job_run)
{
	bool updated;
	ScanKeyData scankeys[2];

	ScanKeyInit(&scankeys[0],
				Anum_bgw_policy_chunk_stats_job_id_chunk_id_idx_job_id,
				BTEqualStrategyNumber,
				F_INT4EQ,
				Int32GetDatum(job_id));
	ScanKeyInit(&scankeys[1],
				Anum_bgw_policy_chunk_stats_job_id_chunk_id_idx_chunk_id,
				BTEqualStrategyNumber,
				F_INT4EQ,
				Int32GetDatum(chunk_id));

	updated = ts_catalog_scan_one(BGW_POLICY_CHUNK_STATS,
								  BGW_POLICY_CHUNK_STATS_JOB_ID_CHUNK_ID_IDX,
								  scankeys,
								  2,
								  bgw_policy_chunk_stats_update_tuple_found,
								  RowExclusiveLock,
								  BGW_POLICY_CHUNK_STATS_TABLE_NAME,
								  &last_time_job_run);

	if (!updated)
	{
		BgwPolicyChunkStats new_stat = {
			.fd = {
				.job_id = job_id,
				.chunk_id = chunk_id,
				.num_times_job_run = 1,
				.last_time_job_run = last_time_job_run,
			},
		};

		ts_bgw_policy_chunk_stats_insert(&new_stat);
	}
}
