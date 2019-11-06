/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

#include <postgres.h>
#include <funcapi.h>

#include "bgw/job.h"
#include "catalog.h"
#include "compress_chunks.h"
#include "hypertable.h"
#include "interval.h"
#include "policy.h"
#include "scanner.h"
#include "scan_iterator.h"
#include "utils.h"

#include "compat.h"

static ScanTupleResult
bgw_policy_compress_chunks_tuple_found(TupleInfo *ti, void *const data)
{
	BgwPolicyCompressChunks **policy = data;
	bool nulls[Natts_bgw_policy_compress_chunks];
	Datum values[Natts_bgw_policy_compress_chunks];

	heap_deform_tuple(ti->tuple, ti->desc, values, nulls);

	*policy = MemoryContextAllocZero(ti->mctx, sizeof(BgwPolicyCompressChunks));
	Assert(!nulls[AttrNumberGetAttrOffset(Anum_bgw_policy_compress_chunks_job_id)]);
	(*policy)->fd.job_id =
		DatumGetInt32(values[AttrNumberGetAttrOffset(Anum_bgw_policy_compress_chunks_job_id)]);

	Assert(!nulls[AttrNumberGetAttrOffset(Anum_bgw_policy_compress_chunks_hypertable_id)]);
	(*policy)->fd.hypertable_id = DatumGetInt32(
		values[AttrNumberGetAttrOffset(Anum_bgw_policy_compress_chunks_hypertable_id)]);

	Assert(!nulls[AttrNumberGetAttrOffset(Anum_bgw_policy_compress_chunks_older_than)]);

	(*policy)->fd.older_than = *ts_interval_from_tuple(
		values[AttrNumberGetAttrOffset(Anum_bgw_policy_compress_chunks_older_than)]);

	return SCAN_CONTINUE;
}

static ScanTupleResult
compress_policy_delete_row_tuple_found(TupleInfo *ti, void *const data)
{
	CatalogSecurityContext sec_ctx;

	ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);
	ts_catalog_delete(ti->scanrel, ti->tuple);
	ts_catalog_restore_user(&sec_ctx);

	return SCAN_CONTINUE;
}

/* deletes only from the compress_chunks policy table. need to remove the job separately */
bool
ts_bgw_policy_compress_chunks_delete_row_only_by_job_id(int32 job_id)
{
	ScanKeyData scankey[1];

	ScanKeyInit(&scankey[0],
				Anum_bgw_policy_compress_chunks_pkey_job_id,
				BTEqualStrategyNumber,
				F_INT4EQ,
				Int32GetDatum(job_id));

	return ts_catalog_scan_one(BGW_POLICY_COMPRESS_CHUNKS,
							   BGW_POLICY_COMPRESS_CHUNKS_PKEY,
							   scankey,
							   1,
							   compress_policy_delete_row_tuple_found,
							   RowExclusiveLock,
							   BGW_POLICY_COMPRESS_CHUNKS_TABLE_NAME,
							   NULL);
}

BgwPolicyCompressChunks *
ts_bgw_policy_compress_chunks_find_by_hypertable(int32 hypertable_id)
{
	ScanKeyData scankey[1];
	BgwPolicyCompressChunks *ret = NULL;

	ScanKeyInit(&scankey[0],
				Anum_bgw_policy_compress_chunks_hypertable_id_key_hypertable_id,
				BTEqualStrategyNumber,
				F_INT4EQ,
				Int32GetDatum(hypertable_id));

	ts_catalog_scan_one(BGW_POLICY_COMPRESS_CHUNKS,
						BGW_POLICY_COMPRESS_CHUNKS_HYPERTABLE_ID_KEY,
						scankey,
						1,
						bgw_policy_compress_chunks_tuple_found,
						RowExclusiveLock,
						BGW_POLICY_COMPRESS_CHUNKS_TABLE_NAME,
						(void *) &ret);

	return ret;
}

void
ts_bgw_policy_compress_chunks_insert(BgwPolicyCompressChunks *policy)
{
	TupleDesc tupdesc;
	CatalogSecurityContext sec_ctx;
	Datum values[Natts_bgw_policy_compress_chunks];
	bool nulls[Natts_bgw_policy_compress_chunks] = { false };
	HeapTuple ht_older_than;
	Catalog *catalog = ts_catalog_get();
	Relation rel =
		table_open(catalog_get_table_id(catalog, BGW_POLICY_COMPRESS_CHUNKS), RowExclusiveLock);

	tupdesc = RelationGetDescr(rel);

	values[AttrNumberGetAttrOffset(Anum_bgw_policy_compress_chunks_job_id)] =
		Int32GetDatum(policy->fd.job_id);
	values[AttrNumberGetAttrOffset(Anum_bgw_policy_compress_chunks_hypertable_id)] =
		Int32GetDatum(policy->fd.hypertable_id);

	ht_older_than = ts_interval_form_heaptuple(&policy->fd.older_than);

	values[AttrNumberGetAttrOffset(Anum_bgw_policy_compress_chunks_older_than)] =
		HeapTupleGetDatum(ht_older_than);

	ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);
	ts_catalog_insert_values(rel, tupdesc, values, nulls);
	ts_catalog_restore_user(&sec_ctx);
	heap_freetuple(ht_older_than);
	table_close(rel, RowExclusiveLock);
}

BgwPolicyCompressChunks *
ts_bgw_policy_compress_chunks_find_by_job(int32 job_id)
{
	ScanKeyData scankey[1];
	BgwPolicyCompressChunks *ret = NULL;

	ScanKeyInit(&scankey[0],
				Anum_bgw_policy_compress_chunks_pkey_job_id,
				BTEqualStrategyNumber,
				F_INT4EQ,
				Int32GetDatum(job_id));

	ts_catalog_scan_one(BGW_POLICY_COMPRESS_CHUNKS,
						BGW_POLICY_COMPRESS_CHUNKS_PKEY,
						scankey,
						1,
						bgw_policy_compress_chunks_tuple_found,
						RowExclusiveLock,
						BGW_POLICY_COMPRESS_CHUNKS_TABLE_NAME,
						(void *) &ret);

	return ret;
}
