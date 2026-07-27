/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

#include <postgres.h>
#include <access/htup_details.h>
#include <access/table.h>
#include <access/xact.h>
#include <utils/rel.h>

#include "debug_assert.h"
#include "scan_iterator.h"
#include "ts_catalog/catalog.h"
#include "ts_catalog/continuous_aggs_tenant_tracking.h"

static void
init_scan_by_hypertable_id(ScanIterator *iterator, const int32 hypertable_id)
{
	iterator->ctx.index = catalog_get_index(ts_catalog_get(),
											CONTINUOUS_AGGS_TENANT_TRACKING,
											CONTINUOUS_AGGS_TENANT_TRACKING_IDX);

	ts_scan_iterator_scan_key_init(iterator,
								   Anum_continuous_aggs_tenant_tracking_idx_hypertable_id,
								   BTEqualStrategyNumber,
								   F_INT4EQ,
								   Int32GetDatum(hypertable_id));
}

/*
 * Insert one tenant tracking row into an already-open relation, without
 * advancing the command counter (mirrors ts_catalog_insert_only). tenant_id is
 * copied into a text value holding the exact tenant key bytes.
 *
 * Callers that need the row visible to later commands in the same transaction must run
 * CommandCounterIncrement() themselves (see ts_cagg_tenant_tracking_insert).
 */
TSDLLEXPORT void
ts_cagg_tenant_tracking_insert_only(Relation rel, int32 hypertable_id, text *tenant_id,
									int64 min_timestamp, int64 max_timestamp, int32 seqnum)
{
	TupleDesc desc = RelationGetDescr(rel);
	Datum values[Natts_continuous_aggs_tenant_tracking];
	bool nulls[Natts_continuous_aggs_tenant_tracking] = { false };
	CatalogSecurityContext sec_ctx;
	HeapTuple tuple;

	values[AttrNumberGetAttrOffset(Anum_continuous_aggs_tenant_tracking_hypertable_id)] =
		Int32GetDatum(hypertable_id);
	values[AttrNumberGetAttrOffset(Anum_continuous_aggs_tenant_tracking_tenant_id)] =
		PointerGetDatum(tenant_id);
	values[AttrNumberGetAttrOffset(Anum_continuous_aggs_tenant_tracking_min_timestamp)] =
		Int64GetDatum(min_timestamp);
	values[AttrNumberGetAttrOffset(Anum_continuous_aggs_tenant_tracking_max_timestamp)] =
		Int64GetDatum(max_timestamp);
	values[AttrNumberGetAttrOffset(Anum_continuous_aggs_tenant_tracking_seqnum)] =
		Int32GetDatum(seqnum);

	tuple = heap_form_tuple(desc, values, nulls);
	ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);
	ts_catalog_insert_only(rel, tuple);
	ts_catalog_restore_user(&sec_ctx);
	heap_freetuple(tuple);
}

/*
 * Insert one tenant tracking row, opening the relation and making the row
 * visible to later commands in this transaction by calling CommandCounterIncrement().
 */
TSDLLEXPORT void
ts_cagg_tenant_tracking_insert(int32 hypertable_id, const char *tenant_id, int tenant_id_len,
							   int64 min_timestamp, int64 max_timestamp, int32 seqnum)
{
	Catalog *catalog = ts_catalog_get();
	Relation rel = table_open(catalog_get_table_id(catalog, CONTINUOUS_AGGS_TENANT_TRACKING),
							  RowExclusiveLock);
	text *tenant_text = (text *) palloc(VARHDRSZ + tenant_id_len);

	Ensure(tenant_id_len > 0, "tenant_id length must be > 0");

	SET_VARSIZE(tenant_text, VARHDRSZ + tenant_id_len);
	memcpy(VARDATA(tenant_text), tenant_id, tenant_id_len);

	ts_cagg_tenant_tracking_insert_only(rel,
										hypertable_id,
										tenant_text,
										min_timestamp,
										max_timestamp,
										seqnum);
	/* Make the row visible to later commands in this transaction. */
	CommandCounterIncrement();

	table_close(rel, NoLock);
}

/*
 * Streaming inserter state.  Holds the relation (kept open across the whole
 * batch), the assumed catalog-owner context, and a single reusable varlena
 * buffer for the tenant key that grows only when a longer key appears.
 */
typedef struct CaggTenantTrackingInserter
{
	Relation rel;
	TupleDesc desc;
	CatalogSecurityContext sec_ctx;
	int32 hypertable_id;
	int32 seqnum;
	text *tenant_text; /* reusable key buffer, NULL until first row */
	int buf_capacity;  /* key bytes (excludes VARHDRSZ) tenant_text can hold */
} CaggTenantTrackingInserter;

TSDLLEXPORT CaggTenantTrackingInserter *
ts_cagg_tenant_tracking_insert_begin(int32 hypertable_id, int32 seqnum)
{
	Catalog *catalog = ts_catalog_get();
	CaggTenantTrackingInserter *inserter = palloc0(sizeof(CaggTenantTrackingInserter));

	inserter->rel = table_open(catalog_get_table_id(catalog, CONTINUOUS_AGGS_TENANT_TRACKING),
							   RowExclusiveLock);
	inserter->desc = RelationGetDescr(inserter->rel);
	inserter->hypertable_id = hypertable_id;
	inserter->seqnum = seqnum;

	ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &inserter->sec_ctx);

	return inserter;
}

TSDLLEXPORT void
ts_cagg_tenant_tracking_insert_row(CaggTenantTrackingInserter *inserter, const char *tenant_id,
								   int tenant_id_len, int64 min_timestamp, int64 max_timestamp)
{
	Datum values[Natts_continuous_aggs_tenant_tracking];
	bool nulls[Natts_continuous_aggs_tenant_tracking] = { false };
	HeapTuple tuple;
	Ensure(tenant_id_len > 0, "tenant_id length must be > 0");
	/*
	 * Grow the reusable buffer only when a longer key shows up.  heap_form_tuple
	 * copies the datum into the tuple, so the buffer is safe to overwrite on the
	 * next row.
	 */
	if (tenant_id_len > inserter->buf_capacity)
	{
		if (inserter->tenant_text == NULL)
		{
			inserter->tenant_text = (text *) palloc(VARHDRSZ + tenant_id_len);
		}
		else
		{
			inserter->tenant_text =
				(text *) repalloc(inserter->tenant_text, VARHDRSZ + tenant_id_len);
		}
		inserter->buf_capacity = tenant_id_len;
	}

	SET_VARSIZE(inserter->tenant_text, VARHDRSZ + tenant_id_len);
	memcpy(VARDATA(inserter->tenant_text), tenant_id, tenant_id_len);

	values[AttrNumberGetAttrOffset(Anum_continuous_aggs_tenant_tracking_hypertable_id)] =
		Int32GetDatum(inserter->hypertable_id);
	values[AttrNumberGetAttrOffset(Anum_continuous_aggs_tenant_tracking_tenant_id)] =
		PointerGetDatum(inserter->tenant_text);
	values[AttrNumberGetAttrOffset(Anum_continuous_aggs_tenant_tracking_min_timestamp)] =
		Int64GetDatum(min_timestamp);
	values[AttrNumberGetAttrOffset(Anum_continuous_aggs_tenant_tracking_max_timestamp)] =
		Int64GetDatum(max_timestamp);
	values[AttrNumberGetAttrOffset(Anum_continuous_aggs_tenant_tracking_seqnum)] =
		Int32GetDatum(inserter->seqnum);

	tuple = heap_form_tuple(inserter->desc, values, nulls);
	ts_catalog_insert_only(inserter->rel, tuple);
	heap_freetuple(tuple);
}

TSDLLEXPORT void
ts_cagg_tenant_tracking_insert_end(CaggTenantTrackingInserter *inserter)
{
	if (inserter->tenant_text != NULL)
	{
		pfree(inserter->tenant_text);
	}

	/* Publish all inserted rows to later commands in a single step. */
	CommandCounterIncrement();

	ts_catalog_restore_user(&inserter->sec_ctx);
	table_close(inserter->rel, NoLock);

	pfree(inserter);
}

/*
 * Insert the "invalid" marker row <null, null, null, seqnum>, signalling that
 * tenant tracking for this seqnum is incomplete and the refresh must fall back
 * to the full invalidation log.
 */
TSDLLEXPORT void
ts_cagg_tenant_tracking_insert_invalid_marker(int32 hypertable_id, int32 seqnum)
{
	Catalog *catalog = ts_catalog_get();
	Relation rel = table_open(catalog_get_table_id(catalog, CONTINUOUS_AGGS_TENANT_TRACKING),
							  RowExclusiveLock);
	TupleDesc desc = RelationGetDescr(rel);
	Datum values[Natts_continuous_aggs_tenant_tracking] = { 0 };
	bool nulls[Natts_continuous_aggs_tenant_tracking] = { false };
	CatalogSecurityContext sec_ctx;

	values[AttrNumberGetAttrOffset(Anum_continuous_aggs_tenant_tracking_hypertable_id)] =
		Int32GetDatum(hypertable_id);
	nulls[AttrNumberGetAttrOffset(Anum_continuous_aggs_tenant_tracking_tenant_id)] = true;
	nulls[AttrNumberGetAttrOffset(Anum_continuous_aggs_tenant_tracking_min_timestamp)] = true;
	nulls[AttrNumberGetAttrOffset(Anum_continuous_aggs_tenant_tracking_max_timestamp)] = true;
	values[AttrNumberGetAttrOffset(Anum_continuous_aggs_tenant_tracking_seqnum)] =
		Int32GetDatum(seqnum);

	ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);
	ts_catalog_insert_values(rel, desc, values, nulls);
	ts_catalog_restore_user(&sec_ctx);

	table_close(rel, NoLock);
}

TSDLLEXPORT bool
ts_cagg_tenant_tracking_exists(int32 hypertable_id, int32 seqnum)
{
	bool found = false;
	ScanIterator iterator = ts_scan_iterator_create(CONTINUOUS_AGGS_TENANT_TRACKING,
													AccessShareLock,
													CurrentMemoryContext);

	iterator.ctx.index = catalog_get_index(ts_catalog_get(),
										   CONTINUOUS_AGGS_TENANT_TRACKING,
										   CONTINUOUS_AGGS_TENANT_TRACKING_IDX);
	ts_scan_iterator_scan_key_init(&iterator,
								   Anum_continuous_aggs_tenant_tracking_idx_hypertable_id,
								   BTEqualStrategyNumber,
								   F_INT4EQ,
								   Int32GetDatum(hypertable_id));
	ts_scan_iterator_scan_key_init(&iterator,
								   Anum_continuous_aggs_tenant_tracking_idx_seqnum,
								   BTEqualStrategyNumber,
								   F_INT4EQ,
								   Int32GetDatum(seqnum));

	ts_scanner_foreach(&iterator)
	{
		TupleInfo *ti = ts_scan_iterator_tuple_info(&iterator);
		bool isnull;

		/* A real tracking row has a non-NULL tenant_id; invalid markers do not count. */
		slot_getattr(ti->slot, Anum_continuous_aggs_tenant_tracking_tenant_id, &isnull);
		if (!isnull)
		{
			found = true;
			break;
		}
	}
	ts_scan_iterator_close(&iterator);

	return found;
}

/* Delete all tracking rows for a hypertable. */
TSDLLEXPORT void
ts_cagg_tenant_tracking_delete_by_hypertable_id(int32 hypertable_id)
{
	ScanIterator iterator = ts_scan_iterator_create(CONTINUOUS_AGGS_TENANT_TRACKING,
													RowExclusiveLock,
													CurrentMemoryContext);

	init_scan_by_hypertable_id(&iterator, hypertable_id);

	ts_scanner_foreach(&iterator)
	{
		TupleInfo *ti = ts_scan_iterator_tuple_info(&iterator);

		ts_catalog_delete_tid(ti->scanrel, ts_scanner_get_tuple_tid(ti));
	}
	ts_scan_iterator_close(&iterator);
}
