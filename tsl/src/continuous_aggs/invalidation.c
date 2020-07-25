/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <utils/memutils.h>
#include <utils/palloc.h>
#include <utils/snapmgr.h>
#include <nodes/memnodes.h>
#include <storage/lockdefs.h>
#include <access/htup_details.h>
#include <access/htup.h>
#include <access/xact.h>

#include <catalog.h>
#include <scanner.h>
#include <scan_iterator.h>

#include "continuous_agg.h"
#include "continuous_aggs/materialize.h"
#include "invalidation.h"

typedef struct CaggInvalidationState
{
	ContinuousAgg cagg;
	MemoryContext per_tuple_mctx;
	Relation cagg_log_rel;
	Snapshot snapshot;
} CaggInvalidationState;

typedef enum LogType
{
	LOG_HYPER,
	LOG_CAGG,
} LogType;

static Relation
open_invalidation_log(LogType type, LOCKMODE lockmode)
{
	static const CatalogTable logmappings[] = {
		[LOG_HYPER] = CONTINUOUS_AGGS_HYPERTABLE_INVALIDATION_LOG,
		[LOG_CAGG] = CONTINUOUS_AGGS_MATERIALIZATION_INVALIDATION_LOG,
	};
	Catalog *catalog = ts_catalog_get();
	Oid relid = catalog_get_table_id(catalog, logmappings[type]);

	return table_open(relid, lockmode);
}

static void
cagg_scan_by_hypertable_init(ScanIterator *iterator, int32 hyper_id, LOCKMODE lockmode)
{
	*iterator = ts_scan_iterator_create(CONTINUOUS_AGG, lockmode, CurrentMemoryContext);
	iterator->ctx.index =
		catalog_get_index(ts_catalog_get(), CONTINUOUS_AGG, CONTINUOUS_AGG_RAW_HYPERTABLE_ID_IDX);
	ts_scan_iterator_scan_key_init(iterator,
								   Anum_continuous_agg_raw_hypertable_id_idx_raw_hypertable_id,
								   BTEqualStrategyNumber,
								   F_INT4EQ,
								   Int32GetDatum(hyper_id));
}

/*
 * Get a list of continuous aggregate IDs for a hypertable.
 *
 * Since this is just an integer list, the memory cost is not big even if
 * there are a lot of continuous aggregates.
 */
static List *
get_cagg_ids(int32 hyper_id)
{
	List *cagg_ids = NIL;
	ScanIterator iterator;

	cagg_scan_by_hypertable_init(&iterator, hyper_id, AccessShareLock);

	ts_scanner_foreach(&iterator)
	{
		bool isnull;
		TupleInfo *ti_cagg = ts_scan_iterator_tuple_info(&iterator);
		Datum cagg_hyper_id =
			slot_getattr(ti_cagg->slot, Anum_continuous_agg_mat_hypertable_id, &isnull);
		Assert(!isnull);
		cagg_ids = lappend_int(cagg_ids, DatumGetInt32(cagg_hyper_id));
	}

	ts_scan_iterator_close(&iterator);

	return cagg_ids;
}

static void
hypertable_invalidation_scan_init(ScanIterator *iterator, int32 hyper_id, LOCKMODE lockmode)
{
	*iterator = ts_scan_iterator_create(CONTINUOUS_AGGS_HYPERTABLE_INVALIDATION_LOG,
										lockmode,
										CurrentMemoryContext);
	iterator->ctx.index = catalog_get_index(ts_catalog_get(),
											CONTINUOUS_AGGS_HYPERTABLE_INVALIDATION_LOG,
											CONTINUOUS_AGGS_HYPERTABLE_INVALIDATION_LOG_IDX);
	ts_scan_iterator_scan_key_init(
		iterator,
		Anum_continuous_aggs_hypertable_invalidation_log_idx_hypertable_id,
		BTEqualStrategyNumber,
		F_INT4EQ,
		Int32GetDatum(hyper_id));
}

static HeapTuple
create_invalidation_tup(const TupleDesc tupdesc, int32 cagg_hyper_id, int64 modtime, int64 start,
						int64 end)
{
	Datum values[Natts_continuous_aggs_materialization_invalidation_log] = { 0 };
	bool isnull[Natts_continuous_aggs_materialization_invalidation_log] = { false };

	values[AttrNumberGetAttrOffset(
		Anum_continuous_aggs_materialization_invalidation_log_materialization_id)] =
		Int32GetDatum(cagg_hyper_id);
	values[AttrNumberGetAttrOffset(
		Anum_continuous_aggs_materialization_invalidation_log_modification_time)] =
		Int64GetDatum(modtime);
	values[AttrNumberGetAttrOffset(
		Anum_continuous_aggs_materialization_invalidation_log_lowest_modified_value)] =
		Int64GetDatum(start);
	values[AttrNumberGetAttrOffset(
		Anum_continuous_aggs_materialization_invalidation_log_greatest_modified_value)] =
		Int64GetDatum(end);

	return heap_form_tuple(tupdesc, values, isnull);
}

typedef enum InvalidationResult
{
	INVAL_NOMATCH,
	INVAL_DELETE,
	INVAL_CUT,
} InvalidationResult;

/*
 * Try to cut an invalidation against the refresh window.
 *
 * If an invalidation entry overlaps with the refresh window, it needs
 * additional processing: it is either cut, deleted, or left unmodified.
 */
static InvalidationResult
cut_invalidation_along_refresh_window(const CaggInvalidationState *state, int64 modification_time,
									  int64 lowest_modified_value, int64 greatest_modified_value,
									  const InternalTimeRange *refresh_window,
									  const ItemPointer tid)
{
	int32 cagg_hyper_id = state->cagg.data.mat_hypertable_id;
	TupleDesc tupdesc = RelationGetDescr(state->cagg_log_rel);
	InvalidationResult result = INVAL_NOMATCH;
	HeapTuple lower = NULL;
	HeapTuple upper = NULL;

	/* Entry is completely enclosed by the refresh window */
	if (lowest_modified_value >= refresh_window->start &&
		greatest_modified_value < refresh_window->end)
	{
		/*
		 * Entry completely enclosed so can be deleted:
		 *
		 * |---------------|
		 *     [+++++]
		 */

		result = INVAL_DELETE;
	}
	else
	{
		if (lowest_modified_value < refresh_window->start &&
			greatest_modified_value >= refresh_window->start)
		{
			/*
			 * Need to cut in right end:
			 *
			 *     |------|
			 * [++++++]
			 *
			 * [++]
			 */
			lower = create_invalidation_tup(tupdesc,
											cagg_hyper_id,
											modification_time,
											lowest_modified_value,
											refresh_window->start - 1);
			result = INVAL_CUT;
		}

		if (lowest_modified_value < refresh_window->end &&
			greatest_modified_value >= refresh_window->end)
		{
			/*
			 * Need to cut in left end:
			 *
			 * |------|
			 *    [++++++++]
			 *
			 *         [+++]
			 */
			upper = create_invalidation_tup(tupdesc,
											cagg_hyper_id,
											modification_time,
											refresh_window->end,
											greatest_modified_value);

			result = INVAL_CUT;
		}
	}

	/* Insert any modifications into the cagg invalidation log */
	if (result == INVAL_CUT)
	{
		CatalogSecurityContext sec_ctx;
		HeapTuple other_range = NULL;

		ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);

		/* We'd like to do one update (unless the TID is not set), and
		 * optionally one insert. We pick one of the tuples for an update, and
		 * the other one will be an insert. */
		if (lower || upper)
		{
			HeapTuple tup = lower ? lower : upper;
			other_range = lower ? upper : lower;

			/* If the TID is set, we are updating an existing tuple, i.e., we
			 * are processing and entry in the cagg log itself. Otherwise, we
			 * are processing the hypertable invalidation log and need to
			 * insert a new entry. */
			if (tid)
				ts_catalog_update_tid_only(state->cagg_log_rel, tid, tup);
			else
				ts_catalog_insert_only(state->cagg_log_rel, tup);

			heap_freetuple(tup);
		}

		if (other_range)
		{
			ts_catalog_insert_only(state->cagg_log_rel, other_range);
			heap_freetuple(other_range);
		}

		ts_catalog_restore_user(&sec_ctx);
	}

	return result;
}

/*
 * Process invalidations in the hypertable invalidation log.
 *
 * Copy and delete all entries from the hypertable invalidation log.  For the
 * continuous aggregate that is getting refreshed, we also match the
 * invalidation against the refresh window and perform additional processing
 * (cutting or deleting); work that we'd otherwise have to do later in the
 * cagg invalidation log.
 *
 * Note that each entry gets one copy per continuous aggregate in the cagg
 * invalidation log (unless it matched the refresh window). These copied
 * entries are later used to track invalidations across refreshes on a
 * per-cagg basis.
 *
 * After this function has run, there are no entries left in the hypertable
 * invalidation log.
 */
static void
move_invalidations_from_hyper_to_cagg_log(const CaggInvalidationState *state,
										  const InternalTimeRange *refresh_window)
{
	int32 hyper_id = state->cagg.data.raw_hypertable_id;
	List *cagg_ids = get_cagg_ids(hyper_id);
	int32 last_cagg_hyper_id = llast_int(cagg_ids);
	ScanIterator iterator;
	ListCell *lc;

	Assert(list_length(cagg_ids) > 0);

	/* We use a per-tuple memory context in the scan loop since we could be
	 * processing a lot of invalidations (basically an unbounded
	 * amount). Initialize it here by resetting it. */
	MemoryContextReset(state->per_tuple_mctx);
	hypertable_invalidation_scan_init(&iterator, hyper_id, RowExclusiveLock);
	iterator.ctx.snapshot = state->snapshot;

	/*
	 * Looping over all continuous aggregates in the outer loop ensures all
	 * tuples for a specific continuous aggregate is inserted consecutively in
	 * the cagg invalidation log. This creates better locality for scanning
	 * the invalidations later.
	 */
	foreach (lc, cagg_ids)
	{
		int32 cagg_hyper_id = lfirst_int(lc);

		/* Scan all invalidations */
		ts_scanner_foreach(&iterator)
		{
			TupleInfo *ti;
			Form_continuous_aggs_hypertable_invalidation_log form;
			CatalogSecurityContext sec_ctx;
			MemoryContext oldmctx;
			bool should_free;
			HeapTuple tuple;
			bool should_insert = false;

			oldmctx = MemoryContextSwitchTo(state->per_tuple_mctx);
			ti = ts_scan_iterator_tuple_info(&iterator);
			tuple = ts_scan_iterator_fetch_heap_tuple(&iterator, false, &should_free);
			form = (Form_continuous_aggs_hypertable_invalidation_log) GETSTRUCT(tuple);

			/* If we're processing an invalidation for the continuous
			 * aggregate that is getting refreshed, then we can cut or delete
			 * the invalidation immediately, instead of doing it later in the
			 * cagg invalidation log. */
			if (cagg_hyper_id == state->cagg.data.mat_hypertable_id)
			{
				InvalidationResult result;

				result = cut_invalidation_along_refresh_window(state,
															   form->modification_time,
															   form->lowest_modified_value,
															   form->greatest_modified_value,
															   refresh_window,
															   NULL);

				switch (result)
				{
					case INVAL_CUT:
					case INVAL_DELETE:
						/* Work already done, so nothing more to do. */
						break;
					case INVAL_NOMATCH:
						/* The tuple didn't match the refresh window, so we need
						 * to copy this entry over to the cagg invalidation log. */
						should_insert = true;
						break;
				}
			}
			else
			{
				/* We aren't refreshing this continuous aggregate, so the only
				 * work to do is to insert a new entry in the cagg
				 * invalidation log. */
				should_insert = true;
			}

			if (should_insert)
			{
				TupleDesc tupdesc = RelationGetDescr(state->cagg_log_rel);
				HeapTuple newtup;

				newtup = create_invalidation_tup(tupdesc,
												 cagg_hyper_id,
												 form->modification_time,
												 form->lowest_modified_value,
												 form->greatest_modified_value);

				ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);
				ts_catalog_insert_only(state->cagg_log_rel, newtup);
				ts_catalog_restore_user(&sec_ctx);
			}

			if (cagg_hyper_id == last_cagg_hyper_id)
			{
				/* The invalidation has been processed for all caggs, so the
				 * only thing left is to delete it from the source hypertable
				 * invalidation log. */
				ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);
				ts_catalog_delete_only(ti->scanrel, tuple);
				ts_catalog_restore_user(&sec_ctx);
			}

			if (should_free)
				heap_freetuple(tuple);

			MemoryContextSwitchTo(oldmctx);
			MemoryContextReset(state->per_tuple_mctx);
		}
	}

	ts_scan_iterator_close(&iterator);
}

static void
cagg_invalidations_scan_by_hypertable_init(ScanIterator *iterator, int32 cagg_hyper_id,
										   LOCKMODE lockmode)
{
	*iterator = ts_scan_iterator_create(CONTINUOUS_AGGS_MATERIALIZATION_INVALIDATION_LOG,
										lockmode,
										CurrentMemoryContext);
	iterator->ctx.index = catalog_get_index(ts_catalog_get(),
											CONTINUOUS_AGGS_MATERIALIZATION_INVALIDATION_LOG,
											CONTINUOUS_AGGS_MATERIALIZATION_INVALIDATION_LOG_IDX);
	ts_scan_iterator_scan_key_init(
		iterator,
		Anum_continuous_aggs_materialization_invalidation_log_materialization_id,
		BTEqualStrategyNumber,
		F_INT4EQ,
		Int32GetDatum(cagg_hyper_id));
}
/*
 * Clear all cagg invalidations that match a refresh window.
 *
 * This function clears all invalidations in the cagg invalidation log that
 * matches a window. Note that the refresh currently doesn't make use of the
 * invalidations to optimize the materialization.
 *
 * An invalidation entry that gets processed is either completely enclosed
 * (covered) by the refresh window, or it partially overlaps. In the former
 * case, the invalidation entry is removed and for the latter case it is
 * cut. Thus, an entry can either disappear, reduce in size, or be cut in two.
 *
 * Note that the refresh window is inclusive at the start and exclusive at the
 * end.
 */
static void
clear_cagg_invalidations_for_refresh(const CaggInvalidationState *state,
									 const InternalTimeRange *refresh_window)
{
	ScanIterator iterator;
	int32 cagg_hyper_id = state->cagg.data.mat_hypertable_id;

	/* The scanner defaults to SnapshotSelf, which would show the
	 * modifications we do (new and updated tuples) within the scan loop
	 * below. Therefore, we use our own snapshot here. */
	cagg_invalidations_scan_by_hypertable_init(&iterator, cagg_hyper_id, RowExclusiveLock);
	iterator.ctx.snapshot = state->snapshot;
	MemoryContextReset(state->per_tuple_mctx);

	/* Process all invalidations for the continuous aggregate */
	ts_scanner_foreach(&iterator)
	{
		Form_continuous_aggs_materialization_invalidation_log form;
		MemoryContext oldmctx;
		InvalidationResult result;
		bool should_free;
		HeapTuple tuple;

		oldmctx = MemoryContextSwitchTo(state->per_tuple_mctx);
		tuple = ts_scan_iterator_fetch_heap_tuple(&iterator, false, &should_free);
		form = (Form_continuous_aggs_materialization_invalidation_log) GETSTRUCT(tuple);
		result = cut_invalidation_along_refresh_window(state,
													   form->modification_time,
													   form->lowest_modified_value,
													   form->greatest_modified_value,
													   refresh_window,
													   &tuple->t_self);

		switch (result)
		{
			case INVAL_NOMATCH:
			case INVAL_CUT:
				/* Nothing to do */
				break;
			case INVAL_DELETE:
				ts_catalog_delete_tid_only(state->cagg_log_rel, &tuple->t_self);
				break;
		}

		MemoryContextSwitchTo(oldmctx);
		MemoryContextReset(state->per_tuple_mctx);
	}

	ts_scan_iterator_close(&iterator);
}

void
continuous_agg_invalidation_process(const ContinuousAgg *cagg,
									const InternalTimeRange *refresh_window)
{
	CaggInvalidationState state = {
		.cagg = *cagg,
		.cagg_log_rel = open_invalidation_log(LOG_CAGG, RowExclusiveLock),
		.per_tuple_mctx = AllocSetContextCreate(CurrentMemoryContext,
												"Continuous aggregate invalidations",
												ALLOCSET_DEFAULT_SIZES),
		.snapshot = RegisterSnapshot(GetTransactionSnapshot()),
	};

	/* Clear the cagg invalidation log first, so that we don't need to
	 * unnecessarily scan any hew entries that we might insert when we move
	 * invalidations from the hypertable invalidation log. */
	clear_cagg_invalidations_for_refresh(&state, refresh_window);
	move_invalidations_from_hyper_to_cagg_log(&state, refresh_window);
	table_close(state.cagg_log_rel, NoLock);
	UnregisterSnapshot(state.snapshot);
	MemoryContextDelete(state.per_tuple_mctx);
}
