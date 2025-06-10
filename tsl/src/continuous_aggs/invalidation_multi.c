/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

/*
 * Module for processing invalidations for multiple hypertables in one single
 * pass. It will build up a multi-range for each continuous aggregate using
 * the internal time type.
 */

#include "invalidation_multi.h"

#include <postgres.h>

#include <access/tupdesc.h>
#include <catalog/pg_type_d.h>
#include <executor/spi.h>
#include <lib/stringinfo.h>
#include <nodes/pg_list.h>
#include <postgres_ext.h>
#include <replication/logicalproto.h>
#include <utils/array.h>
#include <utils/palloc.h>
#include <utils/rangetypes.h>
#include <utils/typcache.h>

#include "continuous_aggs/invalidation.h"
#include "continuous_aggs/invalidation_record.h"
#include "continuous_aggs/invalidation_threshold.h"
#include "guc.h"
#include "ts_catalog/continuous_agg.h"

void
multi_invalidation_state_init(MultiInvalidationState *state, const char *slot_name,
							  MemoryContext mcxt)
{
	state->hash_context =
		AllocSetContextCreate(mcxt, "InvalidationsHashContext", ALLOCSET_DEFAULT_SIZES);

	state->proc_context =
		AllocSetContextCreate(mcxt, "InvalidationsProcessingContext", ALLOCSET_DEFAULT_SIZES);

	HASHCTL hypertables_ctl = { .keysize = sizeof(int32),
								.entrysize = sizeof(MultiInvalidationEntry),
								.hcxt = state->hash_context };
	HASHCTL ranges_ctl = { .keysize = sizeof(int32),
						   .entrysize = sizeof(MultiInvalidationRangeEntry),
						   .hcxt = state->hash_context };

	state->hypertables = hash_create("Multi-invalidation hypertables cache",
									 32,
									 &hypertables_ctl,
									 HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	state->ranges = hash_create("Multi-invalidation continuous aggregate ranges cache",
								32,
								&ranges_ctl,
								HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	Catalog *const catalog = ts_catalog_get();
	Oid relid = catalog_get_table_id(catalog, CONTINUOUS_AGGS_MATERIALIZATION_INVALIDATION_LOG);
	state->logrel = table_open(relid, RowExclusiveLock);
	state->typecache = lookup_type_cache(INT8MULTIRANGEOID, TYPECACHE_MULTIRANGE_INFO);
	namestrcpy(&state->slot_name, slot_name);
}

void
multi_invalidation_state_cleanup(MultiInvalidationState *state)
{
	hash_destroy(state->ranges);
	hash_destroy(state->hypertables);
	table_close(state->logrel, NoLock);
	MemoryContextDelete(state->hash_context);
	MemoryContextDelete(state->proc_context);
}

static void
multi_invalidation_range_entry_init(MultiInvalidationRangeEntry *entry, int32 materialization_id,
									ContinuousAggBucketFunction *bucket_function)
{
	entry->materialization_id = materialization_id;
	entry->bucket_function = bucket_function;
	entry->ranges_count = 0;
	entry->ranges_alloc = 256; /* Starting count, but will increase */
	entry->ranges = palloc0(entry->ranges_alloc * sizeof(RangeType *));
}

/*
 * Initialize the hypertable invalidation entry.
 *
 * Returns all associated continuous aggregates that are using WAL to collect
 * invalidations.
 */
static List *
multi_invalidation_hypertable_entry_init(MultiInvalidationEntry *entry, int32 hypertable_id)
{
	const Hypertable *const ht = ts_hypertable_get_by_id(hypertable_id);
	const Dimension *const dim = hyperspace_get_open_dimension(ht->space, 0);
	ListCell *lc;

	Ensure(!OidIsValid(get_trigger_oid(ht->main_table_relid, CAGGINVAL_TRIGGER_NAME, true)),
		   "hypertable has invalidation trigger, which was not expected");

	entry->hypertable_id = hypertable_id;
	entry->dimtype = ts_dimension_get_partition_type(dim);
	entry->caggs = NIL;

	/*
	 * We can add all continuous aggregates for the hypertable since they are
	 * using the WAL-based invalidation collection method.
	 */
	List *caggs = ts_continuous_aggs_find_by_raw_table_id(hypertable_id);
	foreach (lc, caggs)
	{
		ContinuousAgg *cagg = lfirst(lc);
		entry->caggs = lappend_int(entry->caggs, cagg->data.mat_hypertable_id);
	}

	return caggs;
}

/*
 * Fetch a hypertable entry for update
 *
 * This will fetch the hypertable entry for the given hypertable, and
 * construct it if it was not there.
 *
 * Returns a pointer to the entry that was either found or added.
 */
MultiInvalidationEntry *
multi_invalidation_state_hypertable_entry_get_for_update(MultiInvalidationState *state,
														 int32 hypertable_id)
{
	bool hypertables_entry_found;
	MultiInvalidationEntry *hypertables_entry =
		hash_search(state->hypertables, &hypertable_id, HASH_ENTER, &hypertables_entry_found);

	/* If hypertable entry was not found, initialize it. */
	if (!hypertables_entry_found)
	{
		List *caggs = multi_invalidation_hypertable_entry_init(hypertables_entry, hypertable_id);

		/* We know that the continuous aggregates for the hypertable were not
		 * in the cache since the hypertable was not in the cache, so let's
		 * write the associated continuous aggregates (that are all using the
		 * WAL) to the ranges cache at this point. */
		ListCell *lc;
		foreach (lc, caggs)
		{
			ContinuousAgg *cagg = lfirst(lc);
			int32 materialization_id = cagg->data.mat_hypertable_id;
			bool ranges_entry_found;
			MultiInvalidationRangeEntry *ranges_entry =
				hash_search(state->ranges, &materialization_id, HASH_ENTER, &ranges_entry_found);

			Assert(!ranges_entry_found);
			multi_invalidation_range_entry_init(ranges_entry,
												materialization_id,
												cagg->bucket_function);
		}
	}

	Assert(hypertables_entry != NULL);
	return hypertables_entry;
}

void
multi_invalidation_range_add(MultiInvalidationState *state, const Invalidation *invalidation)
{
	MultiInvalidationEntry *hypertables_entry =
		multi_invalidation_state_hypertable_entry_get_for_update(state, invalidation->hyper_id);
	TypeCacheEntry *typcache = lookup_type_cache(INT8RANGEOID, TYPECACHE_RANGE_INFO);

	MemoryContext old_context = MemoryContextSwitchTo(state->hash_context);

	TS_DEBUG_LOG("CurrentMemoryContext: %s", CurrentMemoryContext->name);

	/* Iterate over all associated caggs of the hypertable, expand the range
	 * to the bucket boundaries for the continuous aggregate, and add it to
	 * the ranges to build. */
	ListCell *lc;
	foreach (lc, hypertables_entry->caggs)
	{
		const int32 materialized_id = lfirst_int(lc);

		/* We copy all the fields of the invalidation, but modify the hypertable id to be the id of
		 * the materialization table. */
		Invalidation cagg_invalidation = *invalidation;
		cagg_invalidation.hyper_id = materialized_id;

		bool ranges_entry_found;
		MultiInvalidationRangeEntry *ranges_entry =
			hash_search(state->ranges, &materialized_id, HASH_FIND, &ranges_entry_found);

		/* The continuous aggregate should be in the table since we added it when we added the
		 * hypertable. If it is not there, something is wrong. */
		Assert(ranges_entry_found);

		/* Check invariant for processing to make sure that we do not write
		 * outside allocated memory. */
		Assert(ranges_entry->ranges_count < ranges_entry->ranges_alloc);

		invalidation_expand_to_bucket_boundaries(&cagg_invalidation,
												 hypertables_entry->dimtype,
												 ranges_entry->bucket_function);

		/*
		 * Add the new range to the list.
		 *
		 * Ranges in the materialization log are int64, just like in the
		 * hypertable invalidation log, so we use INT8RANGE for those.
		 *
		 * Both the upper and the lower bounds are inclusive for invalidations
		 * that we store.
		 */
		RangeBound lbound = {
			.val = Int64GetDatum(cagg_invalidation.lowest_modified_value),
			.infinite = (cagg_invalidation.lowest_modified_value == INVAL_NEG_INFINITY),
			.inclusive = true,
			.lower = true,
		};
		RangeBound ubound = {
			.val = Int64GetDatum(cagg_invalidation.greatest_modified_value),
			.infinite = (cagg_invalidation.greatest_modified_value == INVAL_POS_INFINITY),
			.inclusive = true,
			.lower = false,
		};

		RangeType *newrange = make_range_compat(typcache, &lbound, &ubound, false, NULL);
		ranges_entry->ranges[ranges_entry->ranges_count++] = newrange;

		TS_DEBUG_LOG("adding range %s for hypertable %d to invalidation state",
					 datum_as_string(INT8RANGEOID, RangeTypePGetDatum(newrange), false),
					 ranges_entry->materialization_id);

		/* Double the capacity if we have hit the ceiling */
		if (ranges_entry->ranges_count >= ranges_entry->ranges_alloc)
		{
			ranges_entry->ranges_alloc *= 2;
			ranges_entry->ranges =
				repalloc(ranges_entry->ranges, ranges_entry->ranges_alloc * sizeof(RangeType *));
		}

		Assert(ranges_entry->ranges_count < ranges_entry->ranges_alloc);
	}

	MemoryContextSwitchTo(old_context);
}

/*
 * Write the aggregated ranges to the log.
 *
 * This will first create a multi-range from all the collected ranges to
 * eliminate duplicates and merge adjacent ranges, and then write all the
 * ranges to the materialization log.
 */
void
multi_invalidation_range_write(MultiInvalidationState *state, MultiInvalidationRangeEntry *entry)
{
	int32 range_count;
	RangeType **ranges;
	TupleDesc tupdesc = RelationGetDescr(state->logrel);
	TypeCacheEntry *typcache = lookup_type_cache(INT8MULTIRANGEOID, TYPECACHE_MULTIRANGE_INFO);

	MemoryContext old_context = MemoryContextSwitchTo(state->proc_context);

	TS_DEBUG_LOG("CurrentMemoryContext: %s", CurrentMemoryContext->name);

	/* Create a multirange from the collected ranges. This will also
	 * combine and merge any overlapping entries. */
	MultirangeType *multirange =
		make_multirange(INT8MULTIRANGEOID, typcache->rngtype, entry->ranges_count, entry->ranges);

	/* Deserialize the multirange to a sequence of ranges and write them
	 * to the materialization log table. */
	multirange_deserialize(typcache->rngtype, multirange, &range_count, &ranges);
	for (int i = 0; i < range_count; i++)
	{
		bool empty;
		RangeBound lower, upper;

		/* Deserialize the range to get the lower and upper bound. */
		range_deserialize(typcache->rngtype, ranges[i], &lower, &upper, &empty);

		/* There should be no empty ranges returned, but if there is, we just ignore them in
		 * release builds. */
		Assert(!empty);
		if (!empty)
		{
			CatalogSecurityContext sec_ctx;
			HeapTuple newtup = create_invalidation_tup(tupdesc,
													   entry->materialization_id,
													   lower.infinite ? INVAL_NEG_INFINITY :
																		DatumGetInt64(lower.val),
													   upper.infinite ? INVAL_POS_INFINITY :
																		DatumGetInt64(upper.val));
			ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);
			ts_catalog_insert_only(state->logrel, newtup);
			ts_catalog_restore_user(&sec_ctx);
		}
	}
	MemoryContextSwitchTo(old_context);
	MemoryContextReset(state->proc_context);
}

void
multi_invalidation_range_write_all(MultiInvalidationState *state)
{
	HASH_SEQ_STATUS hash_seq;
	MultiInvalidationRangeEntry *entry;

	hash_seq_init(&hash_seq, state->ranges);
	while ((entry = hash_seq_search(&hash_seq)) != NULL)
		multi_invalidation_range_write(state, entry);
}
