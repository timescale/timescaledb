/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include "invalidation_multi.h"

#include <postgres.h>

#include "continuous_aggs/invalidation.h"
#include <catalog/pg_type_d.h>
#include <nodes/pg_list.h>
#include <postgres_ext.h>
#include <utils/palloc.h>
#include <utils/rangetypes.h>
#include <utils/typcache.h>

static RangeType *
make_range_internal_time(int64 lower, int64 upper, Oid dimtype)
{
	Assert(lower < upper);
	RangeBound lbound = {
		.val = lower,
		.infinite = TS_TIME_IS_NOBEGIN(lower, dimtype),
		.inclusive = false,
		.lower = true,
	};
	RangeBound ubound = {
		.val = upper,
		.infinite = TS_TIME_IS_NOEND(upper, dimtype),
		.inclusive = false,
		.lower = false,
	};
	TypeCacheEntry *typcache = lookup_type_cache(INT8RANGEOID, TYPECACHE_RANGE_INFO);
#if PG16_LT
	return make_range(typcache, &lbound, &ubound, false);
#else
	return make_range(typcache, &lbound, &ubound, false, NULL);
#endif
}

void
multi_invalidation_state_init(MultiHypertableInvalidationState *state, MemoryContext mcxt)
{
	state->mcxt = AllocSetContextCreate(mcxt,
										"Multi-materialization invalidations processing",
										ALLOCSET_DEFAULT_SIZES);

	HASHCTL hypertables_ctl = { .keysize = sizeof(int32),
								.entrysize = sizeof(MultiHypertableInvalidationEntry),
								.hcxt = mcxt };
	state->hypertables = hash_create("multi-invalidation hypertables cache",
									 32,
									 &hypertables_ctl,
									 HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	HASHCTL ranges_ctl = { .keysize = sizeof(int32),
						   .entrysize = sizeof(MultiHypertableInvalidationRangeEntry),
						   .hcxt = mcxt };
	state->ranges = hash_create("multi-invalidation continuous aggregate ranges cache",
								32,
								&ranges_ctl,
								HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	Oid relid =
		catalog_get_table_id(ts_catalog_get(), CONTINUOUS_AGGS_MATERIALIZATION_INVALIDATION_LOG);
	state->cagg_log = table_open(relid, RowExclusiveLock);
}

void
multi_invalidation_state_cleanup(MultiHypertableInvalidationState *state)
{
	hash_destroy(state->ranges);
	hash_destroy(state->hypertables);
	table_close(state->cagg_log, NoLock);
	MemoryContextDelete(state->mcxt);
}

/*
 * Get multirange type for a type.
 *
 * Right now this is hard-coded, but we should probably look it up in pg_range
 * and cache it. Current syscache does not support looking up multirange type
 * (or range type) for a particular element type.
 */
static Oid
get_multirange_type(Oid elemtyp)
{
	switch (elemtyp)
	{
		case TIMESTAMPOID:
			return TSMULTIRANGEOID;
		case TIMESTAMPTZOID:
			return TSTZMULTIRANGEOID;
		case INT8OID:
			return INT8MULTIRANGEOID;
		case INT4OID:
			return INT4MULTIRANGEOID;
		case DATEOID:
			return DATEMULTIRANGEOID;
		default:
			return InvalidOid;
	}
}

static List *
multi_invalidation_hypertable_entry_init(MultiHypertableInvalidationEntry *entry,
										 int32 hypertable_id)
{
	const Hypertable *const ht = ts_hypertable_get_by_id(hypertable_id);
	const Dimension *const dim = hyperspace_get_open_dimension(ht->space, 0);
	List *caggs = ts_continuous_aggs_find_by_raw_table_id(hypertable_id);
	ListCell *lc;

	entry->hypertable_id = hypertable_id;
	entry->dimtype = ts_dimension_get_partition_type(dim);
	entry->typcache =
		lookup_type_cache(get_multirange_type(entry->dimtype), TYPECACHE_MULTIRANGE_INFO);

	foreach (lc, caggs)
	{
		ContinuousAgg *cagg = lfirst(lc);
		entry->caggs = lappend_int(entry->caggs, cagg->data.mat_hypertable_id);
	}

	return caggs;
}

void
multi_invalidation_add_range(MultiHypertableInvalidationState *state, int32 hypertable_id,
							 int64 lower, int64 upper)
{
	bool hypertables_entry_found;
	MultiHypertableInvalidationEntry *hypertables_entry =
		hash_search(state->hypertables, &hypertable_id, HASH_ENTER, &hypertables_entry_found);

	/* If hypertable entry was not found, initialize it. */
	if (!hypertables_entry_found)
	{
		List *caggs = multi_invalidation_hypertable_entry_init(hypertables_entry, hypertable_id);

		/* We know that the continuous aggregates for the hypertable were not
		 * in the cache since the hypertable was not in the cache, so let's
		 * write the associated continuous aggregates to the ranges cache at
		 * this point. */
		ListCell *lc;
		foreach (lc, caggs)
		{
			ContinuousAgg *cagg = lfirst(lc);
			int32 materialization_id = cagg->data.mat_hypertable_id;
			bool ranges_entry_found;
			MultiHypertableInvalidationRangeEntry *ranges_entry =
				hash_search(state->ranges, &materialization_id, HASH_ENTER, &ranges_entry_found);
			Assert(!ranges_entry_found);
			ranges_entry->materialization_id = materialization_id;
			ranges_entry->bucket_function = cagg->bucket_function;
			ranges_entry->ranges_count = 0;
			ranges_entry->ranges_alloc = 256;
			ranges_entry->ranges = palloc0(ranges_entry->ranges_alloc * sizeof(RangeType *));
			ranges_entry->typcache = hypertables_entry->typcache;
		}
	}

	Oid dimtype = hypertables_entry->dimtype;

	/* Iterate over all associated caggs of the hypertable. */
	ListCell *lc;
	foreach (lc, hypertables_entry->caggs)
	{
		const int32 materialized_id = lfirst_int(lc);
		Invalidation invalidation = {
			.lowest_modified_value = lower,
			.greatest_modified_value = upper,
			.hyper_id = materialized_id,
		};

		bool ranges_entry_found;
		MultiHypertableInvalidationRangeEntry *ranges_entry =
			hash_search(state->ranges, &materialized_id, HASH_FIND, &ranges_entry_found);

		Assert(ranges_entry_found);

		invalidation_expand_to_bucket_boundaries(&invalidation,
												 dimtype,
												 ranges_entry->bucket_function);

		/* Add the new range to the list */
		RangeType *newrange = make_range_internal_time(invalidation.lowest_modified_value,
													   invalidation.greatest_modified_value,
													   dimtype);
		ranges_entry->ranges[ranges_entry->ranges_count++] = newrange;

		/* Double the capacity if we have hit the ceiling. */
		if (ranges_entry->ranges_alloc == ranges_entry->ranges_count)
		{
			ranges_entry->ranges_alloc *= 2;
			ranges_entry->ranges =
				repalloc(ranges_entry->ranges, ranges_entry->ranges_alloc * sizeof(RangeType *));
		}
	}
}

/*
 * Write a range entry for a materialization to the log table.
 */
void
multi_invalidation_write_range_entry(MultiHypertableInvalidationState *state,
									 MultiHypertableInvalidationRangeEntry *entry)
{
	int32 range_count;
	RangeType **ranges;
	TupleDesc tupdesc = RelationGetDescr(state->cagg_log);

	/* Create a multirange from the collected ranges. This will also
	 * combine and merge any overlapping entries. */
	MultirangeType *multirange = make_multirange(entry->typcache->type_id,
												 entry->typcache->rngtype,
												 entry->ranges_count,
												 entry->ranges);

	/* Deserialize the multirange to a sequence of ranges and write them
	 * to the materialization log table. */
	multirange_deserialize(entry->typcache->rngtype, multirange, &range_count, &ranges);
	for (int i = 0; i < range_count; i++)
	{
		bool empty;
		RangeBound lower, upper;

		/* Deserialize the range to get the lower and upper bound. */
		range_deserialize(entry->typcache->rngtype, ranges[i], &lower, &upper, &empty);

		/* There should be no empty ranges returned, but if there is, we just ignore them in
		 * release builds. */
		Assert(!empty);
		if (!empty)
		{
			CatalogSecurityContext sec_ctx;
			HeapTuple newtup =
				create_invalidation_tup(tupdesc,
										entry->materialization_id,
										lower.infinite ? INVAL_NEG_INFINITY : lower.val,
										upper.infinite ? INVAL_POS_INFINITY : upper.val);
			ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);
			ts_catalog_insert_only(state->cagg_log, newtup);
			ts_catalog_restore_user(&sec_ctx);
		}
	}
}

void
multi_invalidation_write_all(MultiHypertableInvalidationState *state)
{
	HASH_SEQ_STATUS hash_seq;
	MultiHypertableInvalidationRangeEntry *entry;

	/* Iterate all the range entries and write the materialization to the
	 * log. */
	hash_seq_init(&hash_seq, state->ranges);
	while ((entry = hash_seq_search(&hash_seq)) != NULL)
		multi_invalidation_write_range_entry(state, entry);
}
