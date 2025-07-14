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
	entry->ranges_alloc = 16; /* Starting count, but will increase */
	entry->ranges = palloc0(entry->ranges_alloc * sizeof(RangeType *));
}

/*
 * Initialize the hypertable invalidation entry.
 *
 * Returns all associated continuous aggregates that are using WAL to collect
 * invalidations.
 */
static List *
multi_invalidation_hypertable_entry_init(MultiInvalidationState *state,
										 MultiInvalidationEntry *entry, int32 hypertable_id)
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
	 * using the WAL-based invalidation collection method and we expect to
	 * start writing to them. We allocate these on the hash context.
	 */
	MemoryContext old_context = MemoryContextSwitchTo(state->hash_context);
	List *caggs = ts_continuous_aggs_find_by_raw_table_id(hypertable_id);
	foreach (lc, caggs)
	{
		ContinuousAgg *cagg = lfirst(lc);
		entry->caggs = lappend_int(entry->caggs, cagg->data.mat_hypertable_id);
	}
	MemoryContextSwitchTo(old_context);
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
		List *caggs =
			multi_invalidation_hypertable_entry_init(state, hypertables_entry, hypertable_id);

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

		/*
		 * Make sure the range is allocated in the range entry context and add
		 * it to the array of ranges.
		 */
		RangeType *newrange =
			make_range_compat(state->typecache->rngtype, &lbound, &ubound, false, NULL);
		ranges_entry->ranges[ranges_entry->ranges_count++] = newrange;

		TS_DEBUG_LOG("adding range %s for hypertable %d to invalidation state",
					 datum_as_string(INT8RANGEOID, RangeTypePGetDatum(newrange), false),
					 ranges_entry->materialization_id);

		/*
		 * Double the capacity if we have hit the ceiling. This will extend
		 * the array in the range memory context.
		 */
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

/*
 * Get array of hypertables to collect changes.
 *
 * The array consists of interleaving table names and attribute names. The
 * attribute name is the name of the primary partition column for the table.

 * This array is intended to be used as the variadic array for
 * pg_logical_slot_get_binary_changes().
 */
static ArrayType *
get_array_for_changes(MultiInvalidationState *state)
{
	HASH_SEQ_STATUS hash_seq;
	MultiInvalidationEntry *entry;
	const long tables_count = hash_get_num_entries(state->hypertables);
	const size_t values_count = 2 * tables_count;
	Datum *values = palloc(values_count * sizeof(Datum));

	hash_seq_init(&hash_seq, state->hypertables);
	for (int i = 0; (entry = hash_seq_search(&hash_seq)) != NULL; i += 2)
	{
		const Hypertable *ht = ts_hypertable_get_by_id(entry->hypertable_id);
		const Dimension *dim = hyperspace_get_open_dimension(ht->space, 0);
		const char *qual_table_name =
			quote_qualified_identifier(NameStr(ht->fd.schema_name), NameStr(ht->fd.table_name));
		const char *column_name = NameStr(dim->fd.column_name);
		TS_DEBUG_LOG("hypertable=%s, attribute=%s", qual_table_name, column_name);
		values[i + 0] = PointerGetDatum(cstring_to_text(qual_table_name));
		values[i + 1] = PointerGetDatum(cstring_to_text(column_name));
	}

	/* Not sure we need the array of values after this. It seems like the
	 * datums are actually copied, but it is not entirely clear. */
#if PG16_GE
	return construct_array_builtin(values, values_count, TEXTOID);
#else
	return construct_array(values, values_count, TEXTOID, -1, false, TYPALIGN_INT);
#endif
}

/*
 * Move invalidations for multiple hypertables in one pass.
 *
 * We process the invalidation in batches to avoid eating up too much
 * memory. This is configurable using the GUC
 * timescaledb.continuous_aggregate_processing_wal_batch_size.
 */
void
multi_invalidation_move_invalidations(MultiInvalidationState *state)
{
	static SPIPlanPtr plan_get_invalidations = NULL;
	static const char *query_get_invalidations =
		"SELECT data FROM pg_logical_slot_get_binary_changes($1, NULL, $2, variadic $3)";
	char nulls[3];
	Datum args[3];

	SPI_connect();

	/* Set up plan if not already set up */
	if (plan_get_invalidations == NULL)
	{
		Oid argtypes[3] = { NAMEOID, INT4OID, TEXTARRAYOID };
		TS_DEBUG_LOG("setting up plan for query \"%s\"", query_get_invalidations);
		SPIPlanPtr plan = SPI_prepare(query_get_invalidations, 3, argtypes);
		if (plan == NULL)
			elog(ERROR, "SPI_prepare failed for \"%s\"", query_get_invalidations);
		SPI_keepplan(plan);
		plan_get_invalidations = plan;
	}

	/* Collect all hypertables with corresponding attributes in an array. We
	 * will use this for the call. The array consists of interleaving table
	 * name and attribute name. */
	ArrayType *array = get_array_for_changes(state);

	/*
	 * Set up arguments for the prepared statement to get a batch of ranges to
	 * process.
	 */
	memset(nulls, ' ', sizeof(nulls) / sizeof(*nulls));
	args[0] = NameGetDatum(&state->slot_name);
	args[1] = Int32GetDatum(ts_guc_cagg_wal_batch_size);
	args[2] = PointerGetDatum(array);

	while (true)
	{
		CatalogSecurityContext sec_ctx;

		TS_DEBUG_LOG("executing plan for query \"%s\"", query_get_invalidations);

		/* Fetch batches and add them to the multi-table state */
		ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);
		int rc = SPI_execute_plan(plan_get_invalidations, args, nulls, true, 0);
		ts_catalog_restore_user(&sec_ctx);

		if (rc < 0)
			elog(ERROR,
				 "SPI_execute_plan_extended failed executing query \"%s\": %s",
				 query_get_invalidations,
				 SPI_result_code_string(rc));

		TS_DEBUG_LOG("got %lu rows to process", (unsigned long) SPI_processed);

		if (SPI_processed == 0)
			break; /* No more batches to read */

		/* Process batch of rows and add invalidation ranges to state. */
		for (uint64 i = 0; i < SPI_processed; i++)
		{
			Datum data;
			bool isnull;

			TS_DEBUG_LOG("processing row %lu", (unsigned long) i);

			data = SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 1, &isnull);

			if (isnull)
				continue;

			bytea *raw_record = DatumGetByteaP(data);

			StringInfoData info;
			initReadOnlyStringInfo(&info, VARDATA_ANY(raw_record), VARSIZE_ANY_EXHDR(raw_record));

			InvalidationMessage msg;
			ts_invalidation_record_decode(&info, &msg);

			Invalidation invalidation = {
				.hyper_id = ts_hypertable_relid_to_id(msg.ver1.hypertable_relid),
				.lowest_modified_value = msg.ver1.lowest_modified,
				.greatest_modified_value = msg.ver1.highest_modified,
			};
			multi_invalidation_range_add(state, &invalidation);
		}
	}

	SPI_finish();

	/* Add invalidations to the materialization log. */
	multi_invalidation_range_write_all(state);
}

void
multi_invalidation_process_hypertable_log(List *hypertables)
{
	MultiInvalidationState state;
	ListCell *lc;
	char slotname[TS_INVALIDATION_SLOT_NAME_MAX];

	ts_get_invalidation_replication_slot_name(slotname, sizeof(slotname));
	multi_invalidation_state_init(&state, slotname, CurrentMemoryContext);

	foreach (lc, hypertables)
		(void) multi_invalidation_state_hypertable_entry_get_for_update(&state, lfirst_int(lc));

	multi_invalidation_move_invalidations(&state);

	multi_invalidation_state_cleanup(&state);
}
