/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

/*
 * Module for processing invalidations for multiple hypertables in one single
 * pass. It will build up a multi-range for each continuous aggregate using
 * the internal time type.
 *
 * The multi-invalidation module will collect ranges for all continuous
 * aggregates associated with a hypertable and automatically write them to the
 * materialization log as needed to meet the memory bounds.
 *
 * When setting up the module, a log and high and low working memory limits
 * are provided. As ranges are added, the used memory is tracked and when it
 * exceeds the high working memory limit, invalidation ranges will be flushed
 * to the materialization log until reaching the low working memory limit.
 */

#include "invalidation_multi.h"

#include <postgres.h>

#include <access/tupdesc.h>
#include <catalog/pg_class_d.h>
#include <catalog/pg_type_d.h>
#include <common/int.h>
#include <executor/spi.h>
#include <lib/stringinfo.h>
#include <miscadmin.h>
#include <nodes/pg_list.h>
#include <postgres_ext.h>
#include <replication/logicalproto.h>
#include <storage/lockdefs.h>
#include <utils/array.h>
#include <utils/elog.h>
#include <utils/memutils.h>
#include <utils/palloc.h>
#include <utils/rangetypes.h>
#include <utils/typcache.h>

#include "continuous_aggs/invalidation.h"
#include "continuous_aggs/invalidation_record.h"
#include "continuous_aggs/invalidation_threshold.h"
#include "debug_point.h"
#include "guc.h"
#include "ts_catalog/continuous_agg.h"

/*
 * Initialize the multi-invalidation state to collect invalidations for
 * continuous aggregates.
 *
 * The multi-invalidation state tracks invalidation ranges for continuous
 * aggregates and is used to combine multiple ranges from the hypertable log
 * into a set of smaller ranges aligned to bucket widths for each continuous
 * aggregate.
 */
void
multi_invalidation_state_init(MultiInvalidationState *state, const char *slot_name,
							  Size low_work_mem, Size high_work_mem)
{
	state->hash_context = AllocSetContextCreate(CurrentMemoryContext,
												"InvalidationHashContext",
												ALLOCSET_START_SMALL_SIZES);

	state->work_context = AllocSetContextCreate(CurrentMemoryContext,
												"InvalidationProcessingContext",
												ALLOCSET_START_SMALL_SIZES);

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
	state->low_work_mem = low_work_mem;
	state->high_work_mem = high_work_mem;
}

void
multi_invalidation_state_cleanup(MultiInvalidationState *state)
{
	hash_destroy(state->ranges);
	hash_destroy(state->hypertables);
	table_close(state->logrel, NoLock);
	MemoryContextDelete(state->hash_context);
	MemoryContextDelete(state->work_context);
}

static void
multi_invalidation_range_entry_init(MultiInvalidationState *state,
									MultiInvalidationRangeEntry *entry, int32 materialization_id,
									ContinuousAggBucketFunction *bucket_function,
									MemoryContext context)
{
	entry->context = context;
	entry->materialization_id = materialization_id;
	entry->bucket_function = bucket_function;
	entry->ranges_count = 0;
	entry->ranges_alloc = 16; /* Starting count, but will increase */
	entry->ranges =
		MemoryContextAllocZero(entry->context, entry->ranges_alloc * sizeof(RangeType *));
}

/*
 * Clear the range entry.
 *
 * This is typically done before it is removed from the hash table. If you
 * want to use it again, you need to call multi_invalidation_range_init.
 *
 * Both the ranges and the range entry are stored in the hash memory context
 * for the processing state, so we need to free them explicitly.
 */
static void
multi_invalidation_range_entry_destroy(MultiInvalidationState *state,
									   MultiInvalidationRangeEntry *entry)
{
	Assert(entry->ranges != NULL && entry->context != NULL);
	TS_DEBUG_LOG("destroying context %s with %u KiB",
				 entry->context->name,
				 (unsigned int) (MemoryContextMemAllocated(entry->context, false) / 1024));
	MemoryContextDelete(entry->context);
	entry->ranges = NULL;
}

/*
 * Initialize the hypertable invalidation entry.
 *
 * Returns all associated continuous aggregates that are using WAL to collect
 * invalidations.
 */
static void
multi_invalidation_hypertable_entry_init(MultiInvalidationState *state,
										 MultiInvalidationEntry *entry, int32 hypertable_id)
{
	const Hypertable *const ht = ts_hypertable_get_by_id(hypertable_id);
	const Dimension *const dim = hyperspace_get_open_dimension(ht->space, 0);
	ListCell *lc;

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
		multi_invalidation_hypertable_entry_init(state, hypertables_entry, hypertable_id);

	Assert(hypertables_entry != NULL);
	return hypertables_entry;
}

void
multi_invalidation_range_add(MultiInvalidationState *state, const Invalidation *invalidation)
{
	Size allocated_hash_mem = MemoryContextMemAllocated(state->hash_context, true);

	/*
	 * If we are exceeding the high working memory mark, start to flush ranges
	 * until we are below the low working memory mark. We first check the
	 * amount of memory allocated, because this is quick, but then we need to
	 * check if it was a false alarm and we actually had space left.
	 */
	TS_DEBUG_LOG("used memory: %u KiB / %u KiB",
				 (unsigned int) (allocated_hash_mem / 1024),
				 (unsigned int) (state->high_work_mem / 1024));
	if (allocated_hash_mem > state->high_work_mem)
		multi_invalidation_flush_ranges(state);

	MemoryContext old_context = MemoryContextSwitchTo(state->hash_context);

	MultiInvalidationEntry *hypertables_entry =
		multi_invalidation_state_hypertable_entry_get_for_update(state, invalidation->hyper_id);

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
			hash_search(state->ranges, &materialized_id, HASH_ENTER, &ranges_entry_found);

		if (!ranges_entry_found)
		{
			/*
			 * The init below does a lot of scans so to be able to throw away that memory we use the
			 * range entry context for this scan. When destroying the memory context while flushing
			 * ranges we will get rid of this as well.
			 */
			MemoryContext context = AllocSetContextCreate(state->hash_context,
														  "InvalidationRangeEntryContext",
														  ALLOCSET_START_SMALL_SIZES);
			MemoryContext old_context = MemoryContextSwitchTo(context);

			ContinuousAgg *cagg =
				ts_continuous_agg_find_by_mat_hypertable_id(materialized_id, false);
			multi_invalidation_range_entry_init(state,
												ranges_entry,
												materialized_id,
												cagg->bucket_function,
												context);
			MemoryContextSwitchTo(old_context);
		}

		MemoryContext old_context = MemoryContextSwitchTo(ranges_entry->context);

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

		MemoryContextSwitchTo(old_context);

		Assert(ranges_entry->ranges_count < ranges_entry->ranges_alloc);
	}
	MemoryContextSwitchTo(old_context);
}

/*
 * Write the aggregated ranges to the log.
 *
 * This will first check if it is necessary to evict any range entries because
 * we risk exceeding the memory limit. If that is the case, it will start to
 * evict entries until it has "sufficient margins" to continue running.
 *
 * This will first create a multi-range from all the collected ranges to
 * eliminate duplicates and merge adjacent ranges, and then write all the
 * ranges to the materialization log.
 */
static void
multi_invalidation_range_entry_write(MultiInvalidationState *state,
									 MultiInvalidationRangeEntry *entry)
{
	int32 range_count;
	RangeType **ranges;
	TupleDesc tupdesc = RelationGetDescr(state->logrel);

	MemoryContext old_context = MemoryContextSwitchTo(state->work_context);

	/* Create a multirange from the collected ranges. This will also combine
	 * and merge any overlapping entries. It is allocated in the work context
	 * that we switched to above. */
	MultirangeType *multirange = make_multirange(INT8MULTIRANGEOID,
												 state->typecache->rngtype,
												 entry->ranges_count,
												 entry->ranges);

	/* Deserialize the multirange to a sequence of ranges and write them
	 * to the materialization log table. */
	multirange_deserialize(state->typecache->rngtype, multirange, &range_count, &ranges);
	for (int i = 0; i < range_count; i++)
	{
		bool empty;
		RangeBound lower, upper;

		/* Deserialize the range to get the lower and upper bound. */
		range_deserialize(state->typecache->rngtype, ranges[i], &lower, &upper, &empty);

		/* There should be no empty ranges returned, but if there is, we just
		 * ignore them in release builds. */
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

	/*
	 * We cannot reset the work memory context inside the loop since the
	 * multi-range is allocated in this memory. Doing so might thrash that
	 * data.
	 *
	 * A possible optimization is to create a temporary working context for
	 * the memory allocated in the loop, but this seems excessive unless we
	 * know a lot of memory is used here.
	 */
	MemoryContextSwitchTo(old_context);
	MemoryContextReset(state->work_context);
}

void
multi_invalidation_range_write_all(MultiInvalidationState *state)
{
	HASH_SEQ_STATUS hash_seq;
	MultiInvalidationRangeEntry *entry;

	hash_seq_init(&hash_seq, state->ranges);
	while ((entry = hash_seq_search(&hash_seq)) != NULL)
		multi_invalidation_range_entry_write(state, entry);
}

#if PG17_LT
static inline int
pg_cmp_size(size_t a, size_t b)
{
	return (a > b) - (a < b);
}
#endif

static int
range_entry_size_cmp(const void *lhs, const void *rhs)
{
	const MultiInvalidationRangeEntry *lhs_entry = *(MultiInvalidationRangeEntry **) lhs;
	const MultiInvalidationRangeEntry *rhs_entry = *(MultiInvalidationRangeEntry **) rhs;
	const Size lhs_size = MemoryContextMemAllocated(lhs_entry->context, false);
	const Size rhs_size = MemoryContextMemAllocated(rhs_entry->context, false);
	return pg_cmp_size(lhs_size, rhs_size);
}

/*
 * Flush ranges until we read low working memory.
 */
void
multi_invalidation_flush_ranges(MultiInvalidationState *state)
{
	HASH_SEQ_STATUS hash_seq;
	MultiInvalidationRangeEntry *entry;
	int count;

	/*
	 * We do *not* allocate this array on the work context since we are using
	 * the work context when processing the range entries. We explicitly free
	 * the memory after processing.
	 */
	Assert(CurrentMemoryContext != state->work_context);
	const long num_entries = hash_get_num_entries(state->ranges);
	MultiInvalidationRangeEntry **entries =
		palloc_array(MultiInvalidationRangeEntry *, num_entries);
	hash_seq_init(&hash_seq, state->ranges);
	for (count = 0; (entry = hash_seq_search(&hash_seq)) != NULL; ++count)
		entries[count] = entry;

	Assert(count == num_entries);

	/* Sort entries on the number of ranges in each entry */
	qsort(entries, num_entries, sizeof(MultiInvalidationRangeEntry *), range_entry_size_cmp);

	/*
	 * Flush range entries (and remove them from the hash table) until we are
	 * below the low working memory limit. We start from the largest entries
	 * (that is, the end of the array).
	 */
	int idx = num_entries;
	while (idx-- > 0 && state->low_work_mem < MemoryContextMemAllocated(state->hash_context, true))
	{
		multi_invalidation_range_entry_write(state, entries[idx]);
		multi_invalidation_range_entry_destroy(state, entries[idx]);
		hash_search(state->ranges, &entries[idx]->materialization_id, HASH_REMOVE, NULL);
	}

	/*
	 * If we ran out of entries to flush and still exceed working memory, we
	 * abort with an error. There is nothing we can do.
	 *
	 * This is not particularly likely, unless maintenance work memory is set
	 * really low.
	 */
	if (state->low_work_mem < MemoryContextMemAllocated(state->hash_context, true))
		ereport(ERROR,
				errcode(ERRCODE_OUT_OF_MEMORY),
				errmsg("not enough memory available to process invalidations"));

	pfree(entries);
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

	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "could not connect to SPI");

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

	TS_DEBUG_LOG("array: %s", datum_as_string(TEXTARRAYOID, args[2], false));

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

	/*
	 * This typically runs under PortalContext that lives for the duration of
	 * the query / call.
	 */
	TS_DEBUG_LOG("CurrentMemoryContext \"%s\" with %u KiB",
				 CurrentMemoryContext->name,
				 (unsigned int) (MemoryContextMemAllocated(CurrentMemoryContext, true) / 1024));

	ts_get_invalidation_replication_slot_name(slotname, sizeof(slotname));
	multi_invalidation_state_init(&state,
								  slotname,
								  1024 * ts_guc_cagg_low_work_mem,
								  1024 * ts_guc_cagg_high_work_mem);

	foreach (lc, hypertables)
		(void) multi_invalidation_state_hypertable_entry_get_for_update(&state, lfirst_int(lc));

	/*
	 * Lock WAL to coordinate refreshes.
	 *
	 * We use LockDatabaseObject() on the materialization log relation since
	 * that will not conflict with relation locks on the relation (it is an
	 * database object lock), only with other "database object lockers" on the
	 * relation.
	 */
	LockDatabaseObject(RelationRelationId, state.logrel->rd_id, 0, AccessExclusiveLock);
	DEBUG_WAITPOINT("multi_invalidation_process_invalidations");
	multi_invalidation_move_invalidations(&state);
	UnlockDatabaseObject(RelationRelationId, state.logrel->rd_id, 0, AccessExclusiveLock);

	multi_invalidation_state_cleanup(&state);
}
