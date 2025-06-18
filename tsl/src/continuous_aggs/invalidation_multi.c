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
#include "continuous_aggs/invalidation_funcs.h"
#include "continuous_aggs/invalidation_threshold.h"
#include "guc.h"
#include "ts_catalog/continuous_agg.h"

void
multi_invalidation_state_init(MultiHypertableInvalidationState *state, const char *slot_name,
							  MemoryContext mcxt)
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

	Catalog *const catalog = ts_catalog_get();
	Oid relid = catalog_get_table_id(catalog, CONTINUOUS_AGGS_MATERIALIZATION_INVALIDATION_LOG);
	state->logrel = table_open(relid, RowExclusiveLock);
	namestrcpy(&state->slot_name, slot_name);
}

void
multi_invalidation_state_cleanup(MultiHypertableInvalidationState *state)
{
	hash_destroy(state->ranges);
	hash_destroy(state->hypertables);
	table_close(state->logrel, NoLock);
	MemoryContextDelete(state->mcxt);
}

/*
 * Get multirange type for a type.
 *
 * Right now this is hard-coded, but we should probably look it up in pg_range
 * and cache it.
 *
 * PostgreSQL syscache does not support looking up multirange type (or range
 * type) for a particular element type, so we would have to scan pg_range and
 * build our own cache.
 *
 * We return the multirange because we can use lookup_type_cache() to fetch
 * all information about the multi-range, range, and element type using this.
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

static void
multi_invalidation_range_entry_init(MultiHypertableInvalidationRangeEntry *entry,
									int32 materialization_id,
									ContinuousAggsBucketFunction *bucket_function,
									TypeCacheEntry *typcache)
{
	entry->materialization_id = materialization_id;
	entry->bucket_function = bucket_function;
	entry->ranges_count = 0;
	entry->ranges_alloc = 256;
	entry->ranges = palloc0(entry->ranges_alloc * sizeof(RangeType *));
	entry->typcache = typcache;
}

/*
 * Initialize the hypertable invalidation entry.
 *
 * Returns all associated continuous aggregates that are using WAL to collect
 * invalidations.
 */
static List *
multi_invalidation_hypertable_entry_init(MultiHypertableInvalidationEntry *entry,
										 int32 hypertable_id)
{
	const Hypertable *const ht = ts_hypertable_get_by_id(hypertable_id);
	const Dimension *const dim = hyperspace_get_open_dimension(ht->space, 0);
	ListCell *lc;

	Ensure(!OidIsValid(get_trigger_oid(ht->main_table_relid, CAGGINVAL_TRIGGER_NAME, true)),
		   "hypertable has invalidation trigger, which was not expected");

	entry->hypertable_id = hypertable_id;
	entry->dimtype = ts_dimension_get_partition_type(dim);
	entry->typcache =
		lookup_type_cache(get_multirange_type(entry->dimtype), TYPECACHE_MULTIRANGE_INFO);
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
MultiHypertableInvalidationEntry *
multi_invalidation_state_hypertable_entry_get_for_update(MultiHypertableInvalidationState *state,
														 int32 hypertable_id)
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
		 * write the associated continuous aggregates (that uses the WAL) to
		 * the ranges cache at this point. */
		ListCell *lc;
		foreach (lc, caggs)
		{
			ContinuousAgg *cagg = lfirst(lc);
			int32 materialization_id = cagg->data.mat_hypertable_id;
			bool ranges_entry_found;
			MultiHypertableInvalidationRangeEntry *ranges_entry =
				hash_search(state->ranges, &materialization_id, HASH_ENTER, &ranges_entry_found);
			Assert(!ranges_entry_found);
			multi_invalidation_range_entry_init(ranges_entry,
												materialization_id,
												cagg->bucket_function,
												hypertables_entry->typcache);
		}
	}
	return hypertables_entry;
}

void
multi_invalidation_range_add(MultiHypertableInvalidationState *state,
							 const Invalidation *invalidation)
{
	MultiHypertableInvalidationEntry *hypertables_entry =
		multi_invalidation_state_hypertable_entry_get_for_update(state, invalidation->hyper_id);

	Oid dimtype = hypertables_entry->dimtype;

	/* Iterate over all associated caggs of the hypertable, expand the range
	 * to the bucket boundaries for the continuous aggregate, and add it to
	 * the ranges to build. */
	ListCell *lc;
	foreach (lc, hypertables_entry->caggs)
	{
		const int32 materialized_id = lfirst_int(lc);
		Invalidation cagg_invalidation = {
			.lowest_modified_value = invalidation->lowest_modified_value,
			.greatest_modified_value = invalidation->greatest_modified_value,
			.hyper_id = materialized_id,
		};

		bool ranges_entry_found;
		MultiHypertableInvalidationRangeEntry *ranges_entry =
			hash_search(state->ranges, &materialized_id, HASH_FIND, &ranges_entry_found);

		Assert(ranges_entry_found);

		invalidation_expand_to_bucket_boundaries(&cagg_invalidation,
												 dimtype,
												 ranges_entry->bucket_function);

		/* Add the new range to the list */
		RangeType *newrange = ts_internal_to_range(cagg_invalidation.lowest_modified_value,
												   cagg_invalidation.greatest_modified_value,
												   dimtype,
												   hypertables_entry->typcache->rngtype->type_id);
		ranges_entry->ranges[ranges_entry->ranges_count++] = newrange;

		/* Double the capacity if we have hit the ceiling */
		if (ranges_entry->ranges_alloc >= ranges_entry->ranges_count)
		{
			ranges_entry->ranges_alloc *= 2;
			ranges_entry->ranges =
				repalloc(ranges_entry->ranges, ranges_entry->ranges_alloc * sizeof(RangeType *));
		}
	}
}

void
multi_invalidation_range_write(MultiHypertableInvalidationState *state,
							   MultiHypertableInvalidationRangeEntry *entry)
{
	int32 range_count;
	RangeType **ranges;
	TupleDesc tupdesc = RelationGetDescr(state->logrel);

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
			ts_catalog_insert_only(state->logrel, newtup);
			ts_catalog_restore_user(&sec_ctx);
		}
	}
}

void
multi_invalidation_range_write_all(MultiHypertableInvalidationState *state)
{
	HASH_SEQ_STATUS hash_seq;
	MultiHypertableInvalidationRangeEntry *entry;

	hash_seq_init(&hash_seq, state->ranges);
	while ((entry = hash_seq_search(&hash_seq)) != NULL)
		multi_invalidation_range_write(state, entry);
}

/*
 * Get array of hypertables to collect changes.
 *
 * This array should be used as the variadic array for
 * pg_logical_slot_get_binary_changes().
 */
static ArrayType *
get_array_for_changes(MultiHypertableInvalidationState *state)
{
	HASH_SEQ_STATUS hash_seq;
	MultiHypertableInvalidationEntry *entry;
	const long count = hash_get_num_entries(state->hypertables);
	Datum *values = palloc(2 * sizeof(Datum) * count);

	hash_seq_init(&hash_seq, state->hypertables);
	for (int i = 0; (entry = hash_seq_search(&hash_seq)) != NULL; i += 2)
	{
		const Hypertable *ht = ts_hypertable_get_by_id(entry->hypertable_id);
		const Dimension *dim = hyperspace_get_open_dimension(ht->space, 0);
		values[i + 0] = PointerGetDatum(cstring_to_text(
			quote_qualified_identifier(NameStr(ht->fd.schema_name), NameStr(ht->fd.table_name))));
		values[i + 1] = PointerGetDatum(cstring_to_text(NameStr(dim->fd.column_name)));
	}

	/* Not sure we need the array of values after this. It seems like the
	 * datums are actually copied, but it is not entirely clear. */
	return construct_array_builtin(values, count, TEXTOID);
}

/*
 * Fill in an invalidation structure from the logical replication tuple data.
 */
static void
fill_invalidation_from_replication_tuple(Invalidation *invalidation, LogicalRepTupleData *rep_tuple,
										 TupleDesc tupdesc)
{
	bool hyper_id_isnull, lowest_isnull, greatest_isnull;
	int32 hypertable_id = ts_hypertable_relid_to_id(
		invalidation_tuple_get_value(rep_tuple, tupdesc, 1, &hyper_id_isnull));
	int64 lowest_modified_value =
		invalidation_tuple_get_value(rep_tuple, tupdesc, 2, &lowest_isnull);
	int64 greatest_modified_value =
		invalidation_tuple_get_value(rep_tuple, tupdesc, 3, &greatest_isnull);

	Ensure(!hyper_id_isnull && !lowest_isnull && !greatest_isnull,
		   "NULL value in invalidation tuple");

	invalidation->hyper_id = DatumGetInt64(hypertable_id);
	invalidation->lowest_modified_value = DatumGetInt64(lowest_modified_value);
	invalidation->greatest_modified_value = DatumGetInt64(greatest_modified_value);
}

/*
 * Move invalidations for multiple hypertables in one pass.
 *
 * We process the invalidation in batches to avoid eating up too much
 * memory. Right now the batch size is hard-coded, but we should probably add
 * a GUC to control the batch size for processing. (There is already a batch
 * size variable for refresh, maybe we can re-use that.)
 */
void
multi_invalidation_state_move_invalidations(MultiHypertableInvalidationState *state)
{
	static SPIPlanPtr plan_get_invalidations = NULL;
	static const char *query_get_invalidations =
		"SELECT data FROM pg_logical_slot_get_binary_changes($1, NULL, $2, variadic $3)";
	TupleDesc tupdesc = RelationGetDescr(state->logrel);
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
	args[1] = Int32GetDatum(1000); /* !!! Hard-coded for now. We should probably have a GUC. */
	args[2] = PointerGetDatum(array);

	while (true)
	{
		CatalogSecurityContext sec_ctx;

		TS_DEBUG_LOG("executing plan for query \"%s\"", query_get_invalidations);

		/* Fetch batches and add them to the multi-table state */
		ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);
		int rc = SPI_execute_plan(plan_get_invalidations, args, nulls, true, 1);
		ts_catalog_restore_user(&sec_ctx);

		if (rc < 0)
			elog(ERROR,
				 "SPI_execute_plan_extended failed executing query \"%s\": %s",
				 query_get_invalidations,
				 SPI_result_code_string(rc));

		TS_DEBUG_LOG("got %lu rows to process", SPI_processed);

		if (SPI_processed == 0)
			break; /* No more batches to read */

		/* Process batch of rows and add invalidation ranges to state. */
		for (uint64 i = 0; i < SPI_processed; i++)
		{
			Datum data;
			Invalidation invalidation;
			LogicalRepTupleData tupleData;
			bool isnull;

			TS_DEBUG_LOG("processing row %lu", i);

			data = SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 1, &isnull);

			if (isnull)
				continue;

			invalidation_tuple_decode(&tupleData, DatumGetByteaP(data));
			fill_invalidation_from_replication_tuple(&invalidation, &tupleData, tupdesc);
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
	MultiHypertableInvalidationState state;
	ListCell *lc;

	multi_invalidation_state_init(&state, ts_invalidation_get_slot_name(), CurrentMemoryContext);

	foreach (lc, hypertables)
		(void) multi_invalidation_state_hypertable_entry_get_for_update(&state, lfirst_int(lc));

	multi_invalidation_state_move_invalidations(&state);

	multi_invalidation_state_cleanup(&state);
}

/*
 * PostgreSQL function to move hypertable invalidations to materialization
 * invalidation log.
 */
Datum
continuous_agg_process_multi_hypertable_invalidations(PG_FUNCTION_ARGS)
{
	Datum value;
	bool isnull;

	ts_feature_flag_check(FEATURE_CAGG);

	TS_PREVENT_IN_TRANSACTION_BLOCK(get_func_name(FC_FN_OID(fcinfo)));

	Ensure(!PG_ARGISNULL(0), "NULL value not expected");

	ArrayType *hypertable_array = PG_GETARG_ARRAYTYPE_P(0);

	ArrayIterator array_iterator = array_create_iterator(hypertable_array, 0, NULL);

	List *hypertables = NIL;
	while (array_iterate(array_iterator, &value, &isnull))
	{
		Oid hypertable_relid = DatumGetObjectId(value);
		int32 hypertable_id = ts_hypertable_relid_to_id(hypertable_relid);
		TS_DEBUG_LOG("add relation \"%s\" to list: hypertable_relid=%d, hypertable_id=%d",
					 get_rel_name(hypertable_relid),
					 hypertable_relid,
					 hypertable_id);
		ts_hypertable_permissions_check(hypertable_relid, GetUserId());
		hypertables = lappend_int(hypertables, hypertable_id);
	}

	array_free_iterator(array_iterator);

	multi_invalidation_process_hypertable_log(hypertables);

	PG_RETURN_VOID();
}
