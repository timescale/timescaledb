/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <access/heapam.h>
#include <access/xact.h>
#include <catalog/catalog.h>
#include <commands/tablecmds.h>
#include <nodes/parsenodes.h>
#include <utils/array.h>
#include <utils/palloc.h>
#include <utils/rel.h>
#include <utils/builtins.h>
#include <utils/snapmgr.h>

#include "ts_catalog/catalog.h"
#include "dimension.h"
#include "dimension_slice.h"
#include "dimension_partition.h"
#include "hypertable_cache.h"
#include "scanner.h"
#include "config.h"

#include "compat/compat.h"

static ScanIterator
ts_dimension_partition_scan_iterator_create(LOCKMODE lockmode)
{
	ScanIterator it = ts_scan_iterator_create(DIMENSION_PARTITION, lockmode, CurrentMemoryContext);
	it.ctx.flags |= SCANNER_F_NOEND_AND_NOCLOSE;

	return it;
}

static void
ts_dimension_partition_scan_iterator_set_dimension_id(ScanIterator *it, int32 dimension_id,
													  const ScanTupLock *tuplock)
{
	it->ctx.index = catalog_get_index(ts_catalog_get(),
									  DIMENSION_PARTITION,
									  DIMENSION_PARTITION_DIMENSION_ID_RANGE_START_IDX);
	ts_scan_iterator_scan_key_reset(it);
	ts_scan_iterator_scan_key_init(
		it,
		Anum_dimension_partition_dimension_id_range_start_idx_dimension_id,
		BTEqualStrategyNumber,
		F_INT4EQ,
		Int32GetDatum(dimension_id));
	it->ctx.tuplock = tuplock;
}

/*
 * Comparison function for dimension partitions.
 *
 * Used to sort arrays of partitions on range_start and to lookup a partition
 * using binary search.
 */
static int
dimpart_cmp(const void *key, const void *node)
{
	const DimensionPartition *dp_key = *((const DimensionPartition **) key);
	const DimensionPartition *dp_node = *((const DimensionPartition **) node);

	/* If the key's range is enclosed (or exactly matches) the partition,
	 * there's a match */
	if (dp_key->range_start >= dp_node->range_start && dp_key->range_end < dp_node->range_end)
		return 0;
	else if (dp_key->range_start < dp_node->range_start)
		return -1;
	else
	{
		Assert(dp_key->range_start > dp_node->range_start);
		return 1;
	}
}

static DimensionPartition *
dimpart_create(const HeapTuple tup, const TupleDesc tupdesc)
{
	DimensionPartition *dp = palloc0(sizeof(DimensionPartition));
	Datum values[Natts_dimension_partition];
	bool isnull[Natts_dimension_partition] = { false };

	heap_deform_tuple(tup, tupdesc, values, isnull);

	dp->dimension_id =
		DatumGetInt32(values[AttrNumberGetAttrOffset(Anum_dimension_partition_dimension_id)]);
	dp->range_start =
		DatumGetInt64(values[AttrNumberGetAttrOffset(Anum_dimension_partition_range_start)]);
	dp->range_end = DIMENSION_SLICE_MAXVALUE;
	dp->data_nodes = NIL;

	if (!isnull[AttrNumberGetAttrOffset(Anum_dimension_partition_data_nodes)])
	{
		ArrayIterator arrit;
		bool elem_isnull = false;
		ArrayType *arr = DatumGetArrayTypeP(
			values[AttrNumberGetAttrOffset(Anum_dimension_partition_data_nodes)]);
		Datum elem = (Datum) NULL;

		arrit = array_create_iterator(arr, 0, NULL);

		while (array_iterate(arrit, &elem, &elem_isnull))
		{
			if (!elem_isnull)
			{
				const char *dn = NameStr(*DatumGetName(elem));
				dp->data_nodes = lappend(dp->data_nodes, pstrdup(dn));
			}
		}

		array_free_iterator(arrit);
	}

	return dp;
}

/*
 * Get the current dimension partitions.
 *
 * The partitions are read into an array which is sorted so that it can be
 * binary searched.
 */
DimensionPartitionInfo *
ts_dimension_partition_info_get(int32 dimension_id)
{
	ScanIterator it;
	DimensionPartitionInfo *dpi;
	DimensionPartition **partitions;
	unsigned int count = 0;
#ifdef TS_DEBUG
	/* Use a low number of initial max partitions in DEBUG in order to "hit"
	 * array expansion below */
	unsigned int max_partitions = 2;
#else
	unsigned int max_partitions = 20;
#endif

	it = ts_dimension_partition_scan_iterator_create(AccessShareLock);
	ts_dimension_partition_scan_iterator_set_dimension_id(&it, dimension_id, NULL);
	/* Create a temporary array for partitions, which is likely too big */
	partitions = palloc0(sizeof(DimensionPartition *) * max_partitions);

	ts_scanner_foreach(&it)
	{
		bool should_free = false;
		HeapTuple tup = ts_scan_iterator_fetch_heap_tuple(&it, false, &should_free);
		TupleDesc tupdesc = ts_scan_iterator_tupledesc(&it);
		DimensionPartition *dp;

		if (count >= max_partitions)
		{
			max_partitions = count + 10;
			partitions = repalloc(partitions, sizeof(DimensionPartition *) * max_partitions);
		}

		dp = dimpart_create(tup, tupdesc);

		if (count > 0)
			partitions[count - 1]->range_end = dp->range_start;

		partitions[count++] = dp;

		if (should_free)
			heap_freetuple(tup);
	}

	ts_scan_iterator_close(&it);

	if (count == 0)
	{
		pfree(partitions);
		return NULL;
	}
	else if (count > 1)
	{
		partitions[count - 2]->range_end = partitions[count - 1]->range_start;
	}

	dpi = palloc0(sizeof(DimensionPartitionInfo));
	dpi->num_partitions = count;
	dpi->partitions = palloc0(sizeof(DimensionPartition *) * count);
	memcpy(dpi->partitions, partitions, sizeof(DimensionPartition *) * count);
	qsort(partitions, count, sizeof(DimensionPartition *), dimpart_cmp);
	pfree(partitions);

	return dpi;
}

/*
 * Find a partition using binary search.
 */
const DimensionPartition *
ts_dimension_partition_find(const DimensionPartitionInfo *dpi, int64 coord)
{
	const DimensionPartition **dp_found;
	const DimensionPartition dp = {
		.range_start = coord,
		.range_end = coord,
	};
	const DimensionPartition *dp_key = &dp;

	dp_found = bsearch(&dp_key,
					   dpi->partitions,
					   dpi->num_partitions,
					   sizeof(DimensionPartition *),
					   dimpart_cmp);

	if (!dp_found)
		elog(ERROR, "no partitions available");

	Assert((*dp_found)->range_start <= coord);
	Assert((*dp_found)->range_end > coord);

	return *dp_found;
}

static List *
get_replica_nodes(List *data_nodes, unsigned int index, int replication_factor)
{
	List *replica_nodes = NIL;
	int max_replicas = replication_factor;
	int i;

	/* Check for single-node case */
	if (data_nodes == NIL)
		return NIL;

	/* Can't have more replicas than we have data nodes */
	if (max_replicas > list_length(data_nodes))
		max_replicas = list_length(data_nodes);

	for (i = 0; i < max_replicas; i++)
	{
		int list_index = (index + i) % list_length(data_nodes);
		replica_nodes = lappend(replica_nodes, list_nth(data_nodes, list_index));
	}

	return replica_nodes;
}

static HeapTuple
create_dimension_partition_tuple(Relation rel, const DimensionPartition *dp)
{
	TupleDesc tupdesc = RelationGetDescr(rel);
	Datum values[Natts_dimension_partition];
	bool nulls[Natts_dimension_partition] = { false };
	int i = 0;

	values[AttrNumberGetAttrOffset(Anum_dimension_partition_dimension_id)] =
		Int32GetDatum(dp->dimension_id);
	values[AttrNumberGetAttrOffset(Anum_dimension_partition_range_start)] =
		Int64GetDatum(dp->range_start);

	if (dp->data_nodes == NIL)
	{
		nulls[AttrNumberGetAttrOffset(Anum_dimension_partition_data_nodes)] = true;
	}
	else
	{
		int data_nodes_len = list_length(dp->data_nodes);
		Datum *dn_datums = palloc(sizeof(Datum) * data_nodes_len);
		NameData *dn_names = palloc(NAMEDATALEN * data_nodes_len);
		ArrayType *dn_arr;
		ListCell *lc;

		foreach (lc, dp->data_nodes)
		{
			const char *dn = lfirst(lc);
			namestrcpy(&dn_names[i], dn);
			dn_datums[i] = NameGetDatum(&dn_names[i]);
			++i;
		}

		dn_arr =
			construct_array(dn_datums, data_nodes_len, NAMEOID, NAMEDATALEN, false, TYPALIGN_CHAR);
		values[AttrNumberGetAttrOffset(Anum_dimension_partition_data_nodes)] =
			PointerGetDatum(dn_arr);
	}

	return heap_form_tuple(tupdesc, values, nulls);
}

static void
dimension_partition_info_delete(int dimension_id, int scanflags, LOCKMODE lockmode)
{
	ScanIterator it;
	CatalogSecurityContext sec_ctx;

	it = ts_dimension_partition_scan_iterator_create(lockmode);
	ts_dimension_partition_scan_iterator_set_dimension_id(&it, dimension_id, NULL);
	it.ctx.flags = scanflags;

	ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);

	ts_scanner_foreach(&it)
	{
		const TupleInfo *ti = ts_scan_iterator_tuple_info(&it);
		ts_catalog_delete_tid_only(ti->scanrel, &ti->slot->tts_tid);
	}

	ts_catalog_restore_user(&sec_ctx);
	ts_scan_iterator_close(&it);
}

/*
 * Delete all dimension partitions for a given dimension.
 */
void
ts_dimension_partition_info_delete(int dimension_id)
{
	dimension_partition_info_delete(dimension_id, SCANNER_F_NOFLAGS, RowExclusiveLock);
}

/*
 * Recreate dimension partitions based on changes to one or more of these
 * variables:
 *
 * - number of partitions
 * - list of data nodes
 * - replication factor
 */
DimensionPartitionInfo *
ts_dimension_partition_info_recreate(int32 dimension_id, unsigned int num_partitions,
									 List *data_nodes, int replication_factor)
{
	int64 partition_size = DIMENSION_SLICE_CLOSED_MAX / ((int64) num_partitions);
	int64 range_start = DIMENSION_SLICE_MINVALUE;
	Catalog *catalog = ts_catalog_get();
	Oid relid = catalog_get_table_id(catalog, DIMENSION_PARTITION);
	DimensionPartitionInfo *dpi;
	DimensionPartition **partitions;
	Relation rel;
	unsigned int i;

	Assert(num_partitions > 0);
	Assert(data_nodes == NIL || replication_factor > 0);

	/* Delete all partitions for the dimension */
	dimension_partition_info_delete(dimension_id, SCANNER_F_KEEPLOCK, RowExclusiveLock);

	/* Lock already held */
	rel = table_open(relid, NoLock);

	partitions = palloc0(sizeof(DimensionPartition *) * num_partitions);

	for (i = 0; i < num_partitions; i++)
	{
		int64 range_end =
			(i == (num_partitions - 1)) ? DIMENSION_SLICE_CLOSED_MAX : range_start + partition_size;
		DimensionPartition *dp;
		CatalogSecurityContext sec_ctx;
		HeapTuple tuple;

		dp = palloc0(sizeof(DimensionPartition));
		*dp = (DimensionPartition){
			.dimension_id = dimension_id,
			.range_start = range_start,
			.range_end = range_end,
			.data_nodes = get_replica_nodes(data_nodes, i, replication_factor),
		};

		ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);
		tuple = create_dimension_partition_tuple(rel, dp);
		ts_catalog_insert_only(rel, tuple);
		ts_catalog_restore_user(&sec_ctx);
		heap_freetuple(tuple);

		partitions[i] = dp;

		/* Hash values for space partitions are in range 0 to INT32_MAX, so
		 * the first partition covers 0 to partition size (although the start
		 * is written as -INF) */
		if (range_start == DIMENSION_SLICE_MINVALUE)
			range_start = 0;

		range_start += partition_size;
	}

	table_close(rel, RowExclusiveLock);

	/* Sort the partitions so that we can later use binary search */
	qsort(partitions, num_partitions, sizeof(DimensionPartition *), dimpart_cmp);

	/* Make changes visible */
	CommandCounterIncrement();

	dpi = palloc(sizeof(DimensionPartitionInfo));
	dpi->partitions = partitions;
	dpi->num_partitions = num_partitions;

	return dpi;
}

/*
 * Manually update the dimension partition state for a hypertable.
 *
 * Used mostly when updating from a TimescaleDB version that didn't have this
 * state previously.
 */
TS_FUNCTION_INFO_V1(ts_dimension_partition_update);

Datum
ts_dimension_partition_update(PG_FUNCTION_ARGS)
{
	Oid hypertable_relid = PG_GETARG_OID(0);
	Cache *hcache;
	const Hypertable *ht =
		ts_hypertable_cache_get_cache_and_entry(hypertable_relid, CACHE_FLAG_NONE, &hcache);
	ts_hypertable_update_dimension_partitions(ht);
	ts_cache_release(hcache);
	PG_RETURN_VOID();
}
