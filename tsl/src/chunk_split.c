/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <access/multixact.h>
#include <access/rewriteheap.h>
#include <catalog/dependency.h>
#include <catalog/heap.h>
#include <catalog/pg_am.h>
#include <catalog/pg_collation.h>
#include <catalog/pg_constraint.h>
#include <commands/tablecmds.h>
#include <storage/bufmgr.h>
#include <storage/lockdefs.h>
#include <utils/acl.h>
#include <utils/snapshot.h>
#include <utils/syscache.h>

#include <math.h>

#include "chunk.h"
#include "compression/api.h"
#include "compression/compression.h"
#include "compression/create.h"
#include "debug_point.h"
#include "hypercube.h"
#include "partitioning.h"
#include "trigger.h"
#include "ts_catalog/array_utils.h"
#include "ts_catalog/catalog.h"
#include "ts_catalog/compression_chunk_size.h"
/*
 * The split_chunk() procedure currently only supports two-way split.
 */
#define SPLIT_FACTOR 2

typedef struct SplitContext SplitContext;

/*
 * SplitPointInfo
 *
 * Information about point where split happens, including column/dimension and
 * type we split along. Needed to route tuples to correct result relation.
 */
typedef struct SplitPoint
{
	const Dimension *dim;
	int64 point; /* Point at which we split */
	/*
	 * Function to route a tuple to a result relation during the split. The
	 * function's implementation is different depending on whether compressed
	 * or non-compressed relations are split.
	 */
	HeapTuple (*route_next_tuple)(TupleTableSlot *slot, SplitContext *scontext, int *routing_index);
} SplitPoint;

/*
 * CompressedSplitPoint
 *
 * Version of SplitPoint for a compressed relation.
 *
 * Since tuples are compressed, routing happens on min/max-metadata so column
 * references are different.
 */
typedef struct CompressedSplitPoint
{
	SplitPoint base;
	AttrNumber attnum_min;
	AttrNumber attnum_max;
	AttrNumber attnum_count;
	TupleDesc noncompressed_tupdesc;
} CompressedSplitPoint;

typedef struct RewriteStats
{
	int64 tuples_written;
	int64 tuples_alive;
	int64 tuples_recently_dead;
	int64 tuples_in_segments;
} RewriteStats;

/*
 * RelationWriteState
 *
 * State used to rewrite the resulting relations when splitting. Holds
 * information about attribute mappings in case the relations have different
 * tuple descriptors (e.g., due to dropped or added columns).
 */
typedef struct RelationWriteState
{
	BulkInsertState bistate;
	TupleTableSlot *dstslot;
	RewriteState rwstate;
	Relation targetrel;
	Datum *values;
	bool *isnull;
	/*
	 * Tuple mapping is needed in case the old relation has dropped
	 * columns. New relations (as result of split) are "clean" without dropped
	 * columns. The tuple map converts tuples between the source and
	 * destination chunks.
	 */
	TupleConversionMap *tupmap;
	RowCompressor compressor;
	RewriteStats stats;
} RelationWriteState;

/*
 * SplitContext
 *
 * Main state for doing a split.
 */
typedef struct SplitContext
{
	Relation rel; /* Relation/chunk being split */
	SplitPoint *sp;
	struct VacuumCutoffs cutoffs;
	int split_factor; /* Number of relations to split into */
	/* Array of rewrite states used to write the new relations. Size of
	 * split_factor. */
	RelationWriteState *rws;
	int rws_index; /* Index into rsi array indicating currently routed
					* relation. Set to -1 if no currently routed relation. */
} SplitContext;

/*
 * SplitRelationInfo
 *
 * Information about the result relations in a split. Also, information about
 * the number of tuples written to the relation is returned in the struct.
 */
typedef struct SplitRelationInfo
{
	Oid relid;		/* The relid of the result relation */
	int32 chunk_id; /* The corresponding chunk's ID */
	bool heap_swap; /* The original relation getting split will receive a heap
					 * swap. New chunks won't get a heap swap since they are
					 * new and not visible to anyone else. */
	RewriteStats stats;
} SplitRelationInfo;

static void
relation_split_info_init(RelationWriteState *rws, Relation srcrel, Oid target_relid,
						 struct VacuumCutoffs *cutoffs)
{
	rws->targetrel = table_open(target_relid, AccessExclusiveLock);
	rws->bistate = GetBulkInsertState();

	rws->rwstate = begin_heap_rewrite(srcrel,
									  rws->targetrel,
									  cutoffs->OldestXmin,
									  cutoffs->FreezeLimit,
									  cutoffs->MultiXactCutoff);

	rws->tupmap =
		convert_tuples_by_name(RelationGetDescr(srcrel), RelationGetDescr(rws->targetrel));

	/* Create tuple slot for new partition. */
	rws->dstslot = table_slot_create(rws->targetrel, NULL);
	ExecStoreAllNullTuple(rws->dstslot);

	rws->values = (Datum *) palloc0(RelationGetDescr(srcrel)->natts * sizeof(Datum));
	rws->isnull = (bool *) palloc0(RelationGetDescr(srcrel)->natts * sizeof(bool));
}

static void
relation_split_info_cleanup(RelationWriteState *rws, int ti_options)
{
	ExecDropSingleTupleTableSlot(rws->dstslot);
	FreeBulkInsertState(rws->bistate);
	table_finish_bulk_insert(rws->targetrel, ti_options);
	end_heap_rewrite(rws->rwstate);
	table_close(rws->targetrel, NoLock);
	pfree(rws->values);
	pfree(rws->isnull);

	if (rws->tupmap)
		free_conversion_map(rws->tupmap);

	rws->targetrel = NULL;
	rws->bistate = NULL;
	rws->dstslot = NULL;
	rws->tupmap = NULL;
	rws->values = NULL;
	rws->isnull = NULL;
}

/*
 * Reconstruct and rewrite the given tuple.
 *
 * Mostly taken from heapam module.
 *
 * When splitting a relation in two, the old relation is retained for one of
 * the result relations while the other is created new. This might lead to a
 * situation where the two result relations have different attribute mappings
 * because the old one could have dropped columns while the new one is "clean"
 * without dropped columns. Therefore, the rewrite function needs to account
 * for this when the tuple is rewritten.
 */
static void
reform_and_rewrite_tuple(HeapTuple tuple, Relation srcrel, RelationWriteState *rws)
{
	TupleDesc oldTupDesc = RelationGetDescr(srcrel);
	TupleDesc newTupDesc = RelationGetDescr(rws->targetrel);
	HeapTuple tupcopy;

	if (rws->tupmap)
	{
		/*
		 * If this is the "new" relation, the tuple map might be different
		 * from the "source" relation.
		 */
		tupcopy = execute_attr_map_tuple(tuple, rws->tupmap);
	}
	else
	{
		int i;

		heap_deform_tuple(tuple, oldTupDesc, rws->values, rws->isnull);

		/* Be sure to null out any dropped columns if this is the "old"
		 * relation. A relation created new doesn't have dropped columns. */
		for (i = 0; i < newTupDesc->natts; i++)
		{
			if (TupleDescAttr(newTupDesc, i)->attisdropped)
				rws->isnull[i] = true;
		}

		tupcopy = heap_form_tuple(newTupDesc, rws->values, rws->isnull);
	}

	/* The heap rewrite module does the rest */
	rewrite_heap_tuple(rws->rwstate, tuple, tupcopy);
	heap_freetuple(tupcopy);
}

static Datum
slot_get_partition_value(TupleTableSlot *slot, AttrNumber attnum, const SplitPoint *sp)
{
	bool isnull = false;
	Datum value = slot_getattr(slot, attnum, &isnull);

	/*
	 * Space-partition columns can have NULL values, but we only support
	 * splits on time dimensions at the moment.
	 */
	Ensure(!isnull, "unexpected NULL value in partitioning column");

	/*
	 * Both time and space dimensions can have partitioning functions, so it
	 * is necessary to always check for a function.
	 */
	if (NULL != sp->dim->partitioning)
	{
		Oid collation;

		collation =
			TupleDescAttr(slot->tts_tupleDescriptor, AttrNumberGetAttrOffset(attnum))->attcollation;
		value = ts_partitioning_func_apply(sp->dim->partitioning, collation, value);
	}

	return value;
}

/*
 * Compute the partition/routing index for a tuple.
 *
 * Returns 0 or 1 for first or second partition, respectively.
 */
static int
route_tuple(TupleTableSlot *slot, const SplitPoint *sp)
{
	Oid dimtype = ts_dimension_get_partition_type(sp->dim);
	Datum value = slot_get_partition_value(slot, sp->dim->column_attno, sp);
	int64 point = ts_time_value_to_internal(value, dimtype);

	/*
	 * Route to partition based on new boundaries. Only 2-way split is
	 * supported now, so routing is easy. An N-way split requires, e.g.,
	 * binary search.
	 */
	return (point < sp->point) ? 0 : 1;
}

/*
 * Compute the partition/routing index for a compressed tuple.
 *
 * Returns 0 or 1 for first or second partition, and -1 if the split point
 * falls within the given compressed tuple.
 */
static int
route_compressed_tuple(TupleTableSlot *slot, const SplitPoint *sp)
{
	const CompressedSplitPoint *csp = (const CompressedSplitPoint *) sp;
	Oid dimtype = ts_dimension_get_partition_type(sp->dim);
	Datum min_value = slot_get_partition_value(slot, csp->attnum_min, sp);
	Datum max_value = slot_get_partition_value(slot, csp->attnum_max, sp);
	int64 min_point = ts_time_value_to_internal(min_value, dimtype);
	int64 max_point = ts_time_value_to_internal(max_value, dimtype);

	if (max_point < sp->point)
		return 0;

	if (min_point >= sp->point)
		return 1;

	Assert(min_point < sp->point && max_point >= sp->point);
	return -1;
}

/*
 * Route a tuple to its partition.
 *
 * Only a 2-way split is supported at this time.
 *
 * For every non-NULL tuple returned, the routing_index will be set to 0 for
 * the first partition, and 1 for then second.
 */
static HeapTuple
route_next_non_compressed_tuple(TupleTableSlot *slot, SplitContext *scontext, int *routing_index)
{
	if (scontext->rws_index != -1)
	{
		scontext->rws_index = -1;
		return NULL;
	}

	scontext->rws_index = route_tuple(slot, scontext->sp);
	*routing_index = scontext->rws_index;

	return ExecFetchSlotHeapTuple(slot, false, NULL);
}

/*
 * Route a compressed tuple (segment) to its corresponding result partition
 * for the split.
 *
 * If the split point is found to be within the segment, it needs to be split
 * and sub-segments returned instead. Therefore, this function should be
 * called in a loop until returning NULL (no sub-segments left). If the
 * segment is not split, only the original segment is returned.
 *
 * For every non-NULL tuple returned, the routing_index will be set to 0 for
 * the first partition, and 1 for the second.
 */
static HeapTuple
route_next_compressed_tuple(TupleTableSlot *slot, SplitContext *scontext, int *routing_index)
{
	CompressedSplitPoint *csp = (CompressedSplitPoint *) scontext->sp;

	Assert(scontext->rws_index >= -1 && scontext->rws_index <= scontext->split_factor);

	if (scontext->rws_index == scontext->split_factor)
	{
		/* Nothing more to route for this tuple, so return NULL */
		scontext->rws_index = -1;
		*routing_index = -1;
		return NULL;
	}
	else if (scontext->rws_index >= 0)
	{
		/* Segment is being split and recompressed into a sub-segment per
		 * partition. Return the sub-segments until done. */
		Assert(scontext->rws_index < scontext->split_factor);
		RelationWriteState *rws = &scontext->rws[scontext->rws_index];
		HeapTuple new_tuple = row_compressor_build_tuple(&rws->compressor);
		HeapTuple old_tuple = ExecFetchSlotHeapTuple(slot, false, NULL);

		/* Copy over visibility information from the original segment
		 * tuple. First copy the HeapTupleFields holding the xmin and
		 * xmax. Then copy the infomask which has, among other things, the
		 * frozen flag bits. */
		memcpy(&new_tuple->t_data->t_choice.t_heap,
			   &old_tuple->t_data->t_choice.t_heap,
			   sizeof(HeapTupleFields));

		new_tuple->t_data->t_infomask &= ~HEAP_XACT_MASK;
		new_tuple->t_data->t_infomask2 &= ~HEAP2_XACT_MASK;
		new_tuple->t_data->t_infomask |= old_tuple->t_data->t_infomask & HEAP_XACT_MASK;

		new_tuple->t_tableOid = RelationGetRelid(rws->targetrel);

		row_compressor_clear_batch(&rws->compressor, false);
		rws->stats.tuples_in_segments += rws->compressor.rowcnt_pre_compression;
		*routing_index = scontext->rws_index;
		scontext->rws_index++;
		row_compressor_close(&rws->compressor);

		return new_tuple;
	}

	*routing_index = route_compressed_tuple(slot, scontext->sp);

	if (*routing_index == -1)
	{
		/*
		 * The split point is within the current compressed segment. It needs
		 * to be split across the partitions by decompressing and
		 * recompressing into sub-segments.
		 */
		HeapTuple tuple;
		CompressionSettings *csettings =
			ts_compression_settings_get_by_compress_relid(RelationGetRelid(scontext->rel));

		tuple = ExecFetchSlotHeapTuple(slot, false, NULL);

		RowDecompressor decompressor =
			build_decompressor(slot->tts_tupleDescriptor, csp->noncompressed_tupdesc);

		heap_deform_tuple(tuple,
						  decompressor.in_desc,
						  decompressor.compressed_datums,
						  decompressor.compressed_is_nulls);

		int nrows = decompress_batch(&decompressor);

		/*
		 * Initialize a compressor for each new partition.
		 */
		for (int i = 0; i < scontext->split_factor; i++)
		{
			RelationWriteState *rws = &scontext->rws[i];
			row_compressor_init(&rws->compressor,
								csettings,
								csp->noncompressed_tupdesc,
								RelationGetDescr(scontext->rws[i].targetrel));
		}

		/*
		 * Route each decompressed tuple to its corresponding partition's
		 * compressor.
		 */
		for (int i = 0; i < nrows; i++)
		{
			int routing_index = route_tuple(decompressor.decompressed_slots[i], scontext->sp);
			Assert(routing_index == 0 || routing_index == 1);
			RelationWriteState *rws = &scontext->rws[routing_index];
			/*
			 * Since we're splitting a segment, the new segments will be
			 * ordered like the original segment. Also, there is no risk of
			 * the segments getting too big since we are only making segments
			 * smaller.
			 */
			row_compressor_append_ordered_slot(&rws->compressor,
											   decompressor.decompressed_slots[i]);
		}

		row_decompressor_close(&decompressor);
		scontext->rws_index = 0;

		/*
		 * Call this function again to return the sub-segments.
		 */
		return route_next_compressed_tuple(slot, scontext, routing_index);
	}

	/* Update tuple count stats for compressed data */
	bool isnull;
	Datum count = slot_getattr(slot, csp->attnum_count, &isnull);
	scontext->rws[*routing_index].stats.tuples_in_segments += DatumGetInt32(count);

	/*
	 * The compressed tuple (segment) can be routed without splitting it.
	 */
	Assert(*routing_index >= 0 && *routing_index < scontext->split_factor);
	scontext->rws_index = scontext->split_factor;

	return ExecFetchSlotHeapTuple(slot, false, NULL);
}

static double
copy_tuples_for_split(SplitContext *scontext)
{
	Relation srcrel = scontext->rel;
	TupleTableSlot *srcslot;
	MemoryContext oldcxt;
	EState *estate;
	ExprContext *econtext;
	TableScanDesc scan;
	SplitPoint *sp = scontext->sp;

	estate = CreateExecutorState();

	/* Create the tuple slot */
	srcslot = table_slot_create(srcrel, NULL);

	/*
	 * Scan through the rows using SnapshotAny to see everything so that we
	 * can transfer tuples that are deleted or updated but still visible to
	 * concurrent transactions.
	 */
	scan = table_beginscan(srcrel, SnapshotAny, 0, NULL);

	/*
	 * Switch to per-tuple memory context and reset it for each tuple
	 * produced, so we don't leak memory.
	 */
	econtext = GetPerTupleExprContext(estate);
	oldcxt = MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));

	/*
	 * Read all the data from the split relation and route the tuples to the
	 * new partitions. Do some vacuuming and cleanup at the same
	 * time. Transfer all visibility information to the new relations.
	 *
	 * Main loop inspired by heapam_relation_copy_for_cluster() used to run
	 * CLUSTER and VACUUM FULL on a table.
	 */
	double num_tuples = 0.0;
	double tups_vacuumed = 0.0;
	double tups_recently_dead = 0.0;

	BufferHeapTupleTableSlot *hslot;
	int routingindex = -1;

	while (table_scan_getnextslot(scan, ForwardScanDirection, srcslot))
	{
		RelationWriteState *rws = NULL;
		HeapTuple tuple;
		Buffer buf;
		bool isdead;
		bool isalive = false;

		CHECK_FOR_INTERRUPTS();
		ResetExprContext(econtext);

		tuple = ExecFetchSlotHeapTuple(srcslot, false, NULL);

		hslot = (BufferHeapTupleTableSlot *) srcslot;

		buf = hslot->buffer;

		LockBuffer(buf, BUFFER_LOCK_SHARE);

		switch (HeapTupleSatisfiesVacuum(tuple, scontext->cutoffs.OldestXmin, buf))
		{
			case HEAPTUPLE_DEAD:
				/* Definitely dead */
				isdead = true;
				break;
			case HEAPTUPLE_RECENTLY_DEAD:
				tups_recently_dead += 1;
				isdead = false;
				break;
			case HEAPTUPLE_LIVE:
				/* Live or recently dead, must copy it */
				isdead = false;
				isalive = true;
				break;
			case HEAPTUPLE_INSERT_IN_PROGRESS:
				/*
				 * Since we hold exclusive lock on the relation, normally the
				 * only way to see this is if it was inserted earlier in our
				 * own transaction. Give a warning if this case does not
				 * apply; in any case we better copy it.
				 */
				if (!TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetXmin(tuple->t_data)))
					elog(WARNING,
						 "concurrent insert in progress within table \"%s\"",
						 RelationGetRelationName(srcrel));
				/* treat as live */
				isdead = false;
				isalive = true;
				break;
			case HEAPTUPLE_DELETE_IN_PROGRESS:
				/*
				 * Similar situation to INSERT_IN_PROGRESS case.
				 */
				if (!TransactionIdIsCurrentTransactionId(
						HeapTupleHeaderGetUpdateXid(tuple->t_data)))
					elog(WARNING,
						 "concurrent delete in progress within table \"%s\"",
						 RelationGetRelationName(srcrel));
				/* treat as recently dead */
				tups_recently_dead += 1;
				isalive = true;
				isdead = false;
				break;
			default:
				elog(ERROR, "unexpected HeapTupleSatisfiesVacuum result");
				isdead = false; /* keep compiler quiet */
				break;
		}

		LockBuffer(buf, BUFFER_LOCK_UNLOCK);

		HeapTuple tuple2;

		/*
		 * Route the tuple to the matching (new) partition. The routing is
		 * done in a loop because compressed tuple segments might be split
		 * into multiple sub-segment tuples if the split is in the middle of
		 * that segment.
		 */
		while ((tuple2 = sp->route_next_tuple(srcslot, scontext, &routingindex)))
		{
			Assert(routingindex >= 0 && routingindex < scontext->split_factor);
			rws = &scontext->rws[routingindex];

			if (isdead)
			{
				tups_vacuumed += 1;
				/* heap rewrite module still needs to see it... */
				if (rewrite_heap_dead_tuple(rws->rwstate, tuple2))
				{
					/* A previous recently-dead tuple is now known dead */
					tups_vacuumed += 1;
					tups_recently_dead -= 1;
				}
			}
			else
			{
				num_tuples++;
				rws->stats.tuples_written++;

				if (isalive)
					rws->stats.tuples_alive++;

				reform_and_rewrite_tuple(tuple2, srcrel, rws);
			}
		}
	}

	MemoryContextSwitchTo(oldcxt);

	const char *nspname = get_namespace_name(RelationGetNamespace(srcrel));

	ereport(DEBUG1,
			(errmsg("\"%s.%s\": found %.0f removable, %.0f nonremovable row versions",
					nspname,
					RelationGetRelationName(srcrel),
					tups_vacuumed,
					num_tuples),
			 errdetail("%.0f dead row versions cannot be removed yet.", tups_recently_dead)));

	table_endscan(scan);
	ExecDropSingleTupleTableSlot(srcslot);
	FreeExecutorState(estate);

	return num_tuples;
}

/*
 * Split a relation into "split_factor" pieces.
 */
static void
split_relation(Relation rel, SplitPoint *sp, unsigned int split_factor,
			   SplitRelationInfo *split_relations)
{
	char relpersistence = rel->rd_rel->relpersistence;
	SplitContext scontext = {
		.rel = rel,
		.split_factor = split_factor,
		.rws = palloc0(sizeof(RelationWriteState) * split_factor),
		.sp = sp,
		.rws_index = -1,
	};

	compute_rel_vacuum_cutoffs(scontext.rel, &scontext.cutoffs);

	for (unsigned int i = 0; i < split_factor; i++)
	{
		SplitRelationInfo *sri = &split_relations[i];
		Oid write_relid = sri->relid;

		if (sri->heap_swap)
		{
			write_relid = make_new_heap(RelationGetRelid(rel),
										rel->rd_rel->reltablespace,
										rel->rd_rel->relam,
										relpersistence,
										AccessExclusiveLock);
		}

		relation_split_info_init(&scontext.rws[i], rel, write_relid, &scontext.cutoffs);
	}

	DEBUG_WAITPOINT("split_chunk_before_tuple_routing");

	copy_tuples_for_split(&scontext);
	table_close(rel, NoLock);

	for (unsigned int i = 0; i < split_factor; i++)
	{
		RelationWriteState *rws = &scontext.rws[i];
		SplitRelationInfo *sri = &split_relations[i];
		ReindexParams reindex_params = { 0 };
		int reindex_flags = REINDEX_REL_SUPPRESS_INDEX_USE;
		Oid write_relid = RelationGetRelid(rws->targetrel);

		Ensure(relpersistence == RELPERSISTENCE_PERMANENT, "only permanent chunks can be split");
		reindex_flags |= REINDEX_REL_FORCE_INDEXES_PERMANENT;

		/* Save stats before cleaning up rewrite state */
		memcpy(&sri->stats, &rws->stats, sizeof(sri->stats));

		relation_split_info_cleanup(rws, TABLE_INSERT_SKIP_FSM);

		/*
		 * Only reindex new chunks. Existing chunk will be reindexed during
		 * the heap swap.
		 */
		if (sri->heap_swap)
		{
			/* Finally, swap the heap of the chunk that we split so that it only
			 * contains the tuples for its new partition boundaries. AccessExclusive
			 * lock is held during the swap. */
			finish_heap_swap(sri->relid,
							 write_relid,
							 false, /* system catalog */
							 false /* swap toast by content */,
							 true, /* check constraints */
							 true, /* internal? */
							 scontext.cutoffs.FreezeLimit,
							 scontext.cutoffs.MultiXactCutoff,
							 relpersistence);
		}
		else
		{
			reindex_relation_compat(NULL, sri->relid, reindex_flags, &reindex_params);
		}
	}

	pfree(scontext.rws);
}

static void
compute_compression_size_stats_fraction(Form_compression_chunk_size ccs, double fraction)
{
	ccs->compressed_heap_size = (int64) rint((double) ccs->compressed_heap_size * fraction);
	ccs->uncompressed_heap_size = (int64) rint((double) ccs->uncompressed_heap_size * fraction);
	ccs->uncompressed_index_size = (int64) rint((double) ccs->uncompressed_index_size * fraction);
	ccs->compressed_index_size = (int64) rint((double) ccs->compressed_index_size * fraction);
	ccs->uncompressed_toast_size = (int64) rint((double) ccs->uncompressed_toast_size * fraction);
	ccs->compressed_toast_size = (int64) rint((double) ccs->compressed_toast_size * fraction);
	ccs->numrows_frozen_immediately =
		(int64) rint((double) ccs->numrows_frozen_immediately * fraction);
	ccs->numrows_pre_compression = (int64) rint((double) ccs->numrows_pre_compression * fraction);
	ccs->numrows_post_compression = (int64) rint((double) ccs->numrows_post_compression * fraction);
}

static void
update_compression_stats_for_split(const SplitRelationInfo *split_relations,
								   const SplitRelationInfo *compressed_split_relations,
								   int split_factor)
{
	double total_tuples = 0;

	Assert(split_factor > 1);

	/*
	 * Set the new chunk status and calculate the total amount of tuples
	 * (compressed and non-compressed), which is used to calculated the
	 * fraction of data each new partition received.
	 */
	for (int i = 0; i < split_factor; i++)
	{
		const SplitRelationInfo *sri = &split_relations[i];
		const SplitRelationInfo *csri = &compressed_split_relations[i];
		Chunk *chunk = ts_chunk_get_by_relid(sri->relid, true);

		if (sri->stats.tuples_written > 0)
			ts_chunk_set_partial(chunk);
		else
			ts_chunk_clear_status(chunk, CHUNK_STATUS_COMPRESSED_PARTIAL);

		total_tuples += sri->stats.tuples_alive + csri->stats.tuples_in_segments;
	}

	/*
	 * Get the existing stats for the original chunk. The stats will be split
	 * across the resulting new chunks.
	 */
	FormData_compression_chunk_size ccs;

	ts_compression_chunk_size_get(split_relations[0].chunk_id, &ccs);

	for (int i = 0; i < split_factor; i++)
	{
		const SplitRelationInfo *sri = &split_relations[i];
		const SplitRelationInfo *csri = &compressed_split_relations[i];
		FormData_compression_chunk_size new_ccs;

		/* Calculate the fraction of compressed and non-compressed data received
		 * by the first partition (chunk) */
		double fraction = 0.0;

		if (total_tuples > 0)
			fraction = (sri->stats.tuples_alive + csri->stats.tuples_in_segments) / total_tuples;

		memcpy(&new_ccs, &ccs, sizeof(ccs));
		compute_compression_size_stats_fraction(&new_ccs, fraction);

		if (sri->heap_swap)
		{
			ts_compression_chunk_size_update(sri->chunk_id, &new_ccs);
		}
		else
		{
			/* The new partition (chunk) doesn't have stats so create new. */
			RelationSize relsize = {
				.heap_size = new_ccs.uncompressed_heap_size,
				.index_size = new_ccs.uncompressed_index_size,
				.toast_size = new_ccs.uncompressed_toast_size,
			};
			RelationSize compressed_relsize = {
				.heap_size = new_ccs.compressed_heap_size,
				.index_size = new_ccs.compressed_index_size,
				.toast_size = new_ccs.compressed_toast_size,
			};

			compression_chunk_size_catalog_insert(sri->chunk_id,
												  &relsize,
												  csri->chunk_id,
												  &compressed_relsize,
												  new_ccs.numrows_pre_compression,
												  new_ccs.numrows_post_compression,
												  new_ccs.numrows_frozen_immediately);
		}
	}
}

/*
 * Update the chunk stats for the split. Also set the chunk state (partial or
 * non-partial) and reltuples in pg_class.
 *
 * To calculate new compression chunk size stats, the existing stats are
 * simply split across the result partitions based on the fraction of data
 * they received. New stats are not calculated since the pre-compression sizes
 * for the split relations are not known (it would require decompression and
 * then measuring the disk usage). The advantage of splitting the stats is
 * that the total size stats is the the same after the split as they were
 * before the split.
 */
static void
update_chunk_stats_for_split(const SplitRelationInfo *split_relations,
							 const SplitRelationInfo *compressed_split_relations, int split_factor)
{
	if (compressed_split_relations)
		update_compression_stats_for_split(split_relations,
										   compressed_split_relations,
										   split_factor);
	/*
	 * Update reltuples in pg_class. The reltuples are normally updated on
	 * reindex, so this update only matters in case of no indexes.
	 */
	Relation relRelation = table_open(RelationRelationId, RowExclusiveLock);

	for (int i = 0; i < split_factor; i++)
	{
		const SplitRelationInfo *sri = &split_relations[i];
		Relation rel;
		double ntuples = sri->stats.tuples_alive;

		rel = table_open(sri->relid, AccessShareLock);
		update_relstats(relRelation, sri->relid, RelationGetNumberOfBlocks(rel), ntuples);
		table_close(rel, NoLock);
	}

	table_close(relRelation, RowExclusiveLock);
}

/*
 * Split a chunk along a given dimension and split point.
 *
 * The column/dimension and "split at" point are optional. If these arguments
 * are not specified, the chunk is split in two equal ranges based on the
 * primary partitioning column.
 *
 * The split is done using the table rewrite approach used by the PostgreSQL
 * CLUSTER code (also used for VACUUM FULL). It uses the rewrite module to
 * retain the visibility information of tuples, and also transferring (old)
 * deleted or updated tuples that are still visible to concurrent transactions
 * reading an older snapshot. Completely dead tuples are garbage collected.
 *
 * The advantage of the rewrite approach is that it is fully MVCC compliant
 * and ensures the result relations have minimal garbage after the split. Note
 * that locks don't fully protect against visibility issues since a concurrent
 * transaction can be pinned to an older snapshot while not (yet) holding any
 * locks on relations (chunks and hypertables) being split.
 */
Datum
chunk_split_chunk(PG_FUNCTION_ARGS)
{
	Oid relid = PG_ARGISNULL(0) ? InvalidOid : PG_GETARG_OID(0);
	const Chunk *chunk;
	Relation srcrel;

	chunk = ts_chunk_get_by_relid_locked(relid, AccessExclusiveLock, true);

	/* Chunk already locked, so use NoLock */
	srcrel = table_open(relid, NoLock);

	if (srcrel->rd_rel->relkind != RELKIND_RELATION)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot split non-table relations")));

	Oid amoid = srcrel->rd_rel->relam;

	if (amoid != HEAP_TABLE_AM_OID)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("access method \"%s\" is not supported for split", get_am_name(amoid))));

	/* Only owner is allowed to split */
	if (!object_ownercheck(RelationRelationId, relid, GetUserId()))
		aclcheck_error(ACLCHECK_NOT_OWNER,
					   get_relkind_objtype(srcrel->rd_rel->relkind),
					   get_rel_name(relid));

	/* Lock toast table to prevent it from being concurrently vacuumed */
	if (srcrel->rd_rel->reltoastrelid)
		LockRelationOid(srcrel->rd_rel->reltoastrelid, AccessExclusiveLock);

	/*
	 * Check for active uses of the relation in the current transaction,
	 * including open scans and pending AFTER trigger events.
	 */
	CheckTableNotInUse(srcrel, "split_chunk");

	if (chunk->fd.osm_chunk)
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("cannot split OSM chunks")));

	if (ts_chunk_is_frozen(chunk))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot split frozen chunk \"%s.%s\" scheduled for tiering",
						NameStr(chunk->fd.schema_name),
						NameStr(chunk->fd.table_name)),
				 errhint("Untier the chunk before splitting it.")));

	Cache *hcache;
	const Hypertable *ht =
		ts_hypertable_cache_get_cache_and_entry(chunk->hypertable_relid, CACHE_FLAG_NONE, &hcache);
	const Dimension *dim = hyperspace_get_open_dimension(ht->space, 0);

	Ensure(dim, "no primary dimension for chunk");

	if (ht->fd.num_dimensions > 1)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot split chunk in multi-dimensional hypertable")));

	NameData splitdim_name;
	namestrcpy(&splitdim_name, NameStr(dim->fd.column_name));

	Oid splitdim_type = ts_dimension_get_partition_type(dim);
	Oid splitcolumn_type = dim->fd.column_type;
	Datum split_at_datum;
	bool have_split_at = false;

	/* Check split_at argument */
	if (!PG_ARGISNULL(1))
	{
		Oid argtype = get_fn_expr_argtype(fcinfo->flinfo, 1);
		Datum arg = PG_GETARG_DATUM(1);

		if (argtype == UNKNOWNOID)
		{
			Oid infuncid = InvalidOid;
			Oid typioparam;

			getTypeInputInfo(splitdim_type, &infuncid, &typioparam);

			switch (get_func_nargs(infuncid))
			{
				case 1:
					/* Functions that take one input argument, e.g., the Date function */
					split_at_datum = OidFunctionCall1(infuncid, arg);
					break;
				case 3:
					/* Timestamp functions take three input arguments */
					split_at_datum = OidFunctionCall3(infuncid,
													  arg,
													  ObjectIdGetDatum(InvalidOid),
													  Int32GetDatum(-1));
					break;
				default:
					/* Shouldn't be any time types with other number of args */
					Ensure(false, "invalid type for split_at");
					pg_unreachable();
			}
			argtype = splitdim_type;
		}
		else
			split_at_datum = arg;

		if (argtype != splitcolumn_type)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("invalid type '%s' for split_at argument", format_type_be(argtype)),
					 errdetail("The argument type must match the dimension \"%s\"",
							   NameStr(dim->fd.column_name))));

		have_split_at = true;
	}

	/* Serialize chunk creation around the root hypertable. NOTE: also taken
	 * in ts_chunk_find_or_create_without_cuts() below. */
	LockRelationOid(ht->main_table_relid, ShareUpdateExclusiveLock);

	/*
	 * Find the existing partition slice for the chunk being split.
	 */
	DimensionSlice *slice = NULL;
	Hypercube *new_cube = ts_hypercube_copy(chunk->cube);

	for (int i = 0; i < new_cube->num_slices; i++)
	{
		DimensionSlice *curr_slice = new_cube->slices[i];

		if (curr_slice->fd.dimension_id == dim->fd.id)
		{
			slice = curr_slice;
			break;
		}
	}

	Ensure(slice, "no chunk slice for dimension %s", NameStr(dim->fd.column_name));

	/*
	 * Pick split point and calculate new ranges. If no split point is given
	 * by the user, then split in the middle.
	 */
	int64 interval_range = slice->fd.range_end - slice->fd.range_start;
	int64 split_at = 0;

	if (have_split_at)
	{
		Datum dim_datum;

		if (NULL != dim->partitioning)
			dim_datum =
				ts_partitioning_func_apply(dim->partitioning, C_COLLATION_OID, split_at_datum);
		else
			dim_datum = split_at_datum;

		split_at = ts_time_value_to_internal(dim_datum, splitdim_type);

		/*
		 * Check that the split_at value actually produces a valid split. Note
		 * that range_start is inclusive while range_end is non-inclusive. The
		 * split_at value needs to produce partition ranges of at least length
		 * 1.
		 */
		if (split_at < (slice->fd.range_start + 1) || split_at > (slice->fd.range_end - 2))
		{
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("cannot split chunk at %s",
							ts_datum_to_string(dim_datum, splitdim_type))));
		}
	}
	else
		split_at = slice->fd.range_start + (interval_range / 2);

	elog(DEBUG1,
		 "splitting chunk %s at %s",
		 get_rel_name(relid),
		 ts_internal_to_time_string(split_at, splitdim_type));

	const CompressionSettings *compress_settings = ts_compression_settings_get(relid);
	int64 old_end = slice->fd.range_end;

	/* Update the slice range for the existing chunk */
	slice->fd.range_end = split_at;
	chunk_update_constraints(chunk, new_cube);

	/* Update the slice for the new chunk */
	slice->fd.range_start = split_at;
	slice->fd.range_end = old_end;
	slice->fd.id = 0; /* Must set to 0 to mark as new for it to be created */

	/* Make updated constraints visible */
	CommandCounterIncrement();

	/* Reread hypertable after constraints changed */
	ts_cache_release(&hcache);
	ht = ts_hypertable_cache_get_cache_and_entry(chunk->hypertable_relid, CACHE_FLAG_NONE, &hcache);
	bool created = false;
	Chunk *new_chunk = ts_chunk_find_or_create_without_cuts(ht,
															new_cube,
															NameStr(chunk->fd.schema_name),
															NULL,
															InvalidOid,
															&created);
	Ensure(created, "could not create chunk for split");
	Assert(new_chunk);

	Chunk *new_compressed_chunk = NULL;

	if (compress_settings != NULL)
	{
		Hypertable *ht_compressed = ts_hypertable_get_by_id(ht->fd.compressed_hypertable_id);
		new_compressed_chunk = create_compress_chunk(ht_compressed, new_chunk, InvalidOid);
		ts_trigger_create_all_on_chunk(new_compressed_chunk);
		ts_chunk_set_compressed_chunk(new_chunk, new_compressed_chunk->fd.id);
	}

	CommandCounterIncrement();

	DEBUG_WAITPOINT("split_chunk_before_tuple_routing");

	SplitPoint sp = {
		.point = split_at,
		.dim = hyperspace_get_open_dimension(ht->space, 0),
		.route_next_tuple = route_next_non_compressed_tuple,
	};

	/*
	 * Array of the heap Oids of the resulting relations. Those relations that
	 * will get a heap swap (i.e., the original chunk) has heap_swap set to
	 * true.
	 */
	SplitRelationInfo split_relations[SPLIT_FACTOR] = {
		[0] = { .relid = relid, .chunk_id = chunk->fd.id, .heap_swap = true },
		[1] = { .relid = new_chunk->table_id, .chunk_id = new_chunk->fd.id, .heap_swap = false }
	};
	SplitRelationInfo csplit_relations[SPLIT_FACTOR] = {};
	SplitRelationInfo *compressed_split_relations = NULL;

	/* Split and rewrite the compressed relation first, if one exists. */
	if (new_compressed_chunk)
	{
		int orderby_pos = ts_array_position(compress_settings->fd.orderby, NameStr(splitdim_name));
		Ensure(orderby_pos > 0,
			   "primary dimension \"%s\" is not in compression settings",
			   NameStr(splitdim_name));

		/*
		 * Get the attribute numbers for the primary dimension's min and max
		 * values in the compressed relation. We'll use these to get the time
		 * range of compressed segments in order to route segments to the
		 * right result chunk.
		 */
		const char *min_attname = column_segment_min_name(orderby_pos);
		const char *max_attname = column_segment_max_name(orderby_pos);

		CompressedSplitPoint csp = {
			.base = {
				.point = split_at,
				.dim = hyperspace_get_open_dimension(ht->space, 0),
				.route_next_tuple = route_next_compressed_tuple,
			},
			.attnum_min = get_attnum(compress_settings->fd.compress_relid, min_attname),
			.attnum_max = get_attnum(compress_settings->fd.compress_relid, max_attname),
			.attnum_count = get_attnum(compress_settings->fd.compress_relid, COMPRESSION_COLUMN_METADATA_COUNT_NAME),
			.noncompressed_tupdesc = CreateTupleDescCopy(RelationGetDescr(srcrel)),
		};

		csplit_relations[0] = (SplitRelationInfo){ .relid = compress_settings->fd.compress_relid,
												   .chunk_id = chunk->fd.compressed_chunk_id,
												   .heap_swap = true };
		csplit_relations[1] = (SplitRelationInfo){ .relid = new_compressed_chunk->table_id,
												   .chunk_id = new_chunk->fd.compressed_chunk_id,
												   .heap_swap = false };

		Relation compressed_rel =
			table_open(compress_settings->fd.compress_relid, AccessExclusiveLock);
		compressed_split_relations = csplit_relations;
		split_relation(compressed_rel, &csp.base, SPLIT_FACTOR, compressed_split_relations);
	}

	/* Now split the non-compressed relation */
	split_relation(srcrel, &sp, SPLIT_FACTOR, split_relations);

	ts_cache_release(&hcache);

	/* Update stats after split is done */
	update_chunk_stats_for_split(split_relations, compressed_split_relations, SPLIT_FACTOR);

	DEBUG_WAITPOINT("split_chunk_at_end");

	PG_RETURN_VOID();
}
