/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <access/tableam.h>
#include <storage/lockdefs.h>
#include <utils/rls.h>

#include "chunk_insert_state.h"
#include "chunk_tuple_routing.h"
#include "cross_module_fn.h"
#include "debug_point.h"
#include "guc.h"
#include "hypercube.h"
#include "subspace_store.h"

ChunkTupleRouting *
ts_chunk_tuple_routing_create(EState *estate, Hypertable *ht, ResultRelInfo *rri)
{
	ChunkTupleRouting *ctr;
	Assert(ht);

	/*
	 * Here we attempt to expend as little effort as possible in setting up
	 * the ChunkTupleRouting. Each partition's ResultRelInfo is built on
	 * demand, only when we actually need to route a tuple to that partition.
	 * The reason for this is that a common case is for INSERT to insert a
	 * single tuple into a partitioned table and this must be fast.
	 */
	ctr = (ChunkTupleRouting *) palloc0(sizeof(ChunkTupleRouting));
	ctr->root_rri = rri;
	ctr->root_rel = rri->ri_RelationDesc;
	ctr->estate = estate;
	ctr->counters = palloc0(sizeof(SharedCounters));
	ctr->hypertable = ht;
	/*
	 * If the relid of ResultRelInfo does not match the Hypertable this is an operation
	 * directly on a chunk.
	 */
	ctr->single_chunk_insert = ht->main_table_relid != RelationGetRelid(rri->ri_RelationDesc);

	ctr->subspace = ts_subspace_store_init(ctr->hypertable->space,
										   estate->es_query_cxt,
										   ts_guc_max_open_chunks_per_insert);

	ctr->has_dropped_attrs = false;

	return ctr;
}

void
ts_chunk_tuple_routing_destroy(ChunkTupleRouting *ctr)
{
	ts_subspace_store_free(ctr->subspace);

	pfree(ctr);
}

static void
destroy_chunk_insert_state_single_chunk(void *cis)
{
	ts_chunk_insert_state_destroy((ChunkInsertState *) cis, true);
}

static void
destroy_chunk_insert_state(void *cis)
{
	ts_chunk_insert_state_destroy((ChunkInsertState *) cis, false);
}

extern ChunkInsertState *
ts_chunk_tuple_routing_find_chunk(ChunkTupleRouting *ctr, Point *point)
{
	Chunk *chunk = NULL;
	ChunkInsertState *cis = NULL;
	cis = ts_subspace_store_get(ctr->subspace, point);

	/*
	 * The chunk search functions may leak memory, so switch to a temporary
	 * memory context.
	 */
	MemoryContext old_context = MemoryContextSwitchTo(GetPerTupleMemoryContext(ctr->estate));

	if (!cis)
	{
		bool chunk_created = false;
		bool needs_partial = false;
		const LOCKMODE lockmode = RowExclusiveLock;

		/*
		 * Normally, for every row of the chunk except the first one, we expect
		 * the chunk to exist already. The "create" function would take a lock
		 * on the hypertable to serialize the concurrent chunk creation. Here we
		 * first use the "find" function to try to find the chunk without
		 * locking the hypertable. This serves as a fast path for the usual case
		 * where the chunk already exists.
		 */
		DEBUG_WAITPOINT("chunk_insert_before_lock");
		chunk = ts_hypertable_find_chunk_for_point(ctr->hypertable, point, lockmode);

		/*
		 * When inserting directly into a chunk, we should always find the chunk and
		 * the returned chunk should match the relid we are inserting into.
		 */
		if (ctr->single_chunk_insert)
		{
			if (!chunk || chunk->table_id != RelationGetRelid(ctr->root_rri->ri_RelationDesc))
				ereport(ERROR,
						(errcode(ERRCODE_CHECK_VIOLATION),
						 errmsg("new row for relation \"%s\" violates chunk constraint",
								RelationGetRelationName(ctr->root_rri->ri_RelationDesc))));
		}

		if (chunk && ts_chunk_is_frozen(chunk))
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("cannot INSERT into frozen chunk \"%s\"",
							get_rel_name(chunk->table_id))));

		if (chunk && IS_OSM_CHUNK(chunk))
		{
			const Dimension *time_dim = hyperspace_get_open_dimension(ctr->hypertable->space, 0);
			Assert(time_dim != NULL);

			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("Cannot insert into tiered chunk range of %s.%s - attempt to create "
							"new chunk "
							"with range  [%s %s] failed",
							NameStr(ctr->hypertable->fd.schema_name),
							NameStr(ctr->hypertable->fd.table_name),
							ts_internal_to_time_string(chunk->cube->slices[0]->fd.range_start,
													   time_dim->fd.column_type),
							ts_internal_to_time_string(chunk->cube->slices[0]->fd.range_end,
													   time_dim->fd.column_type)),
					 errhint(
						 "Hypertable has tiered data with time range that overlaps the insert")));
		}

		if (!chunk)
		{
			chunk = ts_hypertable_create_chunk_for_point(ctr->hypertable, point, lockmode);
			chunk_created = true;
		}

		Ensure(chunk, "no chunk found or created");

#ifdef USE_ASSERT_CHECKING
		/* Ensure we always hold a lock on the chunk table at this point */
		Relation chunk_rel = RelationIdGetRelation(chunk->table_id);
		Assert(CheckRelationLockedByMe(chunk_rel, lockmode, true));
		RelationClose(chunk_rel);
#endif
		if (ctr->create_compressed_chunk && !chunk->fd.compressed_chunk_id)
		{
			/*
			 * When creating a compressed chunk, the operation must be
			 * synchronized with other operations. A RowExclusiveLock is
			 * already held on the chunk table itself so it will conflict with
			 * explicit compress calls like compress_chunk() or
			 * convert_to_columnstore() that take at least
			 * ExclusiveLock. However, it is also necessary to synchronize
			 * with other concurrent inserts doing the same thing.
			 *
			 * We don't want to do a lock upgrade on the chunk table since
			 * that increases the risk of deadlocks.
			 *
			 * Instead we synchronize around a tuple lock on the chunk
			 * metadata row since this is the row getting updated with new
			 * compression status.
			 */
			TM_Result lockres;

			DEBUG_WAITPOINT("insert_create_compressed");

			lockres = ts_chunk_lock_for_creating_compressed_chunk(chunk->fd.id,
																  &chunk->fd.compressed_chunk_id);

			/*
			 * Since the locking function blocks and follows the update chain,
			 * the only reasonable return value is TM_Ok. Everything else is
			 * an error.
			 */
			Ensure(lockres == TM_Ok,
				   "could not lock chunk row for creating "
				   "compressed chunk. Lock result %d",
				   lockres);

			/* recheck whether compressed chunk exists after acquiring the lock */
			if (!chunk->fd.compressed_chunk_id)
			{
				Hypertable *compressed_ht =
					ts_hypertable_get_by_id(ctr->hypertable->fd.compressed_hypertable_id);
				Chunk *compressed_chunk =
					ts_cm_functions->compression_chunk_create(compressed_ht, chunk);
				ts_chunk_set_compressed_chunk(chunk, compressed_chunk->fd.id);
				chunk->fd.compressed_chunk_id = compressed_chunk->fd.id;

				/* mark chunk as partial unless completely new chunk */
				if (!chunk_created)
					needs_partial = true;
			}
		}

		cis = ts_chunk_insert_state_create(chunk->table_id, ctr);
		cis->needs_partial = needs_partial;
		ts_subspace_store_add(ctr->subspace,
							  chunk->cube,
							  cis,
							  ctr->single_chunk_insert ? destroy_chunk_insert_state_single_chunk :
														 destroy_chunk_insert_state);
	}

	MemoryContextSwitchTo(old_context);

	Assert(cis != NULL);

	return cis;
}

extern void
ts_chunk_tuple_routing_decompress_for_insert(ChunkInsertState *cis, TupleTableSlot *slot,
											 EState *estate, bool update_counter)
{
	if (!cis->chunk_compressed || (cis->cached_decompression_state &&
								   !cis->cached_decompression_state->has_primary_or_unique_index))
		return;

	/*
	 * If this is an INSERT into a compressed chunk with UNIQUE or
	 * PRIMARY KEY constraints we need to make sure any batches that could
	 * potentially lead to a conflict are in the decompressed chunk so
	 * postgres can do proper constraint checking.
	 */

	ts_cm_functions->init_decompress_state_for_insert(cis, slot);

	/* If we are dealing with generated stored columns, generate the values
	 * so can use it for uniqueness checks.
	 */
	Relation resultRelationDesc = cis->result_relation_info->ri_RelationDesc;
	if (resultRelationDesc->rd_att->constr &&
		resultRelationDesc->rd_att->constr->has_generated_stored)
	{
		slot->tts_tableOid = RelationGetRelid(resultRelationDesc);
		ExecComputeStoredGenerated(cis->result_relation_info, estate, slot, CMD_INSERT);
		cis->skip_generated_column_computations = true;
	}
	ts_cm_functions->decompress_batches_for_insert(cis, slot);

	/* mark rows visible */
	if (update_counter)
		estate->es_output_cid = GetCurrentCommandId(true);

	if (ts_guc_max_tuples_decompressed_per_dml > 0)
	{
		if (cis->counters->tuples_decompressed > ts_guc_max_tuples_decompressed_per_dml)
		{
			ereport(ERROR,
					(errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
					 errmsg("tuple decompression limit exceeded by operation"),
					 errdetail("current limit: %d, tuples decompressed: %lld",
							   ts_guc_max_tuples_decompressed_per_dml,
							   (long long int) cis->counters->tuples_decompressed),
					 errhint("Consider increasing "
							 "timescaledb.max_tuples_decompressed_per_dml_transaction or set "
							 "to 0 (unlimited).")));
		}
	}
}
