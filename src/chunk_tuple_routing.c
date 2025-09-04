/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <utils/rls.h>

#include "chunk_tuple_routing.h"
#include "cross_module_fn.h"
#include "debug_point.h"
#include "guc.h"
#include "hypercube.h"
#include "nodes/chunk_dispatch/chunk_insert_state.h"
#include "nodes/modify_hypertable.h"
#include "subspace_store.h"

static ChunkInsertState *chunk_insert_state_create(Oid chunk_relid, ChunkTupleRouting *ctr);

ChunkTupleRouting *
ts_chunk_tuple_routing_create(EState *estate, ResultRelInfo *rri)
{
	ChunkTupleRouting *ctr;

	/*
	 * Here we attempt to expend as little effort as possible in setting up
	 * the ChunkTupleRouting. Each partition's ResultRelInfo is built on
	 * demand, only when we actually need to route a tuple to that partition.
	 * The reason for this is that a common case is for INSERT to insert a
	 * single tuple into a partitioned table and this must be fast.
	 */
	ctr = (ChunkTupleRouting *) palloc0(sizeof(ChunkTupleRouting));
	ctr->hypertable_rri = rri;
	ctr->partition_root = rri->ri_RelationDesc;
	ctr->memcxt = CurrentMemoryContext;
	ctr->estate = estate;
	ctr->counters = palloc0(sizeof(SharedCounters));

	ctr->hypertable =
		ts_hypertable_cache_get_cache_and_entry(RelationGetRelid(rri->ri_RelationDesc),
												CACHE_FLAG_NONE,
												&ctr->hypertable_cache);
	ctr->subspace = ts_subspace_store_init(ctr->hypertable->space,
										   estate->es_query_cxt,
										   ts_guc_max_open_chunks_per_insert);

	return ctr;
}

void
ts_chunk_tuple_routing_destroy(ChunkTupleRouting *ctr)
{
	ts_subspace_store_free(ctr->subspace);
	ts_cache_release(&ctr->hypertable_cache);

	pfree(ctr);
}

static void
destroy_chunk_insert_state(void *cis)
{
	ts_chunk_insert_state_destroy((ChunkInsertState *) cis);
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

		/*
		 * Normally, for every row of the chunk except the first one, we expect
		 * the chunk to exist already. The "create" function would take a lock
		 * on the hypertable to serialize the concurrent chunk creation. Here we
		 * first use the "find" function to try to find the chunk without
		 * locking the hypertable. This serves as a fast path for the usual case
		 * where the chunk already exists.
		 */
		chunk = ts_hypertable_find_chunk_for_point(ctr->hypertable, point);

		/*
		 * Frozen chunks require at least PG14.
		 */
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
			chunk = ts_hypertable_create_chunk_for_point(ctr->hypertable, point);
			chunk_created = true;
		}

		Ensure(chunk, "no chunk found or created");

		if (ctr->create_compressed_chunk && !chunk->fd.compressed_chunk_id)
		{
			/*
			 * When we try to create a compressed chunk, we need to grab a lock on the
			 * chunk to synchronize with other concurrent insert operations trying to
			 * create the same compressed chunk.
			 */
			LockRelationOid(chunk->table_id, ShareUpdateExclusiveLock);
			chunk = ts_chunk_get_by_id(chunk->fd.id, CACHE_FLAG_NONE);
			/* recheck whether compressed chunk exists after acquiring the lock */
			if (!chunk->fd.compressed_chunk_id)
			{
				Hypertable *compressed_ht =
					ts_hypertable_get_by_id(ctr->hypertable->fd.compressed_hypertable_id);
				Chunk *compressed_chunk =
					ts_cm_functions->compression_chunk_create(compressed_ht, chunk);
				ts_chunk_set_compressed_chunk(chunk, compressed_chunk->fd.id);

				/* mark chunk as partial unless completely new chunk */
				if (!chunk_created)
					needs_partial = true;
			}
		}

		cis = chunk_insert_state_create(chunk->table_id, ctr);
		cis->needs_partial = needs_partial;
		ts_subspace_store_add(ctr->subspace, chunk->cube, cis, destroy_chunk_insert_state);
	}

	MemoryContextSwitchTo(old_context);

	Assert(cis != NULL);

	return cis;
}

static ChunkInsertState *
chunk_insert_state_create(Oid chunk_relid, ChunkTupleRouting *ctr)
{
	ChunkInsertState *state;
	Relation rel, parent_rel;
	MemoryContext cis_context = AllocSetContextCreate(ctr->estate->es_query_cxt,
													  "chunk insert state memory context",
													  ALLOCSET_DEFAULT_SIZES);
	ResultRelInfo *relinfo;
	const Chunk *chunk;

	/* permissions NOT checked here; were checked at hypertable level */
	if (check_enable_rls(chunk_relid, InvalidOid, false) == RLS_ENABLED)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("hypertables do not support row-level security")));

	/*
	 * Since we insert data and won't modify metadata, a RowExclusiveLock
	 * should be sufficient. This should conflict with any metadata-modifying
	 * operations as they should take higher-level locks (ShareLock and
	 * above).
	 */
	DEBUG_WAITPOINT("chunk_insert_before_lock");
	rel = table_open(chunk_relid, RowExclusiveLock);

	/*
	 * A concurrent chunk operation (e.g., compression) might have changed the
	 * chunk metadata before we got a lock, so re-read it.
	 *
	 * This works even in higher levels of isolation since catalog data is
	 * always read from latest snapshot.
	 */
	chunk = ts_chunk_get_by_relid(chunk_relid, true);
	Assert(chunk->relkind == RELKIND_RELATION);
	ts_chunk_validate_chunk_status_for_operation(chunk, CHUNK_INSERT, true);

	MemoryContext old_mcxt = MemoryContextSwitchTo(cis_context);
	relinfo = create_chunk_result_relation_info(ctr->hypertable_rri, rel, ctr->estate);

	state = palloc0(sizeof(ChunkInsertState));
	state->counters = ctr->counters;
	state->mctx = cis_context;
	state->rel = rel;
	state->result_relation_info = relinfo;
	state->estate = ctr->estate;
	ts_set_compression_status(state, chunk);

	if (relinfo->ri_RelationDesc->rd_rel->relhasindex && relinfo->ri_IndexRelationDescs == NULL)
	{
		bool speculative = false;
		if (ctr->mht_state && ctr->mht_state->mt->onConflictAction != ONCONFLICT_NONE)
			speculative = true;

		ExecOpenIndices(relinfo, speculative);
	}

	if (relinfo->ri_TrigDesc != NULL)
	{
		TriggerDesc *tg = relinfo->ri_TrigDesc;

		/* instead of triggers can only be created on VIEWs */
		Assert(!tg->trig_insert_instead_row);

		/*
		 * A statement that targets a parent table in an inheritance or
		 * partitioning hierarchy does not cause the statement-level triggers
		 * of affected child tables to be fired; only the parent table's
		 * statement-level triggers are fired. However, row-level triggers
		 * of any affected child tables will be fired.
		 * During chunk creation we only copy ROW trigger to chunks so
		 * statement triggers should not exist on chunks.
		 */
		if (tg->trig_insert_after_statement || tg->trig_insert_before_statement)
			elog(ERROR, "statement trigger on chunk table not supported");
	}

	parent_rel = table_open(ctr->hypertable->main_table_relid, AccessShareLock);
	state->hyper_to_chunk_map =
		convert_tuples_by_name(RelationGetDescr(parent_rel), RelationGetDescr(rel));

	/* Need a tuple table slot to store tuples going into this chunk. We don't
	 * want this slot tied to the executor's tuple table, since that would tie
	 * the slot's lifetime to the entire length of the execution and we want
	 * to be able to dynamically create and destroy chunk insert
	 * state. Otherwise, memory might blow up when there are many chunks being
	 * inserted into. This also means that the slot needs to be destroyed with
	 * the chunk insert state. */
	state->slot = MakeSingleTupleTableSlot(RelationGetDescr(relinfo->ri_RelationDesc),
										   table_slot_callbacks(relinfo->ri_RelationDesc));
	table_close(parent_rel, AccessShareLock);

	state->hypertable_relid = chunk->hypertable_relid;
	state->chunk_id = chunk->fd.id;

	MemoryContextSwitchTo(old_mcxt);

	return state;
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
