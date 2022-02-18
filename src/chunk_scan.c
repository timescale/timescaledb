/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <catalog/namespace.h>
#include <storage/lmgr.h>
#include <utils/syscache.h>
#include <utils/builtins.h>

#include "debug_point.h"
#include "dimension_vector.h"
#include "hypertable.h"
#include "hypercube.h"
#include "scan_iterator.h"
#include "chunk_scan.h"
#include "chunk.h"
#include "chunk_constraint.h"
#include "ts_catalog/chunk_data_node.h"

/*
 * Find the chunks that match a query.
 *
 * The input is a set of dimension vectors that contain the dimension slices
 * that match a query. Each dimension vector contains all matching dimension
 * slices in one particular dimension.
 *
 * The output is a list of chunks (in the form of partial chunk stubs) whose
 * complete set of dimension slices exist in the given dimension vectors. In
 * other words, we only care about the chunks that match in all dimensions.
 */
static List *
scan_stubs_by_constraints(ScanIterator *constr_it, const Hyperspace *hs, const List *dimension_vecs,
						  MemoryContext per_tuple_mcxt)
{
	ListCell *lc;
	HTAB *htab;
	struct HASHCTL hctl = {
		.keysize = sizeof(int32),
		.entrysize = sizeof(ChunkScanEntry),
		.hcxt = CurrentMemoryContext,
	};
	List *chunk_stubs = NIL;

	htab = hash_create("chunk-stubs-hash", 20, &hctl, HASH_ELEM | HASH_CONTEXT | HASH_BLOBS);

	/*
	 * Scan for chunk constraints that reference the slices in the dimension
	 * vectors. Collect the chunk constraints in a hash table keyed on chunk
	 * ID. After the scan, there will be some chunk IDs in the hash table that
	 * have a complete set of constraints (one for each dimension). These are
	 * the chunks that match the query.
	 */
	foreach (lc, dimension_vecs)
	{
		const DimensionVec *vec = lfirst(lc);
		int i;

		for (i = 0; i < vec->num_slices; i++)
		{
			const DimensionSlice *slice = vec->slices[i];
			ChunkStub *stub;

			ts_chunk_constraint_scan_iterator_set_slice_id(constr_it, slice->fd.id);
			ts_scan_iterator_start_or_restart_scan(constr_it);

			while (ts_scan_iterator_next(constr_it) != NULL)
			{
				bool isnull, found;
				TupleInfo *ti = ts_scan_iterator_tuple_info(constr_it);
				Datum chunk_id_datum;
				int32 chunk_id;
				MemoryContext old_mcxt;
				ChunkScanEntry *entry;

				old_mcxt = MemoryContextSwitchTo(per_tuple_mcxt);
				MemoryContextReset(per_tuple_mcxt);

				chunk_id_datum = slot_getattr(ti->slot, Anum_chunk_constraint_chunk_id, &isnull);
				Assert(!isnull);
				chunk_id = DatumGetInt32(chunk_id_datum);

				if (slot_attisnull(ts_scan_iterator_slot(constr_it),
								   Anum_chunk_constraint_dimension_slice_id))
					continue;

				entry = hash_search(htab, &chunk_id, HASH_ENTER, &found);
				MemoryContextSwitchTo(ti->mctx);

				if (!found)
				{
					stub = ts_chunk_stub_create(chunk_id, hs->num_dimensions);
					stub->cube = ts_hypercube_alloc(hs->num_dimensions);
					entry->stub = stub;
				}
				else
					stub = entry->stub;

				ts_chunk_constraints_add_from_tuple(stub->constraints, ti);
				ts_hypercube_add_slice(stub->cube, slice);
				MemoryContextSwitchTo(old_mcxt);

				/* A stub is complete when we've added constraints for all its
				 * dimensions */
				if (chunk_stub_is_complete(stub, hs))
				{
					/* The hypercube should also be complete */
					Assert(stub->cube->num_slices == hs->num_dimensions);
					/* Slices should be in dimension ID order */
					ts_hypercube_slice_sort(stub->cube);
					chunk_stubs = lappend(chunk_stubs, stub);
				}
			}
		}
	}

	hash_destroy(htab);

	return chunk_stubs;
}

/*
 * Scan for chunks matching a query.
 *
 * Given a number of dimension slices that match a query (a vector of slices
 * is given for each dimension), find the chunks that reference one slice in
 * each of the given dimensions. The matching chunks are built across multiple
 * scans:
 *
 * 1. Dimensional chunk constraints
 * 2. Chunk metadata
 * 3. Additional chunk constraints
 * 4. Chunk data nodes
 *
 * For performance, try not to interleave scans of different metadata tables
 * in order to maintain data locality while scanning. Also, keep scanned
 * tables and indexes open until all the metadata is scanned for all chunks.
 */
Chunk **
ts_chunk_scan_by_constraints(const Hyperspace *hs, const List *dimension_vecs,
							 LOCKMODE chunk_lockmode, unsigned int *numchunks)
{
	MemoryContext work_mcxt =
		AllocSetContextCreate(CurrentMemoryContext, "chunk-scan-work", ALLOCSET_DEFAULT_SIZES);
	MemoryContext per_tuple_mcxt =
		AllocSetContextCreate(work_mcxt, "chunk-scan-per-tuple", ALLOCSET_SMALL_SIZES);
	MemoryContext orig_mcxt;
	ScanIterator constr_it;
	ScanIterator chunk_it;
	Chunk **chunks = NULL;
	Chunk **unlocked_chunks = NULL;
	unsigned int chunk_count = 0;
	unsigned int unlocked_chunk_count = 0;
	List *chunk_stubs;
	ListCell *lc;
	int remote_chunk_count = 0;
	int i = 0;
	bool chunk_sort_needed = false;
	Oid prev_chunk_oid = InvalidOid;

	Assert(OidIsValid(hs->main_table_relid));
	orig_mcxt = MemoryContextSwitchTo(work_mcxt);

	/*
	 * Step 1: Scan for chunks that match in all the given dimensions. The
	 * matching chunks are returned as chunk stubs as they are not yet full
	 * chunks.
	 */
	constr_it = ts_chunk_constraint_scan_iterator_create(orig_mcxt);
	chunk_stubs = scan_stubs_by_constraints(&constr_it, hs, dimension_vecs, per_tuple_mcxt);

	/*
	 * If no chunks matched, return early.
	 */
	if (list_length(chunk_stubs) == 0)
	{
		ts_scan_iterator_close(&constr_it);
		MemoryContextSwitchTo(orig_mcxt);
		MemoryContextDelete(work_mcxt);

		if (numchunks)
			*numchunks = 0;

		return NULL;
	}

	/*
	 * Step 2: For each matching chunk, fill in the metadata from the "chunk"
	 * table. Make sure to filter out "dropped" chunks..
	 */
	chunk_it = ts_chunk_scan_iterator_create(orig_mcxt);
	unlocked_chunks = MemoryContextAlloc(work_mcxt, sizeof(Chunk *) * list_length(chunk_stubs));

	foreach (lc, chunk_stubs)
	{
		const ChunkStub *stub = lfirst(lc);
		TupleInfo *ti;

		Assert(CurrentMemoryContext == work_mcxt);
		Assert(chunk_stub_is_complete(stub, hs));

		ts_chunk_scan_iterator_set_chunk_id(&chunk_it, stub->id);
		ts_scan_iterator_start_or_restart_scan(&chunk_it);
		ti = ts_scan_iterator_next(&chunk_it);

		if (ti)
		{
			bool isnull;
			Datum datum = slot_getattr(ti->slot, Anum_chunk_dropped, &isnull);
			bool is_dropped = isnull ? false : DatumGetBool(datum);

			MemoryContextSwitchTo(per_tuple_mcxt);
			MemoryContextReset(per_tuple_mcxt);

			if (!is_dropped)
			{
				Chunk *chunk;
				MemoryContext old_mcxt;
				Oid schema_oid;

				chunk = MemoryContextAllocZero(ti->mctx, sizeof(Chunk));

				/* The stub constraints only contain dimensional
				 * constraints. Use them for now and scan for other
				 * constraints in the next step below. */
				chunk->constraints = stub->constraints;

				/* Copy the hypercube into the result memory context */
				old_mcxt = MemoryContextSwitchTo(ti->mctx);
				ts_chunk_formdata_fill(&chunk->fd, ti);
				chunk->cube = ts_hypercube_copy(stub->cube);
				MemoryContextSwitchTo(old_mcxt);

				schema_oid = get_namespace_oid(NameStr(chunk->fd.schema_name), false);
				chunk->table_id = get_relname_relid(NameStr(chunk->fd.table_name), schema_oid);
				chunk->hypertable_relid = hs->main_table_relid;
				chunk->relkind = get_rel_relkind(chunk->table_id);
				Assert(OidIsValid(chunk->table_id));
				unlocked_chunks[unlocked_chunk_count] = chunk;
				unlocked_chunk_count++;

				/*
				 * Try to detect when sorting is needed for locking
				 * purposes. Sometimes, the chunk order resulting from
				 * scanning will align with the Oid order and no locking is
				 * needed.
				 */
				if (OidIsValid(prev_chunk_oid) && prev_chunk_oid > chunk->table_id)
					chunk_sort_needed = true;

				prev_chunk_oid = chunk->table_id;
			}

			/* Only one chunk should match */
			Assert(ts_scan_iterator_next(&chunk_it) == NULL);
			MemoryContextSwitchTo(work_mcxt);
		}
	}

	ts_scan_iterator_close(&chunk_it);

	Assert(chunks == NULL || chunk_count > 0);
	Assert(chunk_count <= list_length(chunk_stubs));
	Assert(CurrentMemoryContext == work_mcxt);

	/*
	 * Lock chunks in Oid order in order to avoid deadlocks. Since we
	 * sometimes fall back to PG find_inheritance_children() for hypertable
	 * expansion, we must lock in the same order as in that function to be
	 * consistent. Make sure to ignore the chunks that were concurrently
	 * dropped before we could get a lock.
	 */
	if (unlocked_chunk_count > 1 && chunk_sort_needed)
		qsort(unlocked_chunks, unlocked_chunk_count, sizeof(Chunk *), ts_chunk_oid_cmp);

	DEBUG_WAITPOINT("expanded_chunks");

	for (i = 0; i < unlocked_chunk_count; i++)
	{
		Chunk *chunk = unlocked_chunks[i];

		if (ts_chunk_lock_if_exists(chunk->table_id, chunk_lockmode))
		{
			/* Lazy initialize the chunks array */
			if (NULL == chunks)
				chunks = MemoryContextAlloc(orig_mcxt, sizeof(Chunk *) * unlocked_chunk_count);

			chunks[chunk_count] = chunk;

			if (chunk->relkind == RELKIND_FOREIGN_TABLE)
				remote_chunk_count++;

			chunk_count++;
		}
	}

	/*
	 * Step 3: The chunk stub scan only contained dimensional
	 * constraints. Scan the chunk constraints again to get all
	 * constraints.
	 */
	for (i = 0; i < chunk_count; i++)
	{
		Chunk *chunk = chunks[i];
		int num_constraints_hint = chunk->constraints->num_constraints;

		chunk->constraints = ts_chunk_constraints_alloc(num_constraints_hint, orig_mcxt);

		ts_chunk_constraint_scan_iterator_set_chunk_id(&constr_it, chunk->fd.id);
		ts_scan_iterator_rescan(&constr_it);

		while (ts_scan_iterator_next(&constr_it) != NULL)
		{
			TupleInfo *constr_ti = ts_scan_iterator_tuple_info(&constr_it);
			MemoryContextSwitchTo(per_tuple_mcxt);
			ts_chunk_constraints_add_from_tuple(chunk->constraints, constr_ti);
			MemoryContextSwitchTo(work_mcxt);
		}
	}

	Assert(CurrentMemoryContext == work_mcxt);
	ts_scan_iterator_close(&constr_it);

	/*
	 * Step 4: Fill in data nodes for remote chunks.
	 *
	 * Avoid the loop if there are no remote chunks. (Typically, either all
	 * chunks are remote chunks or none are.)
	 */
	if (remote_chunk_count > 0)
	{
		ScanIterator data_node_it = ts_chunk_data_nodes_scan_iterator_create(orig_mcxt);

		for (i = 0; i < chunk_count; i++)
		{
			Chunk *chunk = chunks[i];

			if (chunk->relkind == RELKIND_FOREIGN_TABLE)
			{
				/* Must start or restart the scan on the longer-lived context */
				ts_chunk_data_nodes_scan_iterator_set_chunk_id(&data_node_it, chunk->fd.id);
				ts_scan_iterator_start_or_restart_scan(&data_node_it);

				while (ts_scan_iterator_next(&data_node_it) != NULL)
				{
					bool should_free;
					TupleInfo *ti = ts_scan_iterator_tuple_info(&data_node_it);
					ChunkDataNode *chunk_data_node;
					Form_chunk_data_node form;
					MemoryContext old_mcxt;
					HeapTuple tuple;

					MemoryContextSwitchTo(per_tuple_mcxt);
					MemoryContextReset(per_tuple_mcxt);

					tuple = ts_scanner_fetch_heap_tuple(ti, false, &should_free);
					form = (Form_chunk_data_node) GETSTRUCT(tuple);
					old_mcxt = MemoryContextSwitchTo(ti->mctx);
					chunk_data_node = palloc(sizeof(ChunkDataNode));
					memcpy(&chunk_data_node->fd, form, sizeof(FormData_chunk_data_node));
					chunk_data_node->foreign_server_oid =
						get_foreign_server_oid(NameStr(form->node_name),
											   /* missing_ok = */ false);
					chunk->data_nodes = lappend(chunk->data_nodes, chunk_data_node);
					MemoryContextSwitchTo(old_mcxt);

					if (should_free)
						heap_freetuple(tuple);

					MemoryContextSwitchTo(work_mcxt);
				}
			}
		}

		ts_scan_iterator_close(&data_node_it);
	}

	if (numchunks)
		*numchunks = chunk_count;

	MemoryContextSwitchTo(orig_mcxt);
	MemoryContextDelete(work_mcxt);

#ifdef USE_ASSERT_CHECKING
	/* Assert that we always return valid chunks */
	for (i = 0; i < chunk_count; i++)
	{
		ASSERT_IS_VALID_CHUNK(chunks[i]);
	}
#endif

	return chunks;
}
