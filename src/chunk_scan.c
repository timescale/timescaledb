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
#include "guc.h"
#include "hypertable.h"
#include "hypercube.h"
#include "scan_iterator.h"
#include "chunk_scan.h"
#include "chunk.h"
#include "chunk_constraint.h"
#include "ts_catalog/chunk_data_node.h"
#include "utils.h"

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
ts_chunk_scan_by_chunk_ids(const Hyperspace *hs, const List *chunk_ids, unsigned int *num_chunks)
{
	MemoryContext work_mcxt =
		AllocSetContextCreate(CurrentMemoryContext, "chunk-scan-work", ALLOCSET_DEFAULT_SIZES);
	Chunk **locked_chunks = NULL;
	int locked_chunk_count = 0;
	ListCell *lc;
	int remote_chunk_count = 0;

	Assert(OidIsValid(hs->main_table_relid));
	MemoryContext orig_mcxt = MemoryContextSwitchTo(work_mcxt);

	/*
	 * For each matching chunk, fill in the metadata from the "chunk" table.
	 * Make sure to filter out "dropped" chunks.
	 */
	ScanIterator chunk_it = ts_chunk_scan_iterator_create(orig_mcxt);
	locked_chunks = MemoryContextAlloc(orig_mcxt, sizeof(Chunk *) * list_length(chunk_ids));
	foreach (lc, chunk_ids)
	{
		int chunk_id = lfirst_int(lc);

		Assert(CurrentMemoryContext == work_mcxt);

		ts_chunk_scan_iterator_set_chunk_id(&chunk_it, chunk_id);
		ts_scan_iterator_start_or_restart_scan(&chunk_it);
		TupleInfo *ti = ts_scan_iterator_next(&chunk_it);
		if (ti == NULL)
		{
			continue;
		}
		bool isnull;
		Datum datum = slot_getattr(ti->slot, Anum_chunk_dropped, &isnull);
		const bool is_dropped = isnull ? false : DatumGetBool(datum);
		if (is_dropped)
		{
			continue;
		}

		/* We found a chunk that is not dropped. First, try to lock it. */
		Name schema_name = DatumGetName(slot_getattr(ti->slot, Anum_chunk_schema_name, &isnull));
		Assert(!isnull);
		Name table_name = DatumGetName(slot_getattr(ti->slot, Anum_chunk_table_name, &isnull));
		Assert(!isnull);

		Oid chunk_reloid = ts_get_relation_relid(NameStr(*schema_name),
												 NameStr(*table_name),
												 /* return_invalid = */ false);
		Assert(OidIsValid(chunk_reloid));

		/* Only one chunk should match */
		Assert(ts_scan_iterator_next(&chunk_it) == NULL);

		DEBUG_WAITPOINT("hypertable_expansion_before_lock_chunk");
		if (!ts_chunk_lock_if_exists(chunk_reloid, AccessShareLock))
		{
			continue;
		}

		/*
		 * Now after we have locked the chunk, we have to reread its metadata.
		 * It might have been modified concurrently by decompression, for
		 * example.
		 */
		ts_chunk_scan_iterator_set_chunk_id(&chunk_it, chunk_id);
		ts_scan_iterator_start_or_restart_scan(&chunk_it);
		ti = ts_scan_iterator_next(&chunk_it);
		Assert(ti != NULL);
		Chunk *chunk = MemoryContextAllocZero(orig_mcxt, sizeof(Chunk));

		ts_chunk_formdata_fill(&chunk->fd, ti);

		chunk->constraints = NULL;
		chunk->cube = NULL;
		chunk->hypertable_relid = hs->main_table_relid;
		chunk->table_id = chunk_reloid;

		locked_chunks[locked_chunk_count] = chunk;
		locked_chunk_count++;

		/* Only one chunk should match */
		Assert(ts_scan_iterator_next(&chunk_it) == NULL);
	}

	ts_scan_iterator_close(&chunk_it);

	Assert(locked_chunk_count == 0 || locked_chunks != NULL);
	Assert(locked_chunk_count <= list_length(chunk_ids));
	Assert(CurrentMemoryContext == work_mcxt);

	for (int i = 0; i < locked_chunk_count; i++)
	{
		Chunk *chunk = locked_chunks[i];
		chunk->relkind = get_rel_relkind(chunk->table_id);
		if (chunk->relkind == RELKIND_FOREIGN_TABLE)
			remote_chunk_count++;
	}

	/*
	 * Fetch the chunk constraints.
	 */
	ScanIterator constr_it = ts_chunk_constraint_scan_iterator_create(orig_mcxt);

	for (int i = 0; i < locked_chunk_count; i++)
	{
		Chunk *chunk = locked_chunks[i];
		chunk->constraints = ts_chunk_constraints_alloc(/* size_hint = */ 0, orig_mcxt);

		ts_chunk_constraint_scan_iterator_set_chunk_id(&constr_it, chunk->fd.id);
		ts_scan_iterator_start_or_restart_scan(&constr_it);

		while (ts_scan_iterator_next(&constr_it) != NULL)
		{
			TupleInfo *constr_ti = ts_scan_iterator_tuple_info(&constr_it);
			ts_chunk_constraints_add_from_tuple(chunk->constraints, constr_ti);
		}
	}
	ts_scan_iterator_close(&constr_it);

	/*
	 * Build hypercubes for the chunks by finding and combining the dimension
	 * slices that match the chunk constraints.
	 */
	ScanIterator slice_iterator = ts_dimension_slice_scan_iterator_create(NULL, orig_mcxt);
	for (int chunk_index = 0; chunk_index < locked_chunk_count; chunk_index++)
	{
		Chunk *chunk = locked_chunks[chunk_index];
		ChunkConstraints *constraints = chunk->constraints;
		MemoryContextSwitchTo(orig_mcxt);
		Hypercube *cube = ts_hypercube_alloc(constraints->num_dimension_constraints);
		MemoryContextSwitchTo(work_mcxt);
		for (int constraint_index = 0; constraint_index < constraints->num_constraints;
			 constraint_index++)
		{
			ChunkConstraint *constraint = &constraints->constraints[constraint_index];
			if (!is_dimension_constraint(constraint))
			{
				continue;
			}

			/*
			 * Find the slice by id. Don't have to lock it because the chunk is
			 * locked.
			 */
			const int slice_id = constraint->fd.dimension_slice_id;
			DimensionSlice *slice_ptr =
				ts_dimension_slice_scan_iterator_get_by_id(&slice_iterator,
														   slice_id,
														   /* tuplock = */ NULL);
			if (slice_ptr == NULL)
			{
				elog(ERROR, "dimension slice %d is not found", slice_id);
			}
			MemoryContextSwitchTo(orig_mcxt);
			DimensionSlice *slice_copy = ts_dimension_slice_create(slice_ptr->fd.dimension_id,
																   slice_ptr->fd.range_start,
																   slice_ptr->fd.range_end);
			slice_copy->fd.id = slice_ptr->fd.id;
			MemoryContextSwitchTo(work_mcxt);
			Assert(cube->capacity > cube->num_slices);
			cube->slices[cube->num_slices++] = slice_copy;
		}
		ts_hypercube_slice_sort(cube);
		chunk->cube = cube;
	}
	ts_scan_iterator_close(&slice_iterator);

	Assert(CurrentMemoryContext == work_mcxt);

	/*
	 * Fill in data nodes for remote chunks.
	 *
	 * Avoid the loop if there are no remote chunks. (Typically, either all
	 * chunks are remote chunks or none are.)
	 */
	if (remote_chunk_count > 0)
	{
		ScanIterator data_node_it = ts_chunk_data_nodes_scan_iterator_create(orig_mcxt);

		for (int i = 0; i < locked_chunk_count; i++)
		{
			Chunk *chunk = locked_chunks[i];

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
				}
			}
		}

		ts_scan_iterator_close(&data_node_it);
	}

	MemoryContextSwitchTo(orig_mcxt);
	MemoryContextDelete(work_mcxt);

#ifdef USE_ASSERT_CHECKING
	/* Assert that we always return valid chunks */
	for (int i = 0; i < locked_chunk_count; i++)
	{
		ASSERT_IS_VALID_CHUNK(locked_chunks[i]);
	}
#endif

	*num_chunks = locked_chunk_count;
	Assert(*num_chunks == 0 || locked_chunks != NULL);
	return locked_chunks;
}
