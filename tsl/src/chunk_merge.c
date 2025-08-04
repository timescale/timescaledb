/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <access/multixact.h>
#include <catalog/catalog.h>
#include <catalog/dependency.h>
#include <catalog/heap.h>
#include <catalog/pg_am.h>
#include <catalog/pg_constraint.h>
#include <commands/tablecmds.h>
#include <storage/bufmgr.h>
#include <utils/acl.h>
#include <utils/syscache.h>

#include "chunk.h"
#include "debug_point.h"
#include "hypercube.h"
#include "ts_catalog/catalog.h"
#include "ts_catalog/compression_chunk_size.h"

typedef struct RelationMergeInfo
{
	Oid relid;
	struct VacuumCutoffs cutoffs;
	FormData_compression_chunk_size ccs;
	Chunk *chunk;
	Relation rel;
	char relpersistence;
	bool isresult;
	bool iscompressed_rel;
} RelationMergeInfo;

typedef enum MergeLockUpgrade
{
	MERGE_LOCK_UPGRADE,
	MERGE_LOCK_CONDITIONAL_UPGRADE,
	MERGE_LOCK_ACCESS_EXCLUSIVE,
} MergeLockUpgrade;

void
compute_rel_vacuum_cutoffs(Relation rel, struct VacuumCutoffs *cutoffs)
{
	VacuumParams params;

	memset(&params, 0, sizeof(VacuumParams));
	vacuum_get_cutoffs(rel, &params, cutoffs);

	/* Frozen Id should not go backwards */
	TransactionId relfrozenxid = rel->rd_rel->relfrozenxid;

	if (TransactionIdIsValid(relfrozenxid) &&
		TransactionIdPrecedes(cutoffs->FreezeLimit, relfrozenxid))
		cutoffs->FreezeLimit = relfrozenxid;

	MultiXactId relminmxid = rel->rd_rel->relminmxid;

	if (MultiXactIdIsValid(relminmxid) && MultiXactIdPrecedes(cutoffs->MultiXactCutoff, relminmxid))
		cutoffs->MultiXactCutoff = relminmxid;
}

static void
merge_chunks_finish(Oid new_relid, RelationMergeInfo *relinfos, int nrelids,
					MergeLockUpgrade lock_upgrade)
{
	RelationMergeInfo *result_minfo = NULL;

	/*
	 * The relations being merged are currently locked in ExclusiveLock, which
	 * means other readers can have locks. To delete the relations, we first
	 * need to upgrade to an exclusive lock. However, this might lead to
	 * deadlocks so we need to bail out if we cannot get the lock immediately.
	 */
	for (int i = 0; i < nrelids; i++)
	{
		Oid relid = relinfos[i].relid;

		if (relinfos[i].isresult)
			result_minfo = &relinfos[i];

		/* If merging internal compressed relations, not all chunks have one */
		if (!OidIsValid(relid))
			continue;

		switch (lock_upgrade)
		{
			case MERGE_LOCK_CONDITIONAL_UPGRADE:
				if (!ConditionalLockRelationOid(relid, AccessExclusiveLock))
					ereport(ERROR,
							(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
							 errmsg("could not lock relation \"%s\" for merge",
									get_rel_name(relid))));
				break;
			case MERGE_LOCK_UPGRADE:
				LockRelationOid(relid, AccessExclusiveLock);
				break;
			case MERGE_LOCK_ACCESS_EXCLUSIVE:
				/* We should already hold AccessExclusivelock. Could preventively
				 * take it or assert the lock is taken, but it would require
				 * opening the relation again. */
				break;
		}
	}

	Ensure(result_minfo != NULL, "no chunk to merge into found");
	struct VacuumCutoffs *cutoffs = &result_minfo->cutoffs;

	finish_heap_swap(result_minfo->relid,
					 new_relid,
					 false, /* system catalog */
					 false /* swap toast by content */,
					 false, /* check constraints */
					 true,	/* internal? */
					 cutoffs->FreezeLimit,
					 cutoffs->MultiXactCutoff,
					 result_minfo->relpersistence);

	/* Don't need to drop objects for internal compressed relations, they are
	 * dropped when the main chunk is dropped. */
	if (result_minfo->iscompressed_rel)
		return;

	if (ts_chunk_is_compressed(result_minfo->chunk))
		ts_chunk_set_partial(result_minfo->chunk);

	/*
	 * Delete all the merged relations except the result one, since we are
	 * keeping it for the heap swap.
	 */
	ObjectAddresses *objects = new_object_addresses();

	for (int i = 0; i < nrelids; i++)
	{
		Oid relid = relinfos[i].relid;
		ObjectAddress object = {
			.classId = RelationRelationId,
			.objectId = relid,
		};

		if (!OidIsValid(relid) || relinfos[i].isresult)
			continue;

		/* Cannot drop if relation is still open */
		Assert(relinfos[i].rel == NULL);

		if (relinfos[i].chunk)
		{
			const Oid namespaceid = get_rel_namespace(relid);
			const char *schemaname = get_namespace_name(namespaceid);
			const char *tablename = get_rel_name(relid);

			ts_chunk_delete_by_name(schemaname, tablename, DROP_RESTRICT);
		}

		add_exact_object_address(&object, objects);
	}

	performMultipleDeletions(objects, DROP_RESTRICT, PERFORM_DELETION_INTERNAL);
	free_object_addresses(objects);
}

static int
cmp_relations(const void *left, const void *right)
{
	const RelationMergeInfo *linfo = ((RelationMergeInfo *) left);
	const RelationMergeInfo *rinfo = ((RelationMergeInfo *) right);

	if (linfo->chunk && rinfo->chunk)
	{
		const Hypercube *lcube = linfo->chunk->cube;
		const Hypercube *rcube = rinfo->chunk->cube;

		Assert(lcube->num_slices == rcube->num_slices);

		for (int i = 0; i < lcube->num_slices; i++)
		{
			const DimensionSlice *lslice = lcube->slices[i];
			const DimensionSlice *rslice = rcube->slices[i];

			Assert(lslice->fd.dimension_id == rslice->fd.dimension_id);

			/* Compare start of range for the dimension */
			if (lslice->fd.range_start < rslice->fd.range_start)
				return -1;

			if (lslice->fd.range_start > rslice->fd.range_start)
				return 1;

			/* If start of range is equal, compare by end of range */
			if (lslice->fd.range_end < rslice->fd.range_end)
				return -1;

			if (lslice->fd.range_end > rslice->fd.range_end)
				return 1;
		}

		/* Should only reach here if partitioning is equal across all
		 * dimensions. Fall back to comparing relids. */
	}

	return pg_cmp_u32(linfo->relid, rinfo->relid);
}

/*
 * Check that the partition boundaries of two chunks align so that a new valid
 * hypercube can be formed if the chunks are merged. This check assumes that
 * the hypercubes are sorted so that cube2 "follows" cube1.
 *
 * The algorithm is simple and only allows merging along a single dimension in
 * the same merge. For example, these two cases are mergeable:
 *
 * ' ____
 * ' |__|
 * ' |__|
 *
 * ' _______
 * ' |__|__|
 *
 * while these cases are not mergeable:
 * '    ____
 * '  __|__|
 * ' |__|
 *
 * ' ______
 * ' |____|
 * ' |__|
 *
 *
 * The validation can handle merges of many chunks at once if they are
 * "naively" aligned and this function is called on chunk hypercubes in
 * "partition order":
 *
 * ' _____________
 * ' |__|__|__|__|
 *
 * However, the validation currently won't accept merges of multiple
 * dimensions at once:
 *
 * ' _____________
 * ' |__|__|__|__|
 * ' |__|__|__|__|
 *
 * It also cannot handle complicated merges of multi-dimensional partitioning
 * schemes like the one below.
 *
 * ' _________
 * ' |__a____|
 * ' |_b_|_c_|
 *
 * Merging a,b,c, should be possible but the validation currently cannot
 * handle such cases. Instead, it is necessary to first merge b,c. Then merge
 * a with the result (b,c) in a separate merge. Note that it is not possible
 * to merge only a,b or a,c.
 *
 * A future, more advanced, validation needs to handle corner-cases like the
 * one below that has gaps:
 *
 * ' _____________
 * ' |__|__|__|__|
 * ' |____|  |___|
 * '
 */
static void
validate_merge_possible(const Hypercube *cube1, const Hypercube *cube2)
{
	int follow_edges = 0;
	int equal_edges = 0;

	Assert(cube1->num_slices == cube2->num_slices);

	for (int i = 0; i < cube1->num_slices; i++)
	{
		const DimensionSlice *slice1 = cube1->slices[i];
		const DimensionSlice *slice2 = cube2->slices[i];

		if (ts_dimension_slices_equal(slice1, slice2))
			equal_edges++;

		if (slice1->fd.range_end == slice2->fd.range_start)
			follow_edges++;
	}

	if (follow_edges != 1 || (cube1->num_slices - equal_edges) != 1)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot create new chunk partition boundaries"),
				 errhint("Try merging chunks that have adjacent partitions.")));
}

static const ChunkConstraint *
get_chunk_constraint_by_slice_id(const ChunkConstraints *ccs, int32 slice_id)
{
	for (int i = 0; i < ccs->num_constraints; i++)
	{
		const ChunkConstraint *cc = &ccs->constraints[i];

		if (cc->fd.dimension_slice_id == slice_id)
			return cc;
	}

	return NULL;
}

void
chunk_update_constraints(const Chunk *chunk, const Hypercube *new_cube)
{
	Cache *hcache;
	const Hypertable *ht =
		ts_hypertable_cache_get_cache_and_entry(chunk->hypertable_relid, CACHE_FLAG_NONE, &hcache);
	List *new_constraints = NIL;

	for (int i = 0; i < new_cube->num_slices; i++)
	{
		const DimensionSlice *old_slice = chunk->cube->slices[i];
		DimensionSlice *new_slice = new_cube->slices[i];
		const ChunkConstraint *cc;
		ScanTupLock tuplock = {
			.waitpolicy = LockWaitBlock,
			.lockmode = LockTupleShare,
		};

		/* If nothing changed in this dimension, move on to the next */
		if (ts_dimension_slices_equal(old_slice, new_slice))
			continue;

		cc = get_chunk_constraint_by_slice_id(chunk->constraints, old_slice->fd.id);

		if (cc)
		{
			ObjectAddress constrobj = {
				.classId = ConstraintRelationId,
				.objectId = get_relation_constraint_oid(chunk->table_id,
														NameStr(cc->fd.constraint_name),
														false),
			};

			performDeletion(&constrobj, DROP_RESTRICT, 0);

			/* Create the new check constraint */
			const Dimension *dim =
				ts_hyperspace_get_dimension_by_id(ht->space, old_slice->fd.dimension_id);
			Constraint *constr =
				ts_chunk_constraint_dimensional_create(dim,
													   new_slice,
													   NameStr(cc->fd.constraint_name));

			/* Constraint could be NULL, e.g., if the merged chunk covers the
			 * entire range in a space dimension it needs no constraint. */
			if (constr != NULL)
				new_constraints = lappend(new_constraints, constr);
		}

		/* Check if there's already a slice with the new range. If so, avoid
		 * inserting a new slice. */
		if (!ts_dimension_slice_scan_for_existing(new_slice, &tuplock))
		{
			new_slice->fd.id = -1;
			ts_dimension_slice_insert(new_slice);
			/* A new Id should be assigned */
			Assert(new_slice->fd.id > 0);
		}

		/* Update the chunk constraint to point to the new slice ID */
		ts_chunk_constraint_update_slice_id(chunk->fd.id, old_slice->fd.id, new_slice->fd.id);

		/* Delete the old slice if it is orphaned now */
		if (ts_chunk_constraint_scan_by_dimension_slice_id(old_slice->fd.id,
														   NULL,
														   CurrentMemoryContext) == 0)
		{
			ts_dimension_slice_delete_by_id(old_slice->fd.id, false);
		}
	}

	/* Add new check constraints, if any */
	if (new_constraints != NIL)
	{
		/* Adding a constraint should require AccessExclusivelock. It should
		 * already be taken at this point, but specify it to be sure. */
		Relation rel = table_open(chunk->table_id, AccessExclusiveLock);
		AddRelationNewConstraints(rel,
								  NIL /* List *newColDefaults */,
								  new_constraints,
								  false /* allow_merge */,
								  true /* is_local */,
								  false /* is_internal */,
								  NULL /* query string */);
		table_close(rel, NoLock);
	}

	ts_cache_release(&hcache);
}

static void
merge_cubes(Hypercube *merged_cube, const Hypercube *cube)
{
	/* Merge dimension slices */
	for (int i = 0; i < cube->num_slices; i++)
	{
		const DimensionSlice *slice = cube->slices[i];
		DimensionSlice *merged_slice = merged_cube->slices[i];

		Assert(slice->fd.dimension_id == merged_slice->fd.dimension_id);

		if (slice->fd.range_start < merged_slice->fd.range_start)
			merged_slice->fd.range_start = slice->fd.range_start;

		if (slice->fd.range_end > merged_slice->fd.range_end)
			merged_slice->fd.range_end = slice->fd.range_end;
	}
}

/*
 * Get the locking mode for merge chunks.
 *
 * By default, a merge happens with access exclusive locks taken on chunks in
 * order to avoid deadlocks. It is possible to use a weaker exclusive lock by
 * setting a session variable, thus allowing reads during merges. However,
 * that can easily lead to deadlocks as shown in isolation tests. Therefore,
 * use the stricter locking settings by default.
 */
static MergeLockUpgrade
merge_chunks_lock_upgrade_mode(void)
{
	const char *lockupgrade =
		GetConfigOption("timescaledb.merge_chunks_lock_upgrade_mode", true, false);

	if (lockupgrade == NULL)
		return MERGE_LOCK_ACCESS_EXCLUSIVE;

	if (strcmp("upgrade", lockupgrade) == 0)
		return MERGE_LOCK_UPGRADE;

	if (strcmp("conditional", lockupgrade) == 0)
		return MERGE_LOCK_CONDITIONAL_UPGRADE;

	return MERGE_LOCK_ACCESS_EXCLUSIVE;
}

/*
 * Use anonymous settings value to disable multidim merges due to a bug in the
 * routing cache with non-aligned partitions/chunks.
 */
static bool
merge_chunks_multidim_allowed(void)
{
	const char *multidim_merge_enabled =
		GetConfigOption(MAKE_EXTOPTION("enable_merge_multidim_chunks"), true, false);

	if (multidim_merge_enabled == NULL)
		return false;

	return (pg_strcasecmp("on", multidim_merge_enabled) == 0 ||
			pg_strcasecmp("1", multidim_merge_enabled) == 0 ||
			pg_strcasecmp("true", multidim_merge_enabled) == 0);
}

#if (PG_VERSION_NUM >= 170000 && PG_VERSION_NUM <= 170002)
/*
 * Workaround for changed behavior in the relation rewrite code that appeared
 * in PostgreSQL 17.0, but was fixed in 17.3.
 *
 * Merge chunks uses the relation rewrite functionality from CLUSTER and
 * VACUUM FULL. This works for merge because, when writing into a non-empty
 * relation, new pages are appended while the existing pages remain the
 * same. In PG17.0, however, that changed so that existing pages in the
 * relation were zeroed out. The changed behavior was introduced as part of
 * this commit:
 *
 * https://github.com/postgres/postgres/commit/8af256524893987a3e534c6578dd60edfb782a77
 *
 * Fortunately, this was fixed in a follow up commit:
 *
 * https://github.com/postgres/postgres/commit/9695835538c2c8e9cd0048028b8c85e1bbf5c79c
 *
 * The fix is part of PG 17.3. Howevever, this still leaves PG 17.0 - 17.2
 * with different behavior.
 *
 * To make the merge chunks code work for the "broken" versions we make PG
 * believe the first rewrite operation is the size of the fully merged
 * relation so that we reserve the full space needed and then "append"
 * backwards into the zeroed space (see illustration below). By doing this, we
 * ensure that no valid data is zeroed out. The downside of this approach is
 * that there will be a lot of unnecessary writing of zero pages. Below is an
 * example of what the rewrite would look like for merging three relations
 * with one page each. When writing the first relation, PG believes the merged
 * relation already contains two pages when starting the rewrite. These two
 * existing pages will be zeroed. When writing the next relation we tell PG
 * that there is only one existing page in the merged relation, and so forth.
 *
 *  _____________
 *  |_0_|_0_|_x_|
 *  _________
 *  |_0_|_x_|
 *  _____
 *  |_x_|
 *
 *  Result:
 *  _____________
 *  |_x_|_x_|_x_|
 *
 */
static BlockNumber merge_rel_nblocks = 0;
static BlockNumber *blockoff = NULL;
static const TableAmRoutine *old_routine = NULL;
static TableAmRoutine routine = {};

/*
 * TAM relation size function to make PG believe that the merged relation
 * contains as specific amount of existing data.
 */
static uint64
pq17_workaround_merge_relation_size(Relation rel, ForkNumber forkNumber)
{
	uint64 nblocks = merge_rel_nblocks;

	if (forkNumber == MAIN_FORKNUM)
		return nblocks * BLCKSZ;

	return old_routine->relation_size(rel, forkNumber);
}

static inline void
pg17_workaround_init(Relation rel, RelationMergeInfo *relinfos, int nrelids)
{
	routine = *rel->rd_tableam;
	routine.relation_size = pq17_workaround_merge_relation_size;
	old_routine = rel->rd_tableam;
	rel->rd_tableam = &routine;
	blockoff = palloc(sizeof(BlockNumber) * nrelids);
	uint64 totalblocks = 0;

	for (int i = 0; i < nrelids; i++)
	{
		blockoff[i] = (BlockNumber) totalblocks;

		if (relinfos[i].rel)
		{
			totalblocks += smgrnblocks(RelationGetSmgr(relinfos[i].rel), MAIN_FORKNUM);

			/* Ensure the offsets don't overflow. For the merge itself, it is
			 * assumed that the write will fail when writing too many blocks */
			Ensure(totalblocks <= MaxBlockNumber, "max number of blocks exceeded for merge");
		}
	}
}

static inline void
pg17_workaround_cleanup(Relation rel)
{
	pfree(blockoff);
	rel->rd_tableam = old_routine;
}

static inline RelationMergeInfo *
get_relmergeinfo(RelationMergeInfo *relinfos, int nrelids, int i)
{
	RelationMergeInfo *relinfo = &relinfos[nrelids - i - 1];
	merge_rel_nblocks = blockoff[nrelids - i - 1];
	return relinfo;
}

#else
#define pg17_workaround_init(rel, relinfos, nrelids)
#define pg17_workaround_cleanup(rel)
#define get_relmergeinfo(relinfos, nrelids, i) &(relinfos)[i]
#endif

/* Update table stats */
void
update_relstats(Relation catrel, Relation rel, double ntuples)
{
	HeapTuple reltup = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(RelationGetRelid(rel)));
	if (!HeapTupleIsValid(reltup))
		elog(ERROR, "cache lookup failed for relation %u", RelationGetRelid(rel));
	Form_pg_class relform = (Form_pg_class) GETSTRUCT(reltup);
	BlockNumber num_pages = RelationGetNumberOfBlocks(rel);
	relform->relpages = num_pages;
	relform->reltuples = ntuples;

	CatalogTupleUpdate(catrel, &reltup->t_self, reltup);
	heap_freetuple(reltup);
}

static double
copy_table_data(Relation fromrel, Relation torel, struct VacuumCutoffs *cutoffs,
				struct VacuumCutoffs *merged_cutoffs)
{
	double num_tuples = 0.0;
	double tups_vacuumed = 0.0;
	double tups_recently_dead = 0.0;

	table_relation_copy_for_cluster(fromrel,
									torel,
									NULL,
									false,
									cutoffs->OldestXmin,
									&cutoffs->FreezeLimit,
									&cutoffs->MultiXactCutoff,
									&num_tuples,
									&tups_vacuumed,
									&tups_recently_dead);

	elog(LOG,
		 "merged rows from \"%s\" into \"%s\": tuples %lf vacuumed %lf recently dead %lf",
		 RelationGetRelationName(fromrel),
		 RelationGetRelationName(torel),
		 num_tuples,
		 tups_vacuumed,
		 tups_recently_dead);

	if (TransactionIdPrecedes(merged_cutoffs->FreezeLimit, cutoffs->FreezeLimit))
		merged_cutoffs->FreezeLimit = cutoffs->FreezeLimit;

	if (MultiXactIdPrecedes(merged_cutoffs->MultiXactCutoff, cutoffs->MultiXactCutoff))
		merged_cutoffs->MultiXactCutoff = cutoffs->MultiXactCutoff;

	/* Close the relations before the heap swap, but keep the locks until
	 * end of transaction. */
	table_close(fromrel, NoLock);

	return num_tuples;
}

static Oid
merge_relinfos(RelationMergeInfo *relinfos, int nrelids, int mergeindex)
{
	RelationMergeInfo *result_minfo = &relinfos[mergeindex];
	Relation result_rel = result_minfo->rel;

	if (result_rel == NULL)
		return InvalidOid;

	Oid tablespace = result_rel->rd_rel->reltablespace;
	struct VacuumCutoffs *merged_cutoffs = &result_minfo->cutoffs;

	/* Create the transient heap that will receive the re-ordered data */
	Oid new_relid = make_new_heap(RelationGetRelid(result_rel),
								  tablespace,
								  result_rel->rd_rel->relam,
								  result_minfo->relpersistence,
								  ExclusiveLock);
	Relation new_rel = table_open(new_relid, AccessExclusiveLock);
	double total_num_tuples = 0.0;
	FormData_compression_chunk_size merged_ccs;

	memset(&merged_ccs, 0, sizeof(FormData_compression_chunk_size));

	pg17_workaround_init(new_rel, relinfos, nrelids);

	/* Step 3: write the data from all the rels into a new merged heap */
	for (int i = 0; i < nrelids; i++)
	{
		RelationMergeInfo *relinfo = get_relmergeinfo(relinfos, nrelids, i);
		struct VacuumCutoffs *cutoffs_i = &relinfo->cutoffs;
		double num_tuples = 0.0;

		if (relinfo->rel)
		{
			num_tuples = copy_table_data(relinfo->rel, new_rel, cutoffs_i, merged_cutoffs);
			total_num_tuples += num_tuples;
			relinfo->rel = NULL;
		}

		/*
		 * Merge compression chunk size stats.
		 *
		 * Simply sum up the stats for all compressed relations that are
		 * merged. Note that we don't add anything for non-compressed
		 * relations that are merged because they don't have stats. This is a
		 * bit weird because the data from uncompressed relations will not be
		 * reflected in the stats of the merged chunk although the data is
		 * part of the chunk.
		 */
		merged_ccs.compressed_heap_size += relinfo->ccs.compressed_heap_size;
		merged_ccs.compressed_toast_size += relinfo->ccs.compressed_toast_size;
		merged_ccs.compressed_index_size += relinfo->ccs.compressed_index_size;
		merged_ccs.uncompressed_heap_size += relinfo->ccs.uncompressed_heap_size;
		merged_ccs.uncompressed_toast_size += relinfo->ccs.uncompressed_toast_size;
		merged_ccs.uncompressed_index_size += relinfo->ccs.uncompressed_index_size;
		merged_ccs.numrows_post_compression += relinfo->ccs.numrows_post_compression;
		merged_ccs.numrows_pre_compression += relinfo->ccs.numrows_pre_compression;
		merged_ccs.numrows_frozen_immediately += relinfo->ccs.numrows_frozen_immediately;
	}

	pg17_workaround_cleanup(new_rel);

	/* Update table stats */
	Relation relRelation = table_open(RelationRelationId, RowExclusiveLock);
	update_relstats(relRelation, new_rel, total_num_tuples);
	table_close(new_rel, NoLock);
	table_close(relRelation, RowExclusiveLock);

	/*
	 * Update compression chunk size stats, but only if at least one of the
	 * merged chunks was compressed. In that case the merged metadata should
	 * be non-zero.
	 */
	if (merged_ccs.compressed_heap_size > 0)
	{
		/*
		 * The result relation should always be compressed because we pick the
		 * first compressed one, if one exists.
		 */

		Assert(result_minfo->ccs.compressed_heap_size > 0);
		ts_compression_chunk_size_update(result_minfo->chunk->fd.id, &merged_ccs);
	}

	return new_relid;
}

/*
 * Merge N chunk relations into one chunk based on Oids.
 *
 * The input chunk relations are ordered according to partition ranges and the
 * "first" relation in that ordered list will be "kept" to hold the merged
 * data. The merged chunk will have its partition ranges updated to cover the
 * ranges of all the merged chunks.
 *
 * The merge happens via a heap rewrite, followed by a heap swap, essentially
 * the same approach implemented by CLUSTER and VACUUM FULL, but applied on
 * several relations in the same operation (many to one).
 *
 *
 * The heap swap approach handles visibility across all PG isolation levels,
 * as implemented by the cluster code.
 *
 * In the first step, all data from each chunk is written to a temporary heap
 * (accounting for vacuum, half-dead/visible, and frozen tuples). In the
 * second step, a heap swap is performed on one of the chunks and all metadata
 * is rewritten to handle, e.g., new partition ranges. Finally, the old chunks
 * are dropped, except for the chunk that received the heap swap.
 *
 * To be able to merge, the function checks that:
 *
 * - all relations are tables (not, e.g,, views)
 * - all relations use same (or compatible) storage on disk
 * - all relations are chunks (and not, e.g., foreign/OSM chunks)
 *
 * Compressed chunks can be merged, and in that case the non-compressed chunk
 * and the (internal) compressed chunk are merged in separate
 * steps. Currently, the merge does not move and recompress data across the
 * two relations so whatever data was compressed or not compressed prior to
 * the merge will remain in the same state after the merge.
 */
Datum
chunk_merge_chunks(PG_FUNCTION_ARGS)
{
	ArrayType *chunks_array = PG_ARGISNULL(0) ? NULL : PG_GETARG_ARRAYTYPE_P(0);
	Datum *relids;
	bool *nulls;
	int nrelids;
	RelationMergeInfo *relinfos;
	RelationMergeInfo *crelinfos; /* For compressed relations */
	int32 hypertable_id = INVALID_HYPERTABLE_ID;
	Hypercube *merged_cube = NULL;
	const Hypercube *prev_cube = NULL;
	const MergeLockUpgrade lock_upgrade = merge_chunks_lock_upgrade_mode();
	int mergeindex = -1;

	PreventCommandIfReadOnly("merge_chunks");

	if (chunks_array == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("no chunks to merge specified")));

	deconstruct_array(chunks_array,
					  REGCLASSOID,
					  sizeof(Oid),
					  true,
					  TYPALIGN_INT,
					  &relids,
					  &nulls,
					  &nrelids);

	if (nrelids < 2)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("must specify at least two chunks to merge")));

	relinfos = palloc0(sizeof(struct RelationMergeInfo) * nrelids);
	crelinfos = palloc0(sizeof(struct RelationMergeInfo) * nrelids);

	/* Sort relids array in order to find duplicates and lock relations in
	 * consistent order to avoid deadlocks. It doesn't matter that we don't
	 * order the nulls array the same since we only care about all relids
	 * being non-null. */
	qsort(relids, nrelids, sizeof(Datum), oid_cmp);

	/* Step 1: Do sanity checks and then prepare to sort rels in consistent order. */
	for (int i = 0; i < nrelids; i++)
	{
		Oid relid = DatumGetObjectId(relids[i]);
		RelationMergeInfo *relinfo = &relinfos[i];
		Chunk *chunk;
		Relation rel;

		if (nulls[i] || !OidIsValid(relid))
			ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("invalid relation")));

		if (i > 0 && DatumGetObjectId(relids[i]) == DatumGetObjectId(relids[i - 1]))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("duplicate relation \"%s\" in merge",
							get_rel_name(DatumGetObjectId(relids[i])))));

		/* Lock the relation before doing other checks that can lock dependent
		 * objects (this can otherwise lead to deadlocks with concurrent
		 * operations). Note that if we take ExclusiveLock here to allow
		 * readers while we are rewriting/merging the relations, the lock
		 * needs to be upgraded to an AccessExclusiveLock later. This can also
		 * lead to deadlocks.
		 *
		 * Ideally, we should probably take locks on all dependent objects as
		 * well, at least on chunk-related objects that will be
		 * dropped. Otherwise, that might also cause deadlocks later. For
		 * example, if doing a concurrent DROP TABLE on one of the chunks will
		 * lead to deadlock because it grabs locks on all dependencies before
		 * dropping.
		 *
		 * However, for now we won't do that because that requires scanning
		 * pg_depends and concurrent operations will probably fail anyway if
		 * we remove the objects. We might as well fail with a deadlock.
		 */
		LOCKMODE lockmode =
			(lock_upgrade == MERGE_LOCK_ACCESS_EXCLUSIVE) ? AccessExclusiveLock : ExclusiveLock;

		rel = try_table_open(relid, lockmode);

		/* Check if the table actually exists. If not, it could have been
		 * deleted in a concurrent merge. */
		if (rel == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_TABLE),
					 errmsg("relation does not exist"),
					 errdetail("The relation with OID %u might have been removed "
							   "by a concurrent merge or other operation.",
							   relid)));

		if (rel->rd_rel->relkind != RELKIND_RELATION)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("cannot merge non-table relations")));

		/* Only owner is allowed to merge */
		if (!object_ownercheck(RelationRelationId, relid, GetUserId()))
			aclcheck_error(ACLCHECK_NOT_OWNER,
						   get_relkind_objtype(rel->rd_rel->relkind),
						   get_rel_name(relid));

		/* Lock toast table to prevent it from being concurrently vacuumed */
		if (rel->rd_rel->reltoastrelid)
			LockRelationOid(rel->rd_rel->reltoastrelid, lockmode);

		/*
		 * Check for active uses of the relation in the current transaction,
		 * including open scans and pending AFTER trigger events.
		 */
		CheckTableNotInUse(rel, "merge_chunks");

		if (IsSystemRelation(rel))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("cannot merge system catalog relations")));

		/*
		 * Find the chunk corresponding to the relation for final checks. Done
		 * after locking the chunk relation because scanning for the chunk
		 * will grab locks on other objects, which might otherwise lead to
		 * deadlocks during concurrent merges instead of more helpful messages
		 * (like chunk does not exist because it was merged).
		 */
		chunk = ts_chunk_get_by_relid(relid, false);

		if (NULL == chunk)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("can only merge hypertable chunks")));

		if (!merge_chunks_multidim_allowed())
		{
			Cache *hcache;
			Hypertable *ht = ts_hypertable_cache_get_cache_and_entry(chunk->hypertable_relid,
																	 CACHE_FLAG_NONE,
																	 &hcache);
			Ensure(ht, "missing hypertable for chunk");

			if (ht->fd.num_dimensions > 1)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("cannot merge chunk in multi-dimensional hypertable")));

			ts_cache_release(&hcache);
		}

		if (chunk->fd.osm_chunk)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("cannot merge OSM chunks")));

		/*
		 * Lock also internal compressed relation, if it exists.
		 *
		 * Don't fill in its MergeRelInfo until we sort relations in partition
		 * order below, because the compressed relations need to be in the
		 * same order.
		 */
		if (chunk->fd.compressed_chunk_id != INVALID_CHUNK_ID)
		{
			Oid crelid = ts_chunk_get_relid(chunk->fd.compressed_chunk_id, false);
			LockRelationOid(crelid, AccessExclusiveLock);

			if (mergeindex == -1)
				mergeindex = i;

			/* Read compression chunk size stats */
			bool found = ts_compression_chunk_size_get(chunk->fd.id, &relinfo->ccs);

			if (!found)
				elog(WARNING,
					 "missing compression chunk size stats for compressed chunk \"%s\"",
					 NameStr(chunk->fd.table_name));
		}

		if (ts_chunk_is_frozen(chunk))
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("cannot merge frozen chunk \"%s.%s\" scheduled for tiering",
							NameStr(chunk->fd.schema_name),
							NameStr(chunk->fd.table_name)),
					 errhint("Untier the chunk before merging.")));

		if (hypertable_id == INVALID_HYPERTABLE_ID)
			hypertable_id = chunk->fd.hypertable_id;
		else if (hypertable_id != chunk->fd.hypertable_id)
		{
			Assert(i > 0);

			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("cannot merge chunks across different hypertables"),
					 errdetail("Chunk \"%s\" is part of hypertable \"%s\" while chunk \"%s\" is "
							   "part of hypertable \"%s\"",
							   get_rel_name(chunk->table_id),
							   get_rel_name(chunk->hypertable_relid),
							   get_rel_name(relinfos[i - 1].chunk->table_id),
							   get_rel_name(relinfos[i - 1].chunk->hypertable_relid))));
		}

		/*
		 * It might not be possible to merge two chunks with different
		 * storage, so better safe than sorry for now.
		 */
		Oid amoid = rel->rd_rel->relam;

		if (amoid != HEAP_TABLE_AM_OID)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("access method \"%s\" is not supported for merge",
							get_am_name(amoid))));

		relinfo->relid = relid;
		relinfo->rel = rel;
		relinfo->chunk = chunk;
		relinfo->relpersistence = rel->rd_rel->relpersistence;
	}

	/* No compressed chunk found, so use index 0 for resulting merged chunk */
	if (mergeindex == -1)
		mergeindex = 0;

	relinfos[mergeindex].isresult = true;

	/* Sort rels in partition order (in case of chunks). This is necessary to
	 * validate that a merge is possible. */
	qsort(relinfos, nrelids, sizeof(RelationMergeInfo), cmp_relations);

	/* Step 2: Check alignment/mergeability and create the merged hypercube
	 * (partition ranges). */
	for (int i = 0; i < nrelids; i++)
	{
		const Chunk *chunk = relinfos[i].chunk;

		Assert(chunk != NULL);

		if (merged_cube == NULL)
		{
			merged_cube = ts_hypercube_copy(chunk->cube);
			Assert(prev_cube == NULL);
		}
		else
		{
			Assert(chunk->cube->num_slices == merged_cube->num_slices);
			Assert(prev_cube != NULL);
			validate_merge_possible(prev_cube, chunk->cube);
			merge_cubes(merged_cube, chunk->cube);
		}

		prev_cube = chunk->cube;
		compute_rel_vacuum_cutoffs(relinfos[i].rel, &relinfos[i].cutoffs);

		/*
		 * Fill in the compressed mergerelinfo array here after final sort of
		 * rels so that the two arrays have the same order.
		 */
		if (chunk->fd.compressed_chunk_id != INVALID_CHUNK_ID)
		{
			RelationMergeInfo *crelinfo = &crelinfos[i];

			crelinfo->chunk = ts_chunk_get_by_id(chunk->fd.compressed_chunk_id, true);
			crelinfo->relid = crelinfo->chunk->table_id;
			crelinfo->rel = table_open(crelinfo->relid, AccessExclusiveLock);
			crelinfo->isresult = relinfos[i].isresult;
			crelinfo->iscompressed_rel = true;
			crelinfo->relpersistence = crelinfo->rel->rd_rel->relpersistence;
			compute_rel_vacuum_cutoffs(crelinfos[i].rel, &crelinfos[i].cutoffs);
		}

		/* Need to update the index of the result (merged) relation after
		 * resort */
		if (relinfos[i].isresult)
			mergeindex = i;
	}

	/*
	 * Now merge all the data into a new temporary heap relation. Do it
	 * separately for the non-compressed and compressed relations.
	 */
	Oid new_relid = merge_relinfos(relinfos, nrelids, mergeindex);
	Oid new_crelid = merge_relinfos(crelinfos, nrelids, mergeindex);

	/* Make new table stats visible */
	CommandCounterIncrement();

	DEBUG_WAITPOINT("merge_chunks_before_heap_swap");

	merge_chunks_finish(new_relid, relinfos, nrelids, lock_upgrade);

	if (OidIsValid(new_crelid))
		merge_chunks_finish(new_crelid, crelinfos, nrelids, lock_upgrade);

	/* Step 5: Update the dimensional metadata and constraints for the chunk
	 * we are keeping. */
	if (merged_cube)
	{
		RelationMergeInfo *result_minfo = &relinfos[mergeindex];
		Assert(result_minfo->chunk);
		chunk_update_constraints(result_minfo->chunk, merged_cube);
		ts_hypercube_free(merged_cube);
	}

	pfree(relids);
	pfree(nulls);
	pfree(relinfos);
	pfree(crelinfos);

	PG_RETURN_VOID();
}
