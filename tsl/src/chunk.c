/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <access/htup.h>
#include <access/htup_details.h>
#include <access/multixact.h>
#include <access/rewriteheap.h>
#include <access/table.h>
#include <access/tableam.h>
#include <access/transam.h>
#include <access/tupconvert.h>
#include <access/xact.h>
#include <catalog/catalog.h>
#include <catalog/dependency.h>
#include <catalog/heap.h>
#include <catalog/namespace.h>
#include <catalog/objectaddress.h>
#include <catalog/pg_am.h>
#include <catalog/pg_class.h>
#include <catalog/pg_constraint.h>
#include <catalog/pg_foreign_server.h>
#include <catalog/pg_foreign_table.h>
#include <catalog/pg_type.h>
#include <commands/defrem.h>
#include <commands/tablecmds.h>
#include <commands/vacuum.h>
#include <common/relpath.h>
#include <executor/executor.h>
#include <executor/tuptable.h>
#include <fmgr.h>
#include <foreign/foreign.h>
#include <funcapi.h>
#include <miscadmin.h>
#include <nodes/lockoptions.h>
#include <nodes/makefuncs.h>
#include <nodes/nodes.h>
#include <nodes/parsenodes.h>
#include <parser/parse_func.h>
#include <storage/block.h>
#include <storage/bufmgr.h>
#include <storage/lmgr.h>
#include <storage/lockdefs.h>
#include <storage/smgr.h>
#include <utils/acl.h>
#include <utils/builtins.h>
#include <utils/elog.h>
#include <utils/guc.h>
#include <utils/inval.h>
#include <utils/lsyscache.h>
#include <utils/memutils.h>
#include <utils/palloc.h>
#include <utils/rel.h>
#include <utils/snapmgr.h>
#include <utils/syscache.h>
#include <utils/tuplestore.h>

#include "compat/compat.h"
#include "annotations.h"
#include "cache.h"
#include "chunk.h"
#include "debug_point.h"
#include "extension.h"
#include "hypercube.h"
#include "hypertable.h"
#include "hypertable_cache.h"
#include "ts_catalog/catalog.h"
#include "ts_catalog/compression_chunk_size.h"
#include "utils.h"

/* Data in a frozen chunk cannot be modified. So any operation
 * that rewrites data for a frozen chunk will be blocked.
 * Note that a frozen chunk can still be dropped.
 */
Datum
chunk_freeze_chunk(PG_FUNCTION_ARGS)
{
	Oid chunk_relid = PG_ARGISNULL(0) ? InvalidOid : PG_GETARG_OID(0);
	TS_PREVENT_FUNC_IF_READ_ONLY();
	Chunk *chunk = ts_chunk_get_by_relid(chunk_relid, true);
	Assert(chunk != NULL);
	if (chunk->relkind == RELKIND_FOREIGN_TABLE)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("operation not supported on tiered chunk \"%s\"",
						get_rel_name(chunk_relid))));
	}
	if (ts_chunk_is_frozen(chunk))
		PG_RETURN_BOOL(true);
	/* get Share lock. will wait for other concurrent transactions that are
	 * modifying the chunk. Does not block SELECTs on the chunk.
	 * Does not block other DDL on the chunk table.
	 */
	DEBUG_WAITPOINT("freeze_chunk_before_lock");
	LockRelationOid(chunk_relid, ShareLock);
	bool ret = ts_chunk_set_frozen(chunk);
	PG_RETURN_BOOL(ret);
}

Datum
chunk_unfreeze_chunk(PG_FUNCTION_ARGS)
{
	Oid chunk_relid = PG_ARGISNULL(0) ? InvalidOid : PG_GETARG_OID(0);
	TS_PREVENT_FUNC_IF_READ_ONLY();
	Chunk *chunk = ts_chunk_get_by_relid(chunk_relid, true);
	Assert(chunk != NULL);
	if (chunk->relkind == RELKIND_FOREIGN_TABLE)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("operation not supported on foreign table \"%s\"",
						get_rel_name(chunk_relid))));
	}
	if (!ts_chunk_is_frozen(chunk))
		PG_RETURN_BOOL(true);
	/* This is a previously frozen chunk. Only selects are permitted on this chunk.
	 * This changes the status in the catalog to allow previously blocked operations.
	 */
	bool ret = ts_chunk_unset_frozen(chunk);
	PG_RETURN_BOOL(ret);
}

/*
 * Invoke drop_chunks via fmgr so that the call can be deparsed and sent to
 * remote data nodes.
 *
 * Given that drop_chunks is an SRF, and has pseudo parameter types, we need
 * to provide a FuncExpr with type information for the deparser.
 *
 * Returns the number of dropped chunks.
 */
int
chunk_invoke_drop_chunks(Oid relid, Datum older_than, Datum older_than_type, bool use_creation_time)
{
	EState *estate;
	ExprContext *econtext;
	FuncExpr *fexpr;
	List *args = NIL;
	int num_results = 0;
	SetExprState *state;
	Oid restype;
	Oid func_oid;
	Const *TypeNullCons = makeNullConst(older_than_type, -1, InvalidOid);
	Const *IntervalVal = makeConst(older_than_type,
								   -1,
								   InvalidOid,
								   get_typlen(older_than_type),
								   older_than,
								   false,
								   get_typbyval(older_than_type));
	Const *argarr[DROP_CHUNKS_NARGS] = { makeConst(REGCLASSOID,
												   -1,
												   InvalidOid,
												   sizeof(relid),
												   ObjectIdGetDatum(relid),
												   false,
												   false),
										 TypeNullCons,
										 TypeNullCons,
										 castNode(Const, makeBoolConst(false, true)),
										 TypeNullCons,
										 TypeNullCons };
	Oid type_id[DROP_CHUNKS_NARGS] = { REGCLASSOID, ANYOID, ANYOID, BOOLOID, ANYOID, ANYOID };
	char *const schema_name = ts_extension_schema_name();
	List *const fqn = list_make2(makeString(schema_name), makeString(DROP_CHUNKS_FUNCNAME));

	StaticAssertStmt(lengthof(type_id) == lengthof(argarr),
					 "argarr and type_id should have matching lengths");

	func_oid = LookupFuncName(fqn, lengthof(type_id), type_id, false);
	Assert(func_oid); /* LookupFuncName should not return an invalid OID */

	/* decide whether to use "older_than" or "drop_created_before" */
	if (use_creation_time)
		argarr[4] = IntervalVal;
	else
		argarr[1] = IntervalVal;

	/* Prepare the function expr with argument list */
	get_func_result_type(func_oid, &restype, NULL);

	for (size_t i = 0; i < lengthof(argarr); i++)
		args = lappend(args, argarr[i]);

	fexpr = makeFuncExpr(func_oid, restype, args, InvalidOid, InvalidOid, COERCE_EXPLICIT_CALL);
	fexpr->funcretset = true;

	/* Execute the SRF */
	estate = CreateExecutorState();
	econtext = CreateExprContext(estate);
	state = ExecInitFunctionResultSet(&fexpr->xpr, econtext, NULL);

	while (true)
	{
		ExprDoneCond isdone;
		bool isnull;

		ExecMakeFunctionResultSet(state, econtext, estate->es_query_cxt, &isnull, &isdone);

		if (isdone == ExprEndResult)
			break;

		if (!isnull)
			num_results++;
	}

	/* Cleanup */
	FreeExprContext(econtext, false);
	FreeExecutorState(estate);

	return num_results;
}

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

static void
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

static void
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

static void
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
	const TableAmRoutine *tableam = NULL;
	double num_tuples = 0.0;
	double tups_vacuumed = 0.0;
	double tups_recently_dead = 0.0;

	if (ts_is_hypercore_am(fromrel->rd_rel->relam))
	{
		tableam = fromrel->rd_tableam;
		fromrel->rd_tableam = GetHeapamTableAmRoutine();
	}

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

	if (tableam != NULL)
		fromrel->rd_tableam = tableam;

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
 * A current limitation is that it is not possible to merge compressed chunks
 * since this requires additional functionality, such as:
 *
 * - Handling merge of compressed and non-compressed chunks
 *
 * - Merging chunks with different compression settings (e.g., different
 *   orderby or segmentby)
 *
 * - Merging partial chunks
 *
 * - Updating additional metadata of the internal compressed relations
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

		if (amoid != HEAP_TABLE_AM_OID && !ts_is_hypercore_am(amoid))
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

typedef struct RelationSplitInfo
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
} RelationSplitInfo;

static RelationSplitInfo *
relation_split_info_create(Relation srcrel, Relation targetrel, struct VacuumCutoffs *cutoffs)
{
	RelationSplitInfo *rsi = palloc0(sizeof(RelationSplitInfo));

	rsi->targetrel = targetrel;
	rsi->bistate = GetBulkInsertState();
	rsi->rwstate = begin_heap_rewrite(srcrel,
									  targetrel,
									  cutoffs->OldestXmin,
									  cutoffs->FreezeLimit,
									  cutoffs->MultiXactCutoff);

	rsi->tupmap = convert_tuples_by_name(RelationGetDescr(srcrel), RelationGetDescr(targetrel));

	/* Create tuple slot for new partition. */
	rsi->dstslot =
		MakeSingleTupleTableSlot(RelationGetDescr(targetrel), table_slot_callbacks(targetrel));
	ExecStoreAllNullTuple(rsi->dstslot);

	rsi->values = (Datum *) palloc0(RelationGetDescr(srcrel)->natts * sizeof(Datum));
	rsi->isnull = (bool *) palloc0(RelationGetDescr(srcrel)->natts * sizeof(bool));

	return rsi;
}

static void
relation_split_info_free(RelationSplitInfo *rsi, int ti_options)
{
	ExecDropSingleTupleTableSlot(rsi->dstslot);
	FreeBulkInsertState(rsi->bistate);
	table_finish_bulk_insert(rsi->targetrel, ti_options);
	end_heap_rewrite(rsi->rwstate);
	table_close(rsi->targetrel, NoLock);
	pfree(rsi->values);
	pfree(rsi->isnull);

	if (rsi->tupmap)
		free_conversion_map(rsi->tupmap);

	rsi->targetrel = NULL;
	rsi->bistate = NULL;
	rsi->dstslot = NULL;
	rsi->tupmap = NULL;
	rsi->values = NULL;
	rsi->isnull = NULL;
	pfree(rsi);
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
reform_and_rewrite_tuple(HeapTuple tuple, Relation srcrel, const RelationSplitInfo *rsi)
{
	TupleDesc oldTupDesc = RelationGetDescr(srcrel);
	TupleDesc newTupDesc = RelationGetDescr(rsi->targetrel);
	HeapTuple tupcopy;

	if (rsi->tupmap)
	{
		/*
		 * If this is the "new" relation, the tuple map might be different
		 * from the "source" relation.
		 */
		tupcopy = execute_attr_map_tuple(tuple, rsi->tupmap);
	}
	else
	{
		int i;

		heap_deform_tuple(tuple, oldTupDesc, rsi->values, rsi->isnull);

		/* Be sure to null out any dropped columns if this is the "old"
		 * relation. A relation created new doesn't have dropped columns. */
		for (i = 0; i < newTupDesc->natts; i++)
		{
			if (TupleDescAttr(newTupDesc, i)->attisdropped)
				rsi->isnull[i] = true;
		}

		tupcopy = heap_form_tuple(newTupDesc, rsi->values, rsi->isnull);
	}

	/* The heap rewrite module does the rest */
	rewrite_heap_tuple(rsi->rwstate, tuple, tupcopy);

	heap_freetuple(tupcopy);
}

static void
copy_tuples_for_split(Relation srcrel, RelationSplitInfo *splitinfos[2], AttrNumber splitdim_attnum,
					  Oid splitdim_type, int64 split_point, struct VacuumCutoffs *cutoffs)
{
	TupleTableSlot *srcslot;
	TableScanDesc scan;
	MemoryContext oldcxt;
	EState *estate;
	ExprContext *econtext;

	estate = CreateExecutorState();

	/* Create the tuple slot */
	srcslot = MakeSingleTupleTableSlot(RelationGetDescr(srcrel), table_slot_callbacks(srcrel));

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

	BufferHeapTupleTableSlot *hslot = (BufferHeapTupleTableSlot *) srcslot;

	while (table_scan_getnextslot(scan, ForwardScanDirection, srcslot))
	{
		const RelationSplitInfo *rsi = NULL;
		bool isnull = true;
		HeapTuple tuple;
		Buffer buf;
		bool isdead;

		CHECK_FOR_INTERRUPTS();
		ResetExprContext(econtext);

		tuple = ExecFetchSlotHeapTuple(srcslot, false, NULL);
		buf = hslot->buffer;

		LockBuffer(buf, BUFFER_LOCK_SHARE);

		switch (HeapTupleSatisfiesVacuum(tuple, cutoffs->OldestXmin, buf))
		{
			case HEAPTUPLE_DEAD:
				/* Definitely dead */
				isdead = true;
				break;
			case HEAPTUPLE_RECENTLY_DEAD:
				tups_recently_dead += 1;
				/* fall through */
				TS_FALLTHROUGH;
			case HEAPTUPLE_LIVE:
				/* Live or recently dead, must copy it */
				isdead = false;
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
				isdead = false;
				break;
			default:
				elog(ERROR, "unexpected HeapTupleSatisfiesVacuum result");
				isdead = false; /* keep compiler quiet */
				break;
		}

		LockBuffer(buf, BUFFER_LOCK_UNLOCK);

		/* Find the partition/chunk to insert the tuple into */
		Datum value = slot_getattr(srcslot, splitdim_attnum, &isnull);
		Assert(!isnull);
		int64 point = ts_time_value_to_internal(value, splitdim_type);

		/*
		 * Route to partition based on new boundaries. Only 2-way split is
		 * supported now, so routing is easy. An N-way split requires, e.g.,
		 * binary search.
		 */
		if (point < split_point)
			rsi = splitinfos[0];
		else
			rsi = splitinfos[1];

		if (isdead)
		{
			tups_vacuumed += 1;
			/* heap rewrite module still needs to see it... */
			if (rewrite_heap_dead_tuple(rsi->rwstate, tuple))
			{
				/* A previous recently-dead tuple is now known dead */
				tups_vacuumed += 1;
				tups_recently_dead -= 1;
			}
			continue;
		}

		num_tuples++;
		reform_and_rewrite_tuple(tuple, srcrel, rsi);
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
	Relation srcrel;

	srcrel = table_open(relid, AccessExclusiveLock);

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

	const Chunk *chunk = ts_chunk_get_by_relid(relid, true);

	if (chunk->fd.compressed_chunk_id != INVALID_CHUNK_ID)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("splitting a compressed chunk is not supported"),
				 errhint("Decompress the chunk before splitting it.")));

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

	AttrNumber splitdim_attnum = get_attnum(relid, NameStr(dim->fd.column_name));
	Oid splitdim_type = get_atttype(relid, splitdim_attnum);
	Datum split_at;
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
					split_at = OidFunctionCall1(infuncid, arg);
					break;
				case 3:
					/* Timestamp functions take three input arguments */
					split_at = OidFunctionCall3(infuncid,
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
			split_at = arg;

		if (argtype != splitdim_type)
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
	int64 split_point = 0;

	if (have_split_at)
	{
		split_point = ts_time_value_to_internal(split_at, splitdim_type);

		/*
		 * Check that the split_at value actually produces a valid split. Note
		 * that range_start is inclusive while range_end is non-inclusive. The
		 * split_at value needs to produce partition ranges of at least length
		 * 1.
		 */
		if (split_point < (slice->fd.range_start + 1) || split_point > (slice->fd.range_end - 2))
		{
			Oid outfuncid = InvalidOid;
			bool isvarlena = false;

			getTypeOutputInfo(splitdim_type, &outfuncid, &isvarlena);
			Datum split_at_str = OidFunctionCall1(outfuncid, split_at);

			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("cannot split chunk at %s", DatumGetCString(split_at_str))));
		}
	}
	else
		split_point = slice->fd.range_start + (interval_range / 2);

	int64 old_end = slice->fd.range_end;

	/* Update the slice range for the existing chunk */
	slice->fd.range_end = split_point;
	chunk_update_constraints(chunk, new_cube);

	/* Update the slice for the new chunk */
	slice->fd.range_start = split_point;
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
	ts_cache_release(&hcache);

	Ensure(created, "could not create chunk for split");
	Assert(new_chunk);

	int ti_options = TABLE_INSERT_SKIP_FSM;
	RelationSplitInfo *splitinfos[2];
	struct VacuumCutoffs cutoffs;
	char relpersistence = srcrel->rd_rel->relpersistence;

	compute_rel_vacuum_cutoffs(srcrel, &cutoffs);

	Oid new_heap_relid = make_new_heap(RelationGetRelid(srcrel),
									   srcrel->rd_rel->reltablespace,
									   srcrel->rd_rel->relam,
									   relpersistence,
									   AccessExclusiveLock);

	/*
	 * Open the new relations that will receive tuples during split. These are
	 * closed by relation_split_info_free().
	 */
	Relation target1_rel = table_open(new_heap_relid, AccessExclusiveLock);
	Relation target2_rel = table_open(new_chunk->table_id, AccessExclusiveLock);

	/* Setup the state we need for each new chunk partition */
	splitinfos[0] = relation_split_info_create(srcrel, target1_rel, &cutoffs);
	splitinfos[1] = relation_split_info_create(srcrel, target2_rel, &cutoffs);

	DEBUG_WAITPOINT("split_chunk_before_tuple_routing");

	copy_tuples_for_split(srcrel,
						  splitinfos,
						  splitdim_attnum,
						  splitdim_type,
						  split_point,
						  &cutoffs);
	
	table_close(srcrel, NoLock);

	/* Cleanup split rel infos and reindex new chunks */
	for (int i = 0; i < 2; i++)
	{
		ReindexParams reindex_params = { 0 };
		int reindex_flags = REINDEX_REL_SUPPRESS_INDEX_USE;
		Oid chunkrelid = splitinfos[i]->targetrel->rd_id;

		Ensure(relpersistence == RELPERSISTENCE_PERMANENT, "only permanent chunks can be split");
		reindex_flags |= REINDEX_REL_FORCE_INDEXES_PERMANENT;

		relation_split_info_free(splitinfos[i], ti_options);

		/* Only reindex new chunks. Existing chunk will be reindexed during
		 * the heap swap below. */
		if (i > 0)
			reindex_relation_compat(NULL, chunkrelid, reindex_flags, &reindex_params);
	}

	/* Finally, swap the heap of the chunk that we split so that it only
	 * contains the tuples for its new partition boundaries. AccessExclusive
	 * lock is held during the swap. */
	finish_heap_swap(relid,
					 new_heap_relid,
					 false, /* system catalog */
					 false /* swap toast by content */,
					 true, /* check constraints */
					 true, /* internal? */
					 cutoffs.FreezeLimit,
					 cutoffs.MultiXactCutoff,
					 relpersistence);

	DEBUG_WAITPOINT("split_chunk_at_end");

	PG_RETURN_VOID();
}
