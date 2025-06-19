/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <access/heapam.h>
#include <access/htup.h>
#include <access/htup_details.h>
#include <access/multixact.h>
#include <access/relscan.h>
#include <access/rewriteheap.h>
#include <access/sdir.h>
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
#include <storage/buf.h>
#include <storage/bufmgr.h>
#include <storage/itemptr.h>
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

#include <math.h>

#include "compat/compat.h"
#include "annotations.h"
#include "cache.h"
#include "chunk.h"
#include "compression/api.h"
#include "compression/compression.h"
#include "compression/create.h"
#include "debug_point.h"
#include "extension.h"
#include "extension_constants.h"
#include "hypercore/arrow_tts.h"
#include "hypercore/hypercore_handler.h"
#include "hypercube.h"
#include "hypertable.h"
#include "hypertable_cache.h"
#include "trigger.h"
#include "ts_catalog/array_utils.h"
#include "ts_catalog/catalog.h"
#include "ts_catalog/compression_chunk_size.h"
#include "ts_catalog/compression_settings.h"
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
	AttrNumber attnum; /* Attnum of dimension/column we split along */
	Oid type;		   /* Type of split dimension */
	int64 point;	   /* Point at which we split */
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

/*
 * Compute the partition/routing index for a tuple.
 *
 * Returns 0 or 1 for first or second partition, respectively.
 */
static int
route_tuple(TupleTableSlot *slot, const SplitPoint *sp)
{
	bool isnull = false;
	Datum value = slot_getattr(slot, sp->attnum, &isnull);
	int64 point;

	Ensure(!isnull, "unexpected NULL value in partitioning column");

	point = ts_time_value_to_internal(value, sp->type);

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
	bool isnull = false;
	Datum min_value = slot_getattr(slot, csp->attnum_min, &isnull);
	Assert(!isnull);

	Datum max_value = slot_getattr(slot, csp->attnum_max, &isnull);
	Assert(!isnull);

	int64 min_point = ts_time_value_to_internal(min_value, sp->type);
	int64 max_point = ts_time_value_to_internal(max_value, sp->type);

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
	 * If a Hypercore TAM relation, we only want to read uncompressed data
	 * since the compressed relation is split separately.
	 */
	hypercore_scan_set_skip_compressed(scan, true);

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

		if (TTS_IS_ARROWTUPLE(srcslot))
		{
			ArrowTupleTableSlot *aslot = (ArrowTupleTableSlot *) srcslot;
			hslot = (BufferHeapTupleTableSlot *) aslot->child_slot;
		}
		else
		{
			hslot = (BufferHeapTupleTableSlot *) srcslot;
		}

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
								   int split_factor, Oid amoid)
{
	double total_tuples = 0;
	bool is_hypercore_tam = ts_is_hypercore_am(amoid);

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

		if (sri->heap_swap || is_hypercore_tam)
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
							 const SplitRelationInfo *compressed_split_relations, int split_factor,
							 Oid amoid)
{
	if (compressed_split_relations)
		update_compression_stats_for_split(split_relations,
										   compressed_split_relations,
										   split_factor,
										   amoid);
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

		if (ts_is_hypercore_am(amoid) && compressed_split_relations)
		{
			/* Hypercore TAM has the total reltuples stats on the user-visible
			 * relation */
			const SplitRelationInfo *csri = &compressed_split_relations[i];
			ntuples += csri->stats.tuples_in_segments;
		}

		rel = table_open(sri->relid, AccessShareLock);
		update_relstats(relRelation, rel, ntuples);
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
	Relation srcrel;

	srcrel = table_open(relid, AccessExclusiveLock);

	if (srcrel->rd_rel->relkind != RELKIND_RELATION)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot split non-table relations")));

	Oid amoid = srcrel->rd_rel->relam;

	if (amoid != HEAP_TABLE_AM_OID && !ts_is_hypercore_am(amoid))
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

	AttrNumber splitdim_attnum = get_attnum(relid, NameStr(dim->fd.column_name));
	Oid splitdim_type = get_atttype(relid, splitdim_attnum);
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
	int64 split_at = 0;

	if (have_split_at)
	{
		split_at = ts_time_value_to_internal(split_at_datum, splitdim_type);

		/*
		 * Check that the split_at value actually produces a valid split. Note
		 * that range_start is inclusive while range_end is non-inclusive. The
		 * split_at value needs to produce partition ranges of at least length
		 * 1.
		 */
		if (split_at < (slice->fd.range_start + 1) || split_at > (slice->fd.range_end - 2))
		{
			Oid outfuncid = InvalidOid;
			bool isvarlena = false;

			getTypeOutputInfo(splitdim_type, &outfuncid, &isvarlena);
			Datum split_at_str = OidFunctionCall1(outfuncid, split_at_datum);

			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("cannot split chunk at %s", DatumGetCString(split_at_str))));
		}
	}
	else
		split_at = slice->fd.range_start + (interval_range / 2);

	Oid outfuncid = InvalidOid;
	bool isvarlena = false;

	getTypeOutputInfo(splitdim_type, &outfuncid, &isvarlena);
	split_at_datum = ts_internal_to_time_value(split_at, splitdim_type);
	Datum split_at_str = OidFunctionCall1(outfuncid, split_at_datum);
	elog(DEBUG1, "splitting chunk %s at %s", get_rel_name(relid), DatumGetCString(split_at_str));

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
															amoid,
															&created);
	Ensure(created, "could not create chunk for split");
	Assert(new_chunk);

	Chunk *new_compressed_chunk = NULL;

	if (compress_settings != NULL)
	{
		if (ts_is_hypercore_am(amoid))
		{
			Relation new_chunk_rel = table_open(new_chunk->table_id, AccessExclusiveLock);
			HypercoreInfo *hinfo = RelationGetHypercoreInfo(new_chunk_rel);
			/*
			 * Hypercore TAM automatically creates the internal compressed
			 * chunk when building the HypercoreInfo, so retrieve it to ensure
			 * the compressed chunk was created.
			 */
			new_compressed_chunk = ts_chunk_get_by_relid(hinfo->compressed_relid, true);
			table_close(new_chunk_rel, NoLock);
		}
		else
		{
			Hypertable *ht_compressed = ts_hypertable_get_by_id(ht->fd.compressed_hypertable_id);
			new_compressed_chunk = create_compress_chunk(ht_compressed, new_chunk, InvalidOid);
			ts_trigger_create_all_on_chunk(new_compressed_chunk);
			ts_chunk_set_compressed_chunk(new_chunk, new_compressed_chunk->fd.id);
		}
	}

	ts_cache_release(&hcache);

	CommandCounterIncrement();

	DEBUG_WAITPOINT("split_chunk_before_tuple_routing");

	SplitPoint sp = {
		.point = split_at,
		.attnum = splitdim_attnum,
		.type = splitdim_type,
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
				.attnum = splitdim_attnum,
				.type = splitdim_type,
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

	/* Update stats after split is done */
	update_chunk_stats_for_split(split_relations, compressed_split_relations, SPLIT_FACTOR, amoid);

	DEBUG_WAITPOINT("split_chunk_at_end");

	PG_RETURN_VOID();
}
