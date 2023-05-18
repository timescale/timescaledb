/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <postgres.h>
#include <access/sysattr.h>
#include <catalog/pg_namespace.h>
#include <catalog/pg_operator.h>
#include <nodes/bitmapset.h>
#include <nodes/extensible.h>
#include <nodes/makefuncs.h>
#include <nodes/nodeFuncs.h>
#include <optimizer/optimizer.h>
#include <optimizer/paths.h>
#include <optimizer/plancat.h>
#include <optimizer/restrictinfo.h>
#include <optimizer/tlist.h>
#include <parser/parse_relation.h>
#include <parser/parsetree.h>
#include <utils/builtins.h>
#include <utils/typcache.h>

#include "compression/compression.h"
#include "compression/create.h"
#include "custom_type_cache.h"
#include "guc.h"
#include "import/planner.h"
#include "nodes/decompress_chunk/decompress_chunk.h"
#include "nodes/decompress_chunk/exec.h"
#include "nodes/decompress_chunk/planner.h"

static CustomScanMethods decompress_chunk_plan_methods = {
	.CustomName = "DecompressChunk",
	.CreateCustomScanState = decompress_chunk_state_create,
};

void
_decompress_chunk_init(void)
{
	TryRegisterCustomScanMethods(&decompress_chunk_plan_methods);
}

static void
check_for_system_columns(Bitmapset *attrs_used)
{
	int bit = bms_next_member(attrs_used, -1);
	if (bit > 0 && bit + FirstLowInvalidHeapAttributeNumber < 0)
	{
		/* we support tableoid so skip that */
		if (bit == TableOidAttributeNumber - FirstLowInvalidHeapAttributeNumber)
			bit = bms_next_member(attrs_used, bit);

		if (bit > 0 && bit + FirstLowInvalidHeapAttributeNumber < 0)
			elog(ERROR, "transparent decompression only supports tableoid system column");
	}
}

/*
 * Given the scan targetlist and the bitmapset of the needed columns, determine
 * which scan columns become which decompressed columns (fill decompression_map).
 *
 * Note that the chunk_attrs_needed bitmap is offset by the
 * FirstLowInvalidHeapAttributeNumber, similar to RelOptInfo.attr_needed. This
 * allows to encode the requirement for system columns, which have negative
 * attnos.
 */
static void
build_decompression_map(PlannerInfo *root, DecompressChunkPath *path, List *scan_tlist,
						Bitmapset *chunk_attrs_needed)
{
	/*
	 * Track which normal and metadata columns we were able to find in the
	 * targetlist.
	 */
	bool missing_count = true;
	bool missing_sequence = path->needs_sequence_num;
	Bitmapset *chunk_attrs_found = NULL;

#if PG16_LT
	Bitmapset *selectedCols = path->info->ht_rte->selectedCols;
#else
	RTEPermissionInfo *perminfo =
		getRTEPermissionInfo(root->parse->rteperminfos, path->info->ht_rte);
	Bitmapset *selectedCols = perminfo->selectedCols;
#endif
	/*
	 * FIXME this way to determine which columns are used is actually wrong, see
	 * https://github.com/timescale/timescaledb/issues/4195#issuecomment-1104238863
	 * Left as is for now, because changing it uncovers a whole new story with
	 * ctid.
	 */
	check_for_system_columns(selectedCols);

	/*
	 * We allow tableoid system column, it won't be in the targetlist but will
	 * be added at decompression time. Always mark it as found.
	 */
	if (bms_is_member(TableOidAttributeNumber - FirstLowInvalidHeapAttributeNumber,
					  chunk_attrs_needed))
	{
		chunk_attrs_found =
			bms_add_member(chunk_attrs_found,
						   TableOidAttributeNumber - FirstLowInvalidHeapAttributeNumber);
	}

	/*
	 * Fill the helper array of compressed attno -> compression info.
	 */
	FormData_hypertable_compression **compressed_attno_to_compression_info =
		palloc0(sizeof(void *) * (path->info->compressed_rel->max_attr + 1));
	ListCell *lc;
	foreach (lc, path->info->hypertable_compression_info)
	{
		FormData_hypertable_compression *fd = lfirst(lc);
		AttrNumber compressed_attno =
			get_attnum(path->info->compressed_rte->relid, NameStr(fd->attname));

		if (compressed_attno == InvalidAttrNumber)
		{
			elog(ERROR,
				 "column '%s' not found in the compressed chunk '%s'",
				 NameStr(fd->attname),
				 get_rel_name(path->info->compressed_rte->relid));
		}

		compressed_attno_to_compression_info[compressed_attno] = fd;
	}

	/*
	 * Go over the scan targetlist and determine to which output column each
	 * scan column goes, saving other additional info as we do that.
	 */
	path->have_bulk_decompression_columns = false;
	path->decompression_map = NIL;
	foreach (lc, scan_tlist)
	{
		TargetEntry *target = (TargetEntry *) lfirst(lc);
		if (!IsA(target->expr, Var))
		{
			elog(ERROR, "compressed scan targetlist entries must be Vars");
		}

		Var *var = castNode(Var, target->expr);
		Assert((Index) var->varno == path->info->compressed_rel->relid);
		AttrNumber compressed_attno = var->varattno;

		if (compressed_attno == InvalidAttrNumber)
		{
			/*
			 * We shouldn't have whole-row vars in the compressed scan tlist,
			 * they are going to be built by final projection of DecompressChunk
			 * custom scan.
			 * See compressed_rel_setup_reltarget().
			 */
			elog(ERROR, "compressed scan targetlist must not have whole-row vars");
		}

		const char *column_name = get_attname(path->info->compressed_rte->relid,
											  compressed_attno,
											  /* missing_ok = */ false);

		AttrNumber destination_attno_in_uncompressed_chunk = 0;
		FormData_hypertable_compression *compression_info =
			compressed_attno_to_compression_info[compressed_attno];
		if (compression_info)
		{
			/*
			 * Normal column, not a metadata column.
			 */
			AttrNumber hypertable_attno = get_attnum(path->info->ht_rte->relid, column_name);
			AttrNumber chunk_attno = get_attnum(path->info->chunk_rte->relid, column_name);
			Assert(hypertable_attno != InvalidAttrNumber);
			Assert(chunk_attno != InvalidAttrNumber);

			/*
			 * The versions older than this commit didn't set up the proper
			 * collation and typmod for segmentby columns in compressed chunks,
			 * so we have to determine them from the main hypertable.
			 * Additionally, we have to set the proper type for the compressed
			 * columns. It would be cool to get rid of this code someday and
			 * just use the types from the compressed chunk, but the problem is
			 * that we have to support the chunks created by the older versions
			 * of TimescaleDB.
			 */
			if (compression_info->algo_id == _INVALID_COMPRESSION_ALGORITHM)
			{
				get_atttypetypmodcoll(path->info->ht_rte->relid,
									  hypertable_attno,
									  &var->vartype,
									  &var->vartypmod,
									  &var->varcollid);
			}

			if (bms_is_member(0 - FirstLowInvalidHeapAttributeNumber, chunk_attrs_needed))
			{
				/*
				 * attno = 0 means whole-row var. Output all the columns.
				 */
				destination_attno_in_uncompressed_chunk = chunk_attno;
				chunk_attrs_found =
					bms_add_member(chunk_attrs_found,
								   chunk_attno - FirstLowInvalidHeapAttributeNumber);
			}
			else if (bms_is_member(chunk_attno - FirstLowInvalidHeapAttributeNumber,
								   chunk_attrs_needed))
			{
				destination_attno_in_uncompressed_chunk = chunk_attno;
				chunk_attrs_found =
					bms_add_member(chunk_attrs_found,
								   chunk_attno - FirstLowInvalidHeapAttributeNumber);
			}
		}
		else
		{
			/*
			 * Metadata column.
			 * We always need count column, and sometimes a sequence number
			 * column. We don't output them, but use them for decompression,
			 * hence the special negative destination attnos.
			 * The min/max metadata columns are normally not required for output
			 * or decompression, they are used only as filter for the compressed
			 * scan, so we skip them here.
			 */
			Assert(strncmp(column_name,
						   COMPRESSION_COLUMN_METADATA_PREFIX,
						   strlen(COMPRESSION_COLUMN_METADATA_PREFIX)) == 0);

			if (strcmp(column_name, COMPRESSION_COLUMN_METADATA_COUNT_NAME) == 0)
			{
				destination_attno_in_uncompressed_chunk = DECOMPRESS_CHUNK_COUNT_ID;
				missing_count = false;
			}
			else if (path->needs_sequence_num &&
					 strcmp(column_name, COMPRESSION_COLUMN_METADATA_SEQUENCE_NUM_NAME) == 0)
			{
				destination_attno_in_uncompressed_chunk = DECOMPRESS_CHUNK_SEQUENCE_NUM_ID;
				missing_sequence = false;
			}
		}

		path->decompression_map =
			lappend_int(path->decompression_map, destination_attno_in_uncompressed_chunk);
		path->is_segmentby_column =
			lappend_int(path->is_segmentby_column,
						compression_info && compression_info->segmentby_column_index != 0);

		const bool bulk_decompression_possible =
			destination_attno_in_uncompressed_chunk > 0 && compression_info &&
			tsl_get_decompress_all_function(compression_info->algo_id) != NULL;
		path->have_bulk_decompression_columns |= bulk_decompression_possible;
		path->bulk_decompression_column =
			lappend_int(path->bulk_decompression_column, bulk_decompression_possible);
	}

	/*
	 * Check that we have found all the needed columns in the compressed targetlist.
	 * We can't conveniently check that we have all columns for all-row vars, so
	 * skip attno 0 in this check.
	 */
	Bitmapset *attrs_not_found = bms_difference(chunk_attrs_needed, chunk_attrs_found);
	int bit = bms_next_member(attrs_not_found, 0 - FirstLowInvalidHeapAttributeNumber);
	if (bit >= 0)
	{
		elog(ERROR,
			 "column '%s' (%d) not found in the targetlist for compressed chunk '%s'",
			 get_attname(path->info->chunk_rte->relid,
						 bit + FirstLowInvalidHeapAttributeNumber,
						 /* missing_ok = */ true),
			 bit + FirstLowInvalidHeapAttributeNumber,
			 get_rel_name(path->info->compressed_rte->relid));
	}

	if (missing_count)
	{
		elog(ERROR, "the count column was not found in the compressed targetlist");
	}

	if (missing_sequence)
	{
		elog(ERROR, "the sequence column was not found in the compressed scan targetlist");
	}
}

/* replace vars that reference the compressed table with ones that reference the
 * uncompressed one. Based on replace_nestloop_params
 */
static Node *
replace_compressed_vars(Node *node, CompressionInfo *info)
{
	if (node == NULL)
		return NULL;

	if (IsA(node, Var))
	{
		Var *var = (Var *) node;
		Var *new_var;
		char *colname;

		/* constify tableoid in quals */
		if ((Index) var->varno == info->chunk_rel->relid &&
			var->varattno == TableOidAttributeNumber)
			return (Node *)
				makeConst(OIDOID, -1, InvalidOid, 4, (Datum) info->chunk_rte->relid, false, true);

		/* Upper-level Vars should be long gone at this point */
		Assert(var->varlevelsup == 0);
		/* If not to be replaced, we can just return the Var unmodified */
		if ((Index) var->varno != info->compressed_rel->relid)
			return node;

		/* Create a decompressed Var to replace the compressed one */
		colname = get_attname(info->compressed_rte->relid, var->varattno, false);
		new_var = makeVar(info->chunk_rel->relid,
						  get_attnum(info->chunk_rte->relid, colname),
						  var->vartype,
						  var->vartypmod,
						  var->varcollid,
						  var->varlevelsup);

		if (!AttributeNumberIsValid(new_var->varattno))
			elog(ERROR, "cannot find column %s on decompressed chunk", colname);

		/* And return the replacement var */
		return (Node *) new_var;
	}
	if (IsA(node, PlaceHolderVar))
		elog(ERROR, "ignoring placeholders");

	return expression_tree_mutator(node, replace_compressed_vars, (void *) info);
}

/*
 * Find the resno of the given attribute in the provided target list
 */
static AttrNumber
find_attr_pos_in_tlist(List *targetlist, AttrNumber pos)
{
	ListCell *lc;

	Assert(targetlist != NIL);
	Assert(pos > 0 && pos != InvalidAttrNumber);

	foreach (lc, targetlist)
	{
		TargetEntry *target = (TargetEntry *) lfirst(lc);

		if (!IsA(target->expr, Var))
			elog(ERROR, "compressed scan targetlist entries must be Vars");

		Var *var = castNode(Var, target->expr);
		AttrNumber compressed_attno = var->varattno;

		if (compressed_attno == pos)
			return target->resno;
	}

	elog(ERROR, "Unable to locate var %d in targetlist", pos);
	pg_unreachable();
}

Plan *
decompress_chunk_plan_create(PlannerInfo *root, RelOptInfo *rel, CustomPath *path,
							 List *decompressed_tlist, List *clauses, List *custom_plans)
{
	DecompressChunkPath *dcpath = (DecompressChunkPath *) path;
	CustomScan *decompress_plan = makeNode(CustomScan);
	Scan *compressed_scan = linitial(custom_plans);
	Path *compressed_path = linitial(path->custom_paths);
	List *settings;
	ListCell *lc;

	Assert(list_length(custom_plans) == 1);
	Assert(list_length(path->custom_paths) == 1);

	decompress_plan->flags = path->flags;
	decompress_plan->methods = &decompress_chunk_plan_methods;
	decompress_plan->scan.scanrelid = dcpath->info->chunk_rel->relid;

	/* output target list */
	decompress_plan->scan.plan.targetlist = decompressed_tlist;
	/* input target list */
	decompress_plan->custom_scan_tlist = NIL;

	if (IsA(compressed_path, IndexPath))
	{
		/*
		 * Check if any of the decompressed scan clauses are redundant with
		 * the compressed index scan clauses. Note that we can't use
		 * is_redundant_derived_clause() here, because it can't work with
		 * IndexClause's, so we use some custom code based on it.
		 */
		IndexPath *ipath = castNode(IndexPath, compressed_path);
		foreach (lc, clauses)
		{
			RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc);

			ListCell *indexclause_cell = NULL;
			if (rinfo->parent_ec != NULL)
			{
				foreach (indexclause_cell, ipath->indexclauses)
				{
					IndexClause *indexclause = lfirst(indexclause_cell);
					RestrictInfo *index_rinfo = indexclause->rinfo;
					if (index_rinfo->parent_ec == rinfo->parent_ec)
					{
						break;
					}
				}
			}

			if (indexclause_cell != NULL)
			{
				/* We already have an index clause derived from same EquivalenceClass. */
				continue;
			}

			/*
			 * We don't have this clause in the underlying index scan, add it
			 * to the decompressed scan.
			 */
			decompress_plan->scan.plan.qual =
				lappend(decompress_plan->scan.plan.qual, rinfo->clause);
		}
	}
	else
	{
		foreach (lc, clauses)
		{
			RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc);
			decompress_plan->scan.plan.qual =
				lappend(decompress_plan->scan.plan.qual, rinfo->clause);
		}
	}

	decompress_plan->scan.plan.qual =
		(List *) replace_compressed_vars((Node *) decompress_plan->scan.plan.qual, dcpath->info);

	/*
	 * Try to use a physical tlist if possible. There's no reason to do the
	 * extra work of projecting the result of compressed chunk scan, because
	 * DecompressChunk can choose only the needed columns itself.
	 * Note that Postgres uses the CP_EXACT_TLIST option when planning the child
	 * paths of the Custom path, so we won't automatically get a physical tlist
	 * here.
	 */
	bool target_list_compressed_is_physical = false;
	if (compressed_path->pathtype == T_IndexOnlyScan)
	{
		compressed_scan->plan.targetlist = ((IndexPath *) compressed_path)->indexinfo->indextlist;
	}
	else
	{
		List *physical_tlist = build_physical_tlist(root, dcpath->info->compressed_rel);
		/* Can be null if the relation has dropped columns. */
		if (physical_tlist)
		{
			compressed_scan->plan.targetlist = physical_tlist;
			target_list_compressed_is_physical = true;
		}
	}

	/*
	 * Determine which columns we have to decompress.
	 * decompressed_tlist is sometimes empty, e.g. for a direct select from
	 * chunk. We have a ProjectionPath above DecompressChunk in this case, and
	 * the targetlist for this path is not built by the planner
	 * (CP_IGNORE_TLIST). This is why we have to examine rel pathtarget.
	 * Looking at the targetlist is not enough, we also have to decompress the
	 * columns participating in quals and in pathkeys.
	 */
	Bitmapset *chunk_attrs_needed = NULL;
	pull_varattnos((Node *) decompress_plan->scan.plan.qual,
				   dcpath->info->chunk_rel->relid,
				   &chunk_attrs_needed);
	pull_varattnos((Node *) dcpath->custom_path.path.pathtarget->exprs,
				   dcpath->info->chunk_rel->relid,
				   &chunk_attrs_needed);

	/*
	 * Determine which compressed colum goes to which output column.
	 */
	build_decompression_map(root, dcpath, compressed_scan->plan.targetlist, chunk_attrs_needed);

	/* Build heap sort info for sorted_merge_append */
	List *sort_options = NIL;

	if (dcpath->batch_sorted_merge)
	{
		/*
		 * 'order by' of the query and the 'order by' of the compressed batches
		 * match, so we will we use a heap to merge the batches. For the heap we
		 * need a compare function that determines the heap order. This function
		 * is constructed here.
		 *
		 * Batch sorted merge is done over the decompressed chunk scan tuple, so
		 * we must match the pathkeys to the decompressed chunk tupdesc.
		 */

		int numsortkeys = list_length(dcpath->custom_path.path.pathkeys);

		List *sort_col_idx = NIL;
		List *sort_ops = NIL;
		List *sort_collations = NIL;
		List *sort_nulls = NIL;

		/*
		 */
		ListCell *lc;
		foreach (lc, dcpath->custom_path.path.pathkeys)
		{
			PathKey *pk = lfirst(lc);
			EquivalenceClass *ec = pk->pk_eclass;

			/*
			 * Find the equivalence member that belongs to decompressed relation.
			 */
			ListCell *membercell = NULL;
			foreach (membercell, ec->ec_members)
			{
				EquivalenceMember *em = lfirst(membercell);

				if (em->em_is_const)
				{
					continue;
				}

				int em_relid;
				if (!bms_get_singleton_member(em->em_relids, &em_relid))
				{
					continue;
				}

				if ((Index) em_relid != dcpath->info->chunk_rel->relid)
				{
					continue;
				}

				Ensure(IsA(em->em_expr, Var),
					   "non-Var pathkey not expected for compressed batch sorted mege");

				/*
				 * We found a Var equivalence member that belongs to the
				 * decompressed relation. We can use its varattno directly for
				 * the comparison operator, because it operates on the
				 * decompressed scan tuple.
				 */
				Var *var = castNode(Var, em->em_expr);
				Assert((Index) var->varno == (Index) em_relid);

				/*
				 * Look up the correct sort operator from the PathKey's slightly
				 * abstracted representation.
				 */
				Oid sortop = get_opfamily_member(pk->pk_opfamily,
												 var->vartype,
												 var->vartype,
												 pk->pk_strategy);
				if (!OidIsValid(sortop)) /* should not happen */
					elog(ERROR,
						 "missing operator %d(%u,%u) in opfamily %u",
						 pk->pk_strategy,
						 var->vartype,
						 var->vartype,
						 pk->pk_opfamily);

				sort_col_idx = lappend_oid(sort_col_idx, var->varattno);
				sort_collations = lappend_oid(sort_collations, var->varcollid);
				sort_nulls = lappend_oid(sort_nulls, pk->pk_nulls_first);
				sort_ops = lappend_oid(sort_ops, sortop);

				break;
			}

			Ensure(membercell != NULL,
				   "could not find matching decompressed chunk column for batch sorted merge "
				   "pathkey");
		}

		sort_options = list_make4(sort_col_idx, sort_ops, sort_collations, sort_nulls);

		/*
		 * Build a sort node for the compressed batches. The sort function is
		 * derived from the sort function of the pathkeys, except that it refers
		 * to the min and max metadata columns of the batches. We have already
		 * verified that the pathkeys match the compression order_by, so this
		 * mapping is possible.
		 */
		AttrNumber *sortColIdx = palloc(sizeof(AttrNumber) * numsortkeys);
		Oid *sortOperators = palloc(sizeof(Oid) * numsortkeys);
		Oid *collations = palloc(sizeof(Oid) * numsortkeys);
		bool *nullsFirst = palloc(sizeof(bool) * numsortkeys);
		for (int i = 0; i < numsortkeys; i++)
		{
			Oid sortop = list_nth_oid(sort_ops, i);

			/* Find the operator in pg_amop --- failure shouldn't happen */
			Oid opfamily, opcintype;
			int16 strategy;
			if (!get_ordering_op_properties(list_nth_oid(sort_ops, i),
											&opfamily,
											&opcintype,
											&strategy))
				elog(ERROR, "operator %u is not a valid ordering operator", sortOperators[i]);

			/*
			 * This way to determine the matching metadata column works, because
			 * we have already verified that the pathkeys match the compression
			 * orderby.
			 */
			Assert(strategy == BTLessStrategyNumber || strategy == BTGreaterStrategyNumber);
			char *meta_col_name = strategy == BTLessStrategyNumber ?
									  column_segment_min_name(i + 1) :
									  column_segment_max_name(i + 1);

			AttrNumber attr_position =
				get_attnum(dcpath->info->compressed_rte->relid, meta_col_name);

			if (attr_position == InvalidAttrNumber)
				elog(ERROR, "couldn't find metadata column \"%s\"", meta_col_name);

			/*
			 * If the the compressed target list is not based on the layout of
			 * the uncompressed chunk (see comment for physical_tlist above),
			 * adjust the position of the attribute.
			 */
			if (target_list_compressed_is_physical)
				sortColIdx[i] = attr_position;
			else
				sortColIdx[i] =
					find_attr_pos_in_tlist(compressed_scan->plan.targetlist, attr_position);

			sortOperators[i] = sortop;
			collations[i] = list_nth_oid(sort_collations, i);
			nullsFirst[i] = list_nth_oid(sort_nulls, i);
		}

		/* Now build the compressed batches sort node */
		Sort *sort = ts_make_sort((Plan *) compressed_scan,
								  numsortkeys,
								  sortColIdx,
								  sortOperators,
								  collations,
								  nullsFirst);

		decompress_plan->custom_plans = list_make1(sort);
	}
	else
	{
		/*
		 * Add a sort if the compressed scan is not ordered appropriately.
		 */
		if (!pathkeys_contained_in(dcpath->compressed_pathkeys, compressed_path->pathkeys))
		{
			List *compressed_pks = dcpath->compressed_pathkeys;
			Sort *sort = ts_make_sort_from_pathkeys((Plan *) compressed_scan,
													compressed_pks,
													bms_make_singleton(compressed_scan->scanrelid));
			decompress_plan->custom_plans = list_make1(sort);
		}
		else
		{
			decompress_plan->custom_plans = custom_plans;
		}
	}

	Assert(list_length(custom_plans) == 1);

	const bool enable_bulk_decompression = !dcpath->batch_sorted_merge &&
										   ts_guc_enable_bulk_decompression &&
										   dcpath->have_bulk_decompression_columns;

	settings = list_make5_int(dcpath->info->hypertable_id,
							  dcpath->info->chunk_rte->relid,
							  dcpath->reverse,
							  dcpath->batch_sorted_merge,
							  enable_bulk_decompression);

	decompress_plan->custom_private = list_make5(settings,
												 dcpath->decompression_map,
												 dcpath->is_segmentby_column,
												 dcpath->bulk_decompression_column,
												 sort_options);

	return &decompress_plan->scan.plan;
}
