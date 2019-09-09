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
#include <optimizer/clauses.h>
#include <optimizer/pathnode.h>
#include <optimizer/paths.h>
#include <optimizer/placeholder.h>
#include <optimizer/planmain.h>
#include <optimizer/prep.h>
#include <optimizer/restrictinfo.h>
#include <optimizer/subselect.h>
#include <optimizer/tlist.h>
#include <optimizer/var.h>
#include <parser/parsetree.h>
#include <utils/builtins.h>
#include <utils/typcache.h>

#include "compat.h"
#include "compression/create.h"
#include "decompress_chunk/decompress_chunk.h"
#include "decompress_chunk/planner.h"
#include "decompress_chunk/exec.h"
#include "planner_import.h"
#include "guc.h"
#include "custom_type_cache.h"

static CustomScanMethods decompress_chunk_plan_methods = {
	.CustomName = "DecompressChunk",
	.CreateCustomScanState = decompress_chunk_state_create,
};

void
_decompress_chunk_init(void)
{
	/*
	 * Because we reinitialize the tsl stuff when the license
	 * changes the init function may be called multiple times
	 * per session so we check if ChunkDecompress node has been
	 * registered already here to prevent registering it twice.
	 */
	if (GetCustomScanMethods("DecompressChunk", true) == NULL)
	{
		RegisterCustomScanMethods(&decompress_chunk_plan_methods);
	}
}

static TargetEntry *
make_compressed_scan_meta_targetentry(DecompressChunkPath *path, char *column_name, int id,
									  int tle_index)
{
	Var *scan_var;
	AttrNumber compressed_attno = get_attnum(path->info->compressed_rte->relid, column_name);
	if (compressed_attno == InvalidAttrNumber)
		elog(ERROR, "lookup failed for column \"%s\"", column_name);

	/*
	 * this is called for adding the count and sequence num column which are both int4
	 * if we ever need columns with different datatype here we need to add
	 * dynamic type lookup
	 */
	Assert(get_atttype(path->info->compressed_rte->relid, compressed_attno) == INT4OID);
	scan_var = makeVar(path->info->compressed_rel->relid, compressed_attno, INT4OID, -1, 0, 0);
	path->varattno_map = lappend_int(path->varattno_map, id);

	return makeTargetEntry((Expr *) scan_var, tle_index, NULL, false);
}

static TargetEntry *
make_compressed_scan_targetentry(DecompressChunkPath *path, AttrNumber ht_attno, int tle_index)
{
	Var *scan_var;
	char *ht_attname = get_attname_compat(path->info->ht_rte->relid, ht_attno, false);
	FormData_hypertable_compression *ht_info =
		get_column_compressioninfo(path->info->hypertable_compression_info, ht_attname);
	AttrNumber scan_varattno = get_compressed_attno(path->info, ht_attno);
	AttrNumber chunk_attno = get_attnum(path->info->chunk_rte->relid, ht_attname);

	if (ht_info->algo_id == 0)
		scan_var = makeVar(path->info->compressed_rel->relid,
						   scan_varattno,
						   get_atttype(path->info->ht_rte->relid, ht_attno),
						   -1,
						   0,
						   0);
	else
		scan_var = makeVar(path->info->compressed_rel->relid,
						   scan_varattno,
						   ts_custom_type_cache_get(CUSTOM_TYPE_COMPRESSED_DATA)->type_oid,
						   -1,
						   0,
						   0);
	path->varattno_map = lappend_int(path->varattno_map, chunk_attno);

	return makeTargetEntry((Expr *) scan_var, tle_index, NULL, false);
}

/*
 * build targetlist for scan on compressed chunk
 *
 * Since we do not adjust selectedCols in RangeTblEntry for chunks
 * we use selectedCols from the hypertable RangeTblEntry to
 * build the target list for the compressed chunk and adjust
 * attno accordingly
 */
static List *
build_scan_tlist(DecompressChunkPath *path)
{
	List *scan_tlist = NIL;
	Bitmapset *attrs_used = path->info->ht_rte->selectedCols;
	TargetEntry *tle;
	int bit;

	path->varattno_map = NIL;

	/* add count column */
	tle = make_compressed_scan_meta_targetentry(path,
												COMPRESSION_COLUMN_METADATA_COUNT_NAME,
												DECOMPRESS_CHUNK_COUNT_ID,
												list_length(scan_tlist) + 1);
	scan_tlist = lappend(scan_tlist, tle);

	/* add sequence num column */
	if (path->needs_sequence_num)
	{
		tle = make_compressed_scan_meta_targetentry(path,
													COMPRESSION_COLUMN_METADATA_SEQUENCE_NUM_NAME,
													DECOMPRESS_CHUNK_SEQUENCE_NUM_ID,
													list_length(scan_tlist) + 1);
		scan_tlist = lappend(scan_tlist, tle);
	}

	bit = bms_next_member(attrs_used, -1);
	if (bit > 0 && bit + FirstLowInvalidHeapAttributeNumber < 0)
		elog(ERROR, "transparent decompression does not support system attributes");

	/* check for reference to whole row */
	if (bms_is_member(0 - FirstLowInvalidHeapAttributeNumber, attrs_used))
	{
		ListCell *lc;
		AttrNumber ht_attno = 0;

		foreach (lc, path->info->ht_rte->eref->colnames)
		{
			Value *chunk_col = (Value *) lfirst(lc);
			ht_attno++;

			/*
			 * dropped columns have empty string
			 */
			if (IsA(lfirst(lc), String) && strlen(chunk_col->val.str) > 0)
			{
				tle = make_compressed_scan_targetentry(path, ht_attno, list_length(scan_tlist) + 1);
				scan_tlist = lappend(scan_tlist, tle);
			}
		}
	}
	else
	{
		/*
		 * we only need to find unique varattno references here
		 * multiple references to the same column will be handled by projection
		 * we need to include junk columns because they might be needed for
		 * filtering or sorting
		 */
		for (bit = bms_next_member(attrs_used, -1); bit > 0; bit = bms_next_member(attrs_used, bit))
		{
			/* bits are offset by FirstLowInvalidHeapAttributeNumber */
			AttrNumber ht_attno = bit + FirstLowInvalidHeapAttributeNumber;

			tle = make_compressed_scan_targetentry(path, ht_attno, list_length(scan_tlist) + 1);
			scan_tlist = lappend(scan_tlist, tle);
		}
	}

	return scan_tlist;
}

static List *
build_compressed_scan_pathkeys(PlannerInfo *root, DecompressChunkPath *path)
{
	Var *var;
	int varattno;
	List *chunk_pathkeys = path->cpath.path.pathkeys;
	List *compressed_pathkeys = NIL;
	PathKey *pk;

	/* all segmentby columns need to be prefix of pathkeys */
	if (path->info->num_segmentby_columns > 0)
	{
		Bitmapset *segmentby_columns = NULL;
		ListCell *lc;
		char *column_name;
		FormData_hypertable_compression *ci;
		Oid sortop;

		for (lc = list_head(chunk_pathkeys);
			 lc != NULL && bms_num_members(segmentby_columns) < path->info->num_segmentby_columns;
			 lc = lnext(lc))
		{
			PathKey *pk = lfirst(lc);
			var = (Var *) ts_find_em_expr_for_rel(pk->pk_eclass, path->info->chunk_rel);

			if (var == NULL || !IsA(var, Var))
				/* this should not happen because we validated the pathkeys when creating the path
				 */
				elog(ERROR, "Invalid pathkey for compressed scan");

			column_name = get_attname_compat(path->info->chunk_rte->relid, var->varattno, false);
			ci = get_column_compressioninfo(path->info->hypertable_compression_info, column_name);

			Assert(ci->segmentby_column_index > 0);
			segmentby_columns = bms_add_member(segmentby_columns, var->varattno);
			varattno = get_attnum(path->info->compressed_rte->relid, column_name);
			var = makeVar(path->info->compressed_rel->relid,
						  varattno,
						  var->vartype,
						  var->vartypmod,
						  var->varcollid,
						  0);

			sortop =
				get_opfamily_member(pk->pk_opfamily, var->vartype, var->vartype, pk->pk_strategy);
			pk = ts_make_pathkey_from_sortop(root,
											 (Expr *) var,
											 NULL,
											 sortop,
											 pk->pk_nulls_first,
											 0,
											 true);
			compressed_pathkeys = lappend(compressed_pathkeys, pk);
		}

		/* we validated this when we created the Path so only asserting here */
		Assert(bms_num_members(segmentby_columns) == path->info->num_segmentby_columns);
	}

	/*
	 * If pathkeys contains non-segmentby columns the rest of the ordering
	 * requirements will be satisfied by ordering by sequence_num
	 */
	if (list_length(chunk_pathkeys) > list_length(compressed_pathkeys))
	{
		Assert(path->needs_sequence_num);
		varattno = get_attnum(path->info->compressed_rte->relid,
							  COMPRESSION_COLUMN_METADATA_SEQUENCE_NUM_NAME);
		var = makeVar(path->info->compressed_rel->relid, varattno, INT4OID, -1, InvalidOid, 0);
		pk =
			ts_make_pathkey_from_sortop(root, (Expr *) var, NULL, Int4LessOperator, false, 0, true);
		compressed_pathkeys = lappend(compressed_pathkeys, pk);
	}
	return compressed_pathkeys;
}

Plan *
decompress_chunk_plan_create(PlannerInfo *root, RelOptInfo *rel, CustomPath *path, List *tlist,
							 List *clauses, List *custom_plans)
{
	DecompressChunkPath *dcpath = (DecompressChunkPath *) path;
	CustomScan *cscan = makeNode(CustomScan);
	Scan *compressed_scan = linitial(custom_plans);
	List *settings;

	Assert(list_length(custom_plans) == 1);
	Assert(IsA(linitial(custom_plans), SeqScan));

	cscan->flags = path->flags;
	cscan->methods = &decompress_chunk_plan_methods;
	cscan->scan.scanrelid = dcpath->info->chunk_rel->relid;

	/* output target list */
	cscan->scan.plan.targetlist = tlist;
	/* input target list */
	cscan->custom_scan_tlist = NIL;
	cscan->scan.plan.qual = get_actual_clauses(clauses);

	compressed_scan->plan.targetlist = build_scan_tlist(dcpath);
	if (path->path.pathkeys)
	{
		List *compressed_pks = build_compressed_scan_pathkeys(root, dcpath);
		Sort *sort = ts_make_sort_from_pathkeys((Plan *) compressed_scan, compressed_pks, NULL);
		cscan->custom_plans = list_make1(sort);
	}
	else
	{
		cscan->custom_plans = custom_plans;
	}

	Assert(list_length(custom_plans) == 1);

	settings = list_make2_int(dcpath->info->hypertable_id, dcpath->reverse);
	cscan->custom_private = list_make2(settings, dcpath->varattno_map);

	return &cscan->scan.plan;
}
