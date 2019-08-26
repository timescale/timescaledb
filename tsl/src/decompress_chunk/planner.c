/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <postgres.h>
#include <access/sysattr.h>
#include <catalog/pg_namespace.h>
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

#include "compat.h"
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

#define META_COUNT_COLUMN_NAME "_ts_meta_count"

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
make_compressed_scan_targetentry(DecompressChunkPath *path, AttrNumber ht_attno, int tle_index)
{
	Var *scan_var;
	if (ht_attno > 0)
	{
		char *ht_attname = get_attname_compat(path->ht_rte->relid, ht_attno, false);
		FormData_hypertable_compression *ht_info =
			get_column_compressioninfo(path->compression_info, ht_attname);
		AttrNumber scan_varattno = get_compressed_attno(path, ht_attno);
		AttrNumber chunk_attno = get_attnum(path->chunk_rte->relid, ht_attname);

		if (ht_info->algo_id == 0)
			scan_var = makeVar(path->compressed_rel->relid,
							   scan_varattno,
							   get_atttype(path->ht_rte->relid, ht_attno),
							   -1,
							   0,
							   0);
		else
			scan_var = makeVar(path->compressed_rel->relid,
							   scan_varattno,
							   ts_custom_type_cache_get(CUSTOM_TYPE_COMPRESSED_DATA)->type_oid,
							   -1,
							   0,
							   0);
		path->varattno_map = lappend_int(path->varattno_map, chunk_attno);
	}
	else
	{
		AttrNumber count_attno = get_attnum(path->compressed_rte->relid, META_COUNT_COLUMN_NAME);
		if (count_attno == InvalidAttrNumber)
			elog(ERROR, "lookup failed for column \"%s\"", META_COUNT_COLUMN_NAME);

		scan_var = makeVar(path->compressed_rel->relid, count_attno, INT4OID, -1, 0, 0);
		path->varattno_map = lappend_int(path->varattno_map, 0);
	}
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
	Bitmapset *attrs_used = path->ht_rte->selectedCols;
	TargetEntry *tle;
	int bit;

	path->varattno_map = NIL;

	/* add count column */
	tle = make_compressed_scan_targetentry(path, 0, list_length(scan_tlist) + 1);
	scan_tlist = lappend(scan_tlist, tle);

	bit = bms_next_member(attrs_used, -1);
	if (bit > 0 && bit + FirstLowInvalidHeapAttributeNumber < 0)
		elog(ERROR, "transparent decompression does not support system attributes");

	/* check for reference to whole row */
	if (bms_is_member(0 - FirstLowInvalidHeapAttributeNumber, attrs_used))
	{
		ListCell *lc;
		AttrNumber ht_attno = 0;

		foreach (lc, path->ht_rte->eref->colnames)
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
	cscan->scan.scanrelid = dcpath->chunk_rel->relid;

	/* output target list */
	cscan->scan.plan.targetlist = tlist;
	/* input target list */
	cscan->custom_scan_tlist = NIL;
	cscan->scan.plan.qual = get_actual_clauses(clauses);

	Assert(list_length(custom_plans) == 1);
	cscan->custom_plans = custom_plans;
	compressed_scan->plan.targetlist = build_scan_tlist(dcpath);

	settings = list_make2_int(dcpath->hypertable_id, dcpath->reverse);
	cscan->custom_private = list_make2(settings, dcpath->varattno_map);

	return &cscan->scan.plan;
}
