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

static CustomScanMethods decompress_chunk_plan_methods = {
	.CustomName = "DecompressChunk",
	.CreateCustomScanState = decompress_chunk_state_create,
};

#define COMPRESSEDDATA_TYPE_NAME "_timescaledb_internal.compressed_data"
#define META_COUNT_COLUMN_NAME "_ts_meta_count"

Oid COMPRESSEDDATAOID = InvalidOid;

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

static void
lookup_compressed_data_oid()
{
	COMPRESSEDDATAOID =
		DatumGetObjectId(DirectFunctionCall1(regtypein, CStringGetDatum(COMPRESSEDDATA_TYPE_NAME)));

	if (!OidIsValid(COMPRESSEDDATAOID))
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("type \"%s\" does not exist", COMPRESSEDDATA_TYPE_NAME)));
}

static TargetEntry *
make_compressed_scan_targetentry(DecompressChunkPath *path, Relation r, AttrNumber chunk_attno,
								 int tle_index)
{
	Var *scan_var;
	if (chunk_attno > 0)
	{
		TupleDesc desc = RelationGetDescr(r);
		Form_pg_attribute attribute = TupleDescAttr(desc, AttrNumberGetAttrOffset(chunk_attno));
		FormData_hypertable_compression *ht_info =
			get_column_compressioninfo(path->compression_info, NameStr(attribute->attname));
		AttrNumber scan_varattno = get_compressed_attno(path, chunk_attno);

		if (ht_info->algo_id == 0)
			scan_var =
				makeVar(path->compressed_rel->relid, scan_varattno, attribute->atttypid, -1, 0, 0);
		else
			scan_var =
				makeVar(path->compressed_rel->relid, scan_varattno, COMPRESSEDDATAOID, -1, 0, 0);
	}
	else
	{
		AttrNumber count_attno = get_attnum(path->compressed_rte->relid, META_COUNT_COLUMN_NAME);
		if (count_attno == InvalidAttrNumber)
			elog(ERROR, "lookup failed for column \"%s\"", META_COUNT_COLUMN_NAME);

		scan_var = makeVar(path->compressed_rel->relid, count_attno, INT4OID, -1, 0, 0);
	}
	return makeTargetEntry((Expr *) scan_var, tle_index, NULL, false);
}

/*
 * build targetlist for scan on compressed chunk
 */
static List *
build_scan_tlist(DecompressChunkPath *dcpath)
{
	List *scan_tlist = NIL;
	List *varattno_map = NIL;
	Bitmapset *attrs_used = dcpath->chunk_rte->selectedCols;
	TargetEntry *tle;
	int bit;
	Relation r = heap_open(dcpath->chunk_rte->relid, NoLock);

	/* add count column */
	tle = make_compressed_scan_targetentry(dcpath, r, 0, list_length(scan_tlist) + 1);
	scan_tlist = lappend(scan_tlist, tle);
	varattno_map = lappend_int(varattno_map, 0);

	bit = bms_next_member(attrs_used, -1);
	if (bit > 0 && bit + FirstLowInvalidHeapAttributeNumber < 0)
		elog(ERROR, "transparent decompression does not support system attributes");

	/* check for reference to whole row */
	if (bms_is_member(0 - FirstLowInvalidHeapAttributeNumber, attrs_used))
	{
		ListCell *lc;
		AttrNumber chunk_attno = 0;

		foreach (lc, dcpath->chunk_rte->eref->colnames)
		{
			Value *chunk_col = (Value *) lfirst(lc);
			chunk_attno++;

			/*
			 * dropped columns have empty string
			 */
			if (IsA(lfirst(lc), String) && strlen(chunk_col->val.str) > 0)
			{
				tle = make_compressed_scan_targetentry(dcpath,
													   r,
													   chunk_attno,
													   list_length(scan_tlist) + 1);
				scan_tlist = lappend(scan_tlist, tle);
				varattno_map = lappend_int(varattno_map, chunk_attno);
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
			AttrNumber uc_attno = bit + FirstLowInvalidHeapAttributeNumber;

			tle =
				make_compressed_scan_targetentry(dcpath, r, uc_attno, list_length(scan_tlist) + 1);
			scan_tlist = lappend(scan_tlist, tle);
			varattno_map = lappend_int(varattno_map, uc_attno);
		}
	}

	dcpath->varattno_map = varattno_map;

	/*
	 * Drop the rel refcount, but keep the access lock till end of transaction
	 * so that the table can't be deleted or have its schema modified
	 * underneath us.
	 */
	heap_close(r, NoLock);

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

	if (!OidIsValid(COMPRESSEDDATAOID))
		lookup_compressed_data_oid();

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
