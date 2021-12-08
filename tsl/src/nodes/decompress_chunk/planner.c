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
#include <optimizer/restrictinfo.h>
#include <optimizer/tlist.h>
#include <parser/parsetree.h>
#include <utils/builtins.h>
#include <utils/typcache.h>

#include "compression/compression.h"
#include "compression/create.h"
#include "nodes/decompress_chunk/decompress_chunk.h"
#include "nodes/decompress_chunk/planner.h"
#include "nodes/decompress_chunk/exec.h"
#include "import/planner.h"
#include "guc.h"
#include "custom_type_cache.h"

static CustomScanMethods decompress_chunk_plan_methods = {
	.CustomName = "DecompressChunk",
	.CreateCustomScanState = decompress_chunk_state_create,
};

void
_decompress_chunk_init(void)
{
	TryRegisterCustomScanMethods(&decompress_chunk_plan_methods);
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

/*
 * Find matching column attno for compressed chunk based on hypertable attno.
 *
 * Since we dont want aliasing to interfere we lookup directly in catalog
 * instead of using RangeTblEntry.
 */
static AttrNumber
get_compressed_attno(CompressionInfo *info, AttrNumber ht_attno)
{
	AttrNumber compressed_attno;
	Assert(info->ht_rte);
	char *chunk_col = get_attname(info->ht_rte->relid, ht_attno, false);
	compressed_attno = get_attnum(info->compressed_rte->relid, chunk_col);

	if (compressed_attno == InvalidAttrNumber)
		elog(ERROR, "no matching column in compressed chunk found");

	return compressed_attno;
}

static TargetEntry *
make_compressed_scan_targetentry(DecompressChunkPath *path, AttrNumber ht_attno, int tle_index)
{
	Var *scan_var;
	char *ht_attname = get_attname(path->info->ht_rte->relid, ht_attno, false);
	FormData_hypertable_compression *ht_info =
		get_column_compressioninfo(path->info->hypertable_compression_info, ht_attname);
	AttrNumber scan_varattno = get_compressed_attno(path->info, ht_attno);
	AttrNumber chunk_attno = get_attnum(path->info->chunk_rte->relid, ht_attname);

	Assert(!get_rte_attribute_is_dropped(path->info->ht_rte, ht_attno));
	Assert(!get_rte_attribute_is_dropped(path->info->chunk_rte, chunk_attno));
	Assert(!get_rte_attribute_is_dropped(path->info->compressed_rte, scan_varattno));

	if (ht_info->algo_id == _INVALID_COMPRESSION_ALGORITHM)
	{
		Oid typid, collid;
		int32 typmod;
		get_atttypetypmodcoll(path->info->ht_rte->relid, ht_attno, &typid, &typmod, &collid);
		scan_var =
			makeVar(path->info->compressed_rel->relid, scan_varattno, typid, typmod, collid, 0);
	}
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

	/* check for system columns */
	bit = bms_next_member(attrs_used, -1);
	if (bit > 0 && bit + FirstLowInvalidHeapAttributeNumber < 0)
	{
		/* we support tableoid so skip that */
		if (bit == TableOidAttributeNumber - FirstLowInvalidHeapAttributeNumber)
			bit = bms_next_member(attrs_used, bit);

		if (bit > 0 && bit + FirstLowInvalidHeapAttributeNumber < 0)
			elog(ERROR, "transparent decompression only supports tableoid system column");
	}

	/* check for reference to whole row */
	if (bms_is_member(0 - FirstLowInvalidHeapAttributeNumber, attrs_used))
	{
		ListCell *lc;
		AttrNumber ht_attno = 0;

		foreach (lc, path->info->ht_rte->eref->colnames)
		{
			const char *chunk_col = strVal(lfirst(lc));
			ht_attno++;

			/*
			 * dropped columns have empty string
			 */
			if (IsA(lfirst(lc), String) && strlen(chunk_col) > 0)
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
		for (bit = bms_next_member(attrs_used, 0 - FirstLowInvalidHeapAttributeNumber); bit > 0;
			 bit = bms_next_member(attrs_used, bit))
		{
			/* bits are offset by FirstLowInvalidHeapAttributeNumber */
			AttrNumber ht_attno = bit + FirstLowInvalidHeapAttributeNumber;

			tle = make_compressed_scan_targetentry(path, ht_attno, list_length(scan_tlist) + 1);
			scan_tlist = lappend(scan_tlist, tle);
		}
	}

	return scan_tlist;
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
		if (var->varno == info->chunk_rel->relid && var->varattno == TableOidAttributeNumber)
			return (Node *)
				makeConst(OIDOID, -1, InvalidOid, 4, (Datum) info->chunk_rte->relid, false, true);

		/* Upper-level Vars should be long gone at this point */
		Assert(var->varlevelsup == 0);
		/* If not to be replaced, we can just return the Var unmodified */
		if (var->varno != info->compressed_rel->relid)
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

typedef struct CompressedAttnoContext
{
	Bitmapset *compressed_attnos;
	Index compress_relid;
} CompressedAttnoContext;

/* check if the clause refers to any attributes that are in compressed
 * form.
 */
static bool
clause_has_compressed_attrs(Node *node, void *context)
{
	if (node == NULL)
		return true;
	if (IsA(node, Var))
	{
		CompressedAttnoContext *cxt = (CompressedAttnoContext *) context;
		Var *var = (Var *) node;
		if (var->varno == cxt->compress_relid)
		{
			if (bms_is_member(var->varattno, cxt->compressed_attnos))
				return true;
		}
	}
	return expression_tree_walker(node, clause_has_compressed_attrs, context);
}

Plan *
decompress_chunk_plan_create(PlannerInfo *root, RelOptInfo *rel, CustomPath *path, List *tlist,
							 List *clauses, List *custom_plans)
{
	DecompressChunkPath *dcpath = (DecompressChunkPath *) path;
	CustomScan *cscan = makeNode(CustomScan);
	Scan *compressed_scan = linitial(custom_plans);
	Path *compressed_path = linitial(path->custom_paths);
	List *settings;
	ListCell *lc;

	Assert(list_length(custom_plans) == 1);
	Assert(list_length(path->custom_paths) == 1);

	cscan->flags = path->flags;
	cscan->methods = &decompress_chunk_plan_methods;
	cscan->scan.scanrelid = dcpath->info->chunk_rel->relid;

	/* output target list */
	cscan->scan.plan.targetlist = tlist;
	/* input target list */
	cscan->custom_scan_tlist = NIL;

	if (IsA(compressed_path, IndexPath))
	{
		/* from create_indexscan_plan() */
		IndexPath *ipath = castNode(IndexPath, compressed_path);
		List *indexqual = NIL;
		Plan *indexplan;
		foreach (lc, clauses)
		{
			RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc);
			if (is_redundant_derived_clause(rinfo, ipath->indexclauses))
				continue; /* dup or derived from same EquivalenceClass */
			cscan->scan.plan.qual = lappend(cscan->scan.plan.qual, rinfo->clause);
		}
		/* joininfo clauses on the compressed chunk rel have to
		 * contain clauses on both compressed and
		 * decompressed attnos. joininfo clauses get translated into
		 * ParamPathInfo for the indexpath. But the index scans can't
		 * handle compressed attributes, so remove them from the
		 * indexscans here. (these are included in the `clauses` passed in
		 * to the function and so were added as filters
		 * for cscan->scan.plan.qual in the loop above. )
		 */
		indexplan = linitial(custom_plans);
		Assert(IsA(indexplan, IndexScan) || IsA(indexplan, IndexOnlyScan));
		foreach (lc, indexplan->qual)
		{
			Node *expr = (Node *) lfirst(lc);
			CompressedAttnoContext cxt;
			Index compress_relid = dcpath->info->compressed_rel->relid;
			cxt.compress_relid = compress_relid;
			cxt.compressed_attnos = dcpath->info->compressed_chunk_compressed_attnos;

			if (!clause_has_compressed_attrs((Node *) expr, &cxt))
				indexqual = lappend(indexqual, expr);
		}
		indexplan->qual = indexqual;
	}
	else if (IsA(compressed_path, BitmapHeapPath))
	{
		/* To increase performance, we should remove quals that are redundant with the Bitmap scan
		 * Code from create_bitmap_scan_plan does something similar, and could be used as a starting
		 * point.
		 */
		foreach (lc, clauses)
		{
			RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc);
			cscan->scan.plan.qual = lappend(cscan->scan.plan.qual, rinfo->clause);
		}
	}
	else
	{
		foreach (lc, clauses)
		{
			RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc);
			cscan->scan.plan.qual = lappend(cscan->scan.plan.qual, rinfo->clause);
		}
	}

	cscan->scan.plan.qual =
		(List *) replace_compressed_vars((Node *) cscan->scan.plan.qual, dcpath->info);

	compressed_scan->plan.targetlist = build_scan_tlist(dcpath);
	if (!pathkeys_contained_in(dcpath->compressed_pathkeys, compressed_path->pathkeys))
	{
		List *compressed_pks = dcpath->compressed_pathkeys;
		Sort *sort = ts_make_sort_from_pathkeys((Plan *) compressed_scan,
												compressed_pks,
												bms_make_singleton(compressed_scan->scanrelid));
		cscan->custom_plans = list_make1(sort);
	}
	else
	{
		cscan->custom_plans = custom_plans;
	}

	Assert(list_length(custom_plans) == 1);

	settings = list_make3_int(dcpath->info->hypertable_id,
							  dcpath->info->chunk_rte->relid,
							  dcpath->reverse);
	cscan->custom_private = list_make2(settings, dcpath->varattno_map);

	return &cscan->scan.plan;
}
