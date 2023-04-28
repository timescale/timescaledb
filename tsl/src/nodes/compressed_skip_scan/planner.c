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
#include <parser/parsetree.h>
#include <utils/builtins.h>
#include <utils/typcache.h>

#include "compression/compression.h"
#include "compression/create.h"
#include "nodes/skip_scan/skip_scan.h"
#include "nodes/compressed_skip_scan/compressed_skip_scan.h"
#include "nodes/compressed_skip_scan/planner.h"
#include "nodes/compressed_skip_scan/exec.h"
#include "import/planner.h"
#include "guc.h"
#include "custom_type_cache.h"

/************************************
 * CompressedSkipScan Plan Creation *
 ***********************************/

static CustomScanMethods compressed_skip_scan_plan_methods = {
	.CustomName = "CompressedSkipScan",
	.CreateCustomScanState = compressed_skip_scan_state_create,
};

void
_compressed_skip_scan_init(void)
{
	TryRegisterCustomScanMethods(&compressed_skip_scan_plan_methods);
}

/*
 * Given the scan targetlist and the bitmapset of the needed columns, determine
 * which scan columns become which decompressed columns (fill decompression_map).
 */
static void
build_decompression_map(CompressedSkipScanPath *path, List *scan_tlist,
						Bitmapset *chunk_attrs_needed)
{
	/*
	 * Track which normal and metadata columns we were able to find in the
	 * targetlist.
	 */
	bool missing_count = true;
	bool missing_sequence = path->needs_sequence_num;
	Bitmapset *chunk_attrs_found = NULL;

	/*
	 * FIXME this way to determine which columns are used is actually wrong, see
	 * https://github.com/timescale/timescaledb/issues/4195#issuecomment-1104238863
	 * Left as is for now, because changing it uncovers a whole new story with
	 * ctid.
	 */
	check_for_system_columns(path->info->ht_rte->selectedCols);

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

		Assert(compressed_attno != InvalidAttrNumber);

		compressed_attno_to_compression_info[compressed_attno] = fd;
	}

	/*
	 * Go over the scan targetlist and determine to which output column each
	 * scan column goes.
	 */
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

		Assert(compressed_attno != InvalidAttrNumber);
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
	}

	/*
	 * Check that we have found all the needed columns in the scan targetlist.
	 * We can't conveniently check that we have all columns for all-row vars, so
	 * skip attno 0 in this check.
	 */
	Bitmapset *attrs_not_found = bms_difference(chunk_attrs_needed, chunk_attrs_found);
	int bit = bms_next_member(attrs_not_found, 0 - FirstLowInvalidHeapAttributeNumber);
	if (bit >= 0)
	{
		elog(ERROR,
			 "column '%s' (%d) not found in the scan targetlist for compressed chunk '%s'",
			 get_attname(path->info->chunk_rte->relid,
						 bit + FirstLowInvalidHeapAttributeNumber,
						 /* missing_ok = */ true),
			 bit + FirstLowInvalidHeapAttributeNumber,
			 get_rel_name(path->info->compressed_rte->relid));
	}

	if (missing_count)
	{
		elog(ERROR, "the count column was not found in the compressed scan targetlist");
	}

	if (missing_sequence)
	{
		elog(ERROR, "the sequence column was not found in the compressed scan targetlist");
	}
}

Plan *
compressed_skip_scan_plan_create(PlannerInfo *root, RelOptInfo *rel, CustomPath *path,
								 List *decompressed_tlist, List *clauses, List *custom_plans)
{
	CompressedSkipScanPath *csspath = (CompressedSkipScanPath *) path;
	CustomScan *compressed_skip_scan_plan = makeNode(CustomScan);
	Scan *compressed_scan = linitial(custom_plans);
	Path *compressed_path = linitial(path->custom_paths);
	List *settings;
	ListCell *lc;

	Assert(list_length(custom_plans) == 1);
	Assert(list_length(path->custom_paths) == 1);

	compressed_skip_scan_plan->flags = path->flags;
	compressed_skip_scan_plan->methods = &compressed_skip_scan_plan_methods;
	compressed_skip_scan_plan->scan.scanrelid = csspath->info->chunk_rel->relid;

	/* output target list */
	compressed_skip_scan_plan->scan.plan.targetlist = decompressed_tlist;
	/* input target list */
	compressed_skip_scan_plan->custom_scan_tlist = NIL;

	foreach (lc, clauses)
	{
		RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc);
		compressed_skip_scan_plan->scan.plan.qual =
			lappend(compressed_skip_scan_plan->scan.plan.qual, rinfo->clause);
	}

	compressed_skip_scan_plan->scan.plan.qual =
		(List *) replace_compressed_vars((Node *) compressed_skip_scan_plan->scan.plan.qual,
										 csspath->info);

	/*
	 * Try to use a physical tlist if possible. There's no reason to do the
	 * extra work of projecting the result of compressed chunk scan, because
	 * DecompressChunk can choose only the needed columns itself.
	 * Note that Postgres uses the CP_EXACT_TLIST option when planning the child
	 * paths of the Custom path, so we won't automatically get a physical tlist
	 * here.
	 */
	if (compressed_path->pathtype == T_IndexOnlyScan)
	{
		compressed_scan->plan.targetlist = ((IndexPath *) compressed_path)->indexinfo->indextlist;
	}
	else
	{
		List *physical_tlist = NULL;
		physical_tlist = compressed_scan->plan.targetlist;
		CustomScan *temp = (CustomScan *) compressed_scan;
		temp->custom_scan_tlist = list_copy(physical_tlist);
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
	pull_varattnos((Node *) compressed_skip_scan_plan->scan.plan.qual,
				   csspath->info->chunk_rel->relid,
				   &chunk_attrs_needed);
	pull_varattnos((Node *) csspath->cpath.path.pathtarget->exprs,
				   csspath->info->chunk_rel->relid,
				   &chunk_attrs_needed);

	/*
	 * Determine which compressed colum goes to which output column.
	 */
	build_decompression_map(csspath, compressed_scan->plan.targetlist, chunk_attrs_needed);

	/* Build heap sort info for sorted_merge_append */
	List *sort_options = NIL;

	/*
	 * Add a sort if the compressed scan is not ordered appropriately.
	 */
	if (!pathkeys_contained_in(csspath->compressed_pathkeys, compressed_path->pathkeys))
	{
		List *compressed_pks = csspath->compressed_pathkeys;
		Sort *sort = ts_make_sort_from_pathkeys((Plan *) compressed_scan,
												compressed_pks,
												bms_make_singleton(compressed_scan->scanrelid));
		compressed_skip_scan_plan->custom_plans = list_make1(sort);
	}
	else
	{
		compressed_skip_scan_plan->custom_plans = custom_plans;
	}

	Assert(list_length(custom_plans) == 1);

	settings = list_make3_int(csspath->info->hypertable_id,
							  csspath->info->chunk_rte->relid,
							  csspath->reverse);

	compressed_skip_scan_plan->custom_private =
		list_make3(settings, csspath->decompression_map, sort_options);

	return &compressed_skip_scan_plan->scan.plan;
}

/************************************
 * CompressedSkipScanPath Creation  *
 ***********************************/

static CustomPathMethods compressed_skip_scan_path_methods = {
	.CustomName = "CompressedSkipScanPath",
	.PlanCustomPath = compressed_skip_scan_plan_create,
};

CompressedSkipScanPath *
compressed_skip_scan_path_create(PlannerInfo *root, CustomPath *custom_path, CompressionInfo *info,
								 List *compressed_pathkeys, bool needs_seq_num, bool reverse,
								 List *subpaths)
{
	ListCell *lc;
	double total_cost = 0, rows = 0;
	CompressedSkipScanPath *csspath = palloc(sizeof(CompressedSkipScanPath));
	memcpy(&csspath->cpath, custom_path, sizeof(CustomPath));
	csspath->cpath.custom_paths = subpaths;
	csspath->cpath.methods = &compressed_skip_scan_path_methods;
	csspath->info = info;
	csspath->decompression_map = NIL;
	csspath->compressed_pathkeys = compressed_pathkeys;
	csspath->needs_sequence_num = needs_seq_num;
	csspath->reverse = reverse;

	foreach (lc, subpaths)
	{
		Path *child = lfirst(lc);
		total_cost += child->total_cost;
		rows += child->rows;
	}
	csspath->cpath.path.total_cost = total_cost;
	csspath->cpath.path.rows = rows;
	return csspath;
}
