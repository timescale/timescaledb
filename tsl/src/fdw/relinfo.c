/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <access/htup_details.h>
#include <nodes/relation.h>
#include <parser/parsetree.h>
#include <optimizer/var.h>
#include <commands/extension.h>
#include <commands/defrem.h>
#include <utils/hsearch.h>
#include <utils/builtins.h>
#include <utils/lsyscache.h>
#include <miscadmin.h>

#include <extension_constants.h>
#include <planner.h>

#include "option.h"
#include "deparse.h"
#include "relinfo.h"
#include "estimate.h"

/* Default CPU cost to start up a foreign query. */
#define DEFAULT_FDW_STARTUP_COST 100.0

/* Default CPU cost to process 1 row (above and beyond cpu_tuple_cost). */
#define DEFAULT_FDW_TUPLE_COST 0.01

#define DEFAULT_FDW_FETCH_SIZE 100

/*
 * Parse options from foreign server and apply them to fpinfo.
 *
 * New options might also require tweaking merge_fdw_options().
 */
static void
apply_server_options(TsFdwRelInfo *fpinfo)
{
	ListCell *lc;

	foreach (lc, fpinfo->server->options)
	{
		DefElem *def = (DefElem *) lfirst(lc);

		if (strcmp(def->defname, "use_remote_estimate") == 0)
			fpinfo->use_remote_estimate = defGetBoolean(def);
		else if (strcmp(def->defname, "fdw_startup_cost") == 0)
			fpinfo->fdw_startup_cost = strtod(defGetString(def), NULL);
		else if (strcmp(def->defname, "fdw_tuple_cost") == 0)
			fpinfo->fdw_tuple_cost = strtod(defGetString(def), NULL);
		else if (strcmp(def->defname, "extensions") == 0)
			fpinfo->shippable_extensions =
				list_concat(fpinfo->shippable_extensions,
							option_extract_extension_list(defGetString(def), false));
		else if (strcmp(def->defname, "fetch_size") == 0)
			fpinfo->fetch_size = strtol(defGetString(def), NULL, 10);
	}
}

TsFdwRelInfo *
fdw_relinfo_get(RelOptInfo *rel)
{
	TimescaleDBPrivate *rel_private = rel->fdw_private;

	Assert(rel_private != NULL);
	Assert(rel_private->fdw_relation_info != NULL);

	return (TsFdwRelInfo *) rel_private->fdw_relation_info;
}

TsFdwRelInfo *
fdw_relinfo_alloc(RelOptInfo *rel, TsFdwRelInfoType reltype)
{
	TimescaleDBPrivate *rel_private;
	TsFdwRelInfo *fpinfo;

	if (NULL == rel->fdw_private)
		rel->fdw_private = palloc0(sizeof(*rel_private));

	rel_private = rel->fdw_private;

	fpinfo = (TsFdwRelInfo *) palloc0(sizeof(*fpinfo));
	rel_private->fdw_relation_info = (void *) fpinfo;
	fpinfo->type = reltype;

	return fpinfo;
}

TsFdwRelInfo *
fdw_relinfo_create(PlannerInfo *root, RelOptInfo *rel, Oid server_oid, Oid local_table_id,
				   TsFdwRelInfoType type)
{
	TsFdwRelInfo *fpinfo;
	ListCell *lc;
	RangeTblEntry *rte = planner_rt_fetch(rel->relid, root);
	const char *namespace;
	const char *relname;
	const char *refname;

	/*
	 * We use TsFdwRelInfo to pass various information to subsequent
	 * functions.
	 */
	fpinfo = fdw_relinfo_alloc(rel, type);

	if (type == TS_FDW_RELINFO_HYPERTABLE)
	{
		/* nothing more to do for hypertables */
		Assert(!OidIsValid(server_oid));

		return fpinfo;
	}
	/* Base foreign tables need to be pushed down always. */
	fpinfo->pushdown_safe = true;

	/* Look up foreign-table catalog info. */
	fpinfo->server = GetForeignServer(server_oid);

	/*
	 * Extract user-settable option values.  Note that per-table setting of
	 * use_remote_estimate overrides per-server setting.
	 */
	fpinfo->use_remote_estimate = false;
	fpinfo->fdw_startup_cost = DEFAULT_FDW_STARTUP_COST;
	fpinfo->fdw_tuple_cost = DEFAULT_FDW_TUPLE_COST;
	fpinfo->shippable_extensions = list_make1_oid(get_extension_oid(EXTENSION_NAME, true));
	fpinfo->fetch_size = DEFAULT_FDW_FETCH_SIZE;

	apply_server_options(fpinfo);

	/*
	 * If the table or the data node is configured to use remote estimates,
	 * identify which user to do remote access as during planning.  This
	 * should match what ExecCheckRTEPerms() does.  If we fail due to lack of
	 * permissions, the query would have failed at runtime anyway.
	 */
	if (fpinfo->use_remote_estimate)
	{
		Oid userid = rte->checkAsUser ? rte->checkAsUser : GetUserId();

		fpinfo->user = GetUserMapping(userid, fpinfo->server->serverid);
	}
	else
		fpinfo->user = NULL;

	/*
	 * Identify which baserestrictinfo clauses can be sent to the data
	 * node and which can't.
	 */
	classify_conditions(root,
						rel,
						rel->baserestrictinfo,
						&fpinfo->remote_conds,
						&fpinfo->local_conds);

	/*
	 * Identify which attributes will need to be retrieved from the data
	 * node.  These include all attrs needed for joins or final output, plus
	 * all attrs used in the local_conds.  (Note: if we end up using a
	 * parameterized scan, it's possible that some of the join clauses will be
	 * sent to the remote and thus we wouldn't really need to retrieve the
	 * columns used in them.  Doesn't seem worth detecting that case though.)
	 */
	fpinfo->attrs_used = NULL;
	pull_varattnos((Node *) rel->reltarget->exprs, rel->relid, &fpinfo->attrs_used);
	foreach (lc, fpinfo->local_conds)
	{
		RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc);

		pull_varattnos((Node *) rinfo->clause, rel->relid, &fpinfo->attrs_used);
	}

	/*
	 * Compute the selectivity and cost of the local_conds, so we don't have
	 * to do it over again for each path.  The best we can do for these
	 * conditions is to estimate selectivity on the basis of local statistics.
	 */
	fpinfo->local_conds_sel =
		clauselist_selectivity(root, fpinfo->local_conds, rel->relid, JOIN_INNER, NULL);

	cost_qual_eval(&fpinfo->local_conds_cost, fpinfo->local_conds, root);

	/*
	 * Set cached relation costs to some negative value, so that we can detect
	 * when they are set to some sensible costs during one (usually the first)
	 * of the calls to fdw_estimate_path_cost_size().
	 */
	fpinfo->rel_startup_cost = -1;
	fpinfo->rel_total_cost = -1;
	fpinfo->rel_retrieved_rows = -1;

	/*
	 * If the table or the data node is configured to use remote estimates,
	 * connect to the data node and execute EXPLAIN to estimate the
	 * number of rows selected by the restriction clauses, as well as the
	 * average row width.  Otherwise, estimate using whatever statistics we
	 * have locally, in a way similar to ordinary tables.
	 */
	if (fpinfo->use_remote_estimate)
	{
		/*
		 * Get cost/size estimates with help of data node.  Save the
		 * values in fpinfo so we don't need to do it again to generate the
		 * basic foreign path.
		 */
		fdw_estimate_path_cost_size(root,
									rel,
									NIL,
									NIL,
									&fpinfo->rows,
									&fpinfo->width,
									&fpinfo->startup_cost,
									&fpinfo->total_cost);

		/* Report estimated rel size to planner. */
		rel->rows = fpinfo->rows;
		rel->reltarget->width = fpinfo->width;
	}
	else
	{
		/*
		 * If the foreign table has never been ANALYZEd, it will have relpages
		 * and reltuples equal to zero, which most likely has nothing to do
		 * with reality.  We can't do a whole lot about that if we're not
		 * allowed to consult the data node, but we can use a hack similar
		 * to plancat.c's treatment of empty relations: use a minimum size
		 * estimate of 10 pages, and divide by the column-datatype-based width
		 * estimate to get the corresponding number of tuples.
		 */
		if (rel->pages == 0 && rel->tuples == 0)
		{
			rel->pages = 10;
			rel->tuples = (10 * BLCKSZ) / (rel->reltarget->width + MAXALIGN(SizeofHeapTupleHeader));
		}
		/* Estimate rel size as best we can with local statistics. */
		set_baserel_size_estimates(root, rel);

		/* Fill in basically-bogus cost estimates for use later. */
		fdw_estimate_path_cost_size(root,
									rel,
									NIL,
									NIL,
									&fpinfo->rows,
									&fpinfo->width,
									&fpinfo->startup_cost,
									&fpinfo->total_cost);
	}

	/*
	 * Set the name of relation in fpinfo, while we are constructing it here.
	 * It will be used to build the string describing the join relation in
	 * EXPLAIN output. We can't know whether VERBOSE option is specified or
	 * not, so always schema-qualify the foreign table name.
	 */
	fpinfo->relation_name = makeStringInfo();
	namespace = get_namespace_name(get_rel_namespace(local_table_id));
	relname = get_rel_name(local_table_id);
	refname = rte->eref->aliasname;
	appendStringInfo(fpinfo->relation_name,
					 "%s.%s",
					 quote_identifier(namespace),
					 quote_identifier(relname));
	if (*refname && strcmp(refname, relname) != 0)
		appendStringInfo(fpinfo->relation_name, " %s", quote_identifier(rte->eref->aliasname));

	/* No outer and inner relations. */
	fpinfo->make_outerrel_subquery = false;
	fpinfo->make_innerrel_subquery = false;
	fpinfo->lower_subquery_rels = NULL;
	/* Set the relation index. */
	fpinfo->relation_index = rel->relid;

	return fpinfo;
}
