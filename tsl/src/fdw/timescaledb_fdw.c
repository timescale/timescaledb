/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
/*
 * No op Foreign Data Wrapper implementation to be used as a mock object
 * when testing or a starting point for implementing/testing real implementation.
 *
 */
#include <postgres.h>
#include <fmgr.h>
#include <catalog/pg_type.h>
#include <foreign/fdwapi.h>
#include <foreign/foreign.h>
#include <nodes/relation.h>
#include <nodes/makefuncs.h>
#include <nodes/nodeFuncs.h>
#include <parser/parsetree.h>
#include <optimizer/pathnode.h>
#include <optimizer/restrictinfo.h>
#include <optimizer/planmain.h>
#include <optimizer/var.h>
#include <optimizer/cost.h>
#include <optimizer/clauses.h>
#include <optimizer/tlist.h>
#include <optimizer/paths.h>
#include <access/htup_details.h>
#include <access/sysattr.h>
#include <access/reloptions.h>
#include <executor/executor.h>
#include <commands/explain.h>
#include <utils/builtins.h>
#include <utils/lsyscache.h>
#include <utils/rel.h>
#include <utils/selfuncs.h>
#include <commands/defrem.h>
#include <commands/explain.h>
#include <miscadmin.h>

#include <export.h>
#include <hypertable_server.h>
#include <hypertable_cache.h>
#include <chunk_server.h>
#include <chunk_insert_state.h>
#include <server_dispatch.h>
#include <remote/dist_txn.h>
#include <remote/async.h>
#include <remote/stmt_params.h>
#include <compat.h>

#include <planner.h>
#include "utils.h"
#include "timescaledb_fdw.h"
#include "deparse.h"
#include "option.h"
#include "server_chunk_assignment.h"
#include "remote/data_format.h"
#include "guc.h"

/* Default CPU cost to start up a foreign query. */
#define DEFAULT_FDW_STARTUP_COST 100.0

/* Default CPU cost to process 1 row (above and beyond cpu_tuple_cost). */
#define DEFAULT_FDW_TUPLE_COST 0.01

/* If no remote estimates, assume a sort costs 20% extra */
#define DEFAULT_FDW_SORT_MULTIPLIER 1.2

/*
 * Indexes of FDW-private information stored in fdw_private lists.
 *
 * These items are indexed with the enum FdwScanPrivateIndex, so an item
 * can be fetched with list_nth().  For example, to get the SELECT statement:
 *		sql = strVal(list_nth(fdw_private, FdwScanPrivateSelectSql));
 */
enum FdwScanPrivateIndex
{
	/* SQL statement to execute remotely (as a String node) */
	FdwScanPrivateSelectSql,
	/* Integer list of attribute numbers retrieved by the SELECT */
	FdwScanPrivateRetrievedAttrs,
	/* Integer representing the desired fetch_size */
	FdwScanPrivateFetchSize,

	/* Integer for the OID of the foreign server, used by EXPLAIN */
	FdwScanPrivateServerId,
	/* OID list of chunk oids, used by EXPLAIN */
	FdwScanPrivateChunkOids,
	/*
	 * String describing join i.e. names of relations being joined and types
	 * of join, added when the scan is join
	 */
	FdwScanPrivateRelations
};

/*
 * This enum describes what's kept in the fdw_private list for a ModifyTable
 * node referencing a timescaledb_fdw foreign table.  We store:
 *
 * 1) INSERT/UPDATE/DELETE statement text to be sent to the remote server
 * 2) Integer list of target attribute numbers for INSERT/UPDATE
 *	  (NIL for a DELETE)
 * 3) Boolean flag showing if the remote query has a RETURNING clause
 * 4) Integer list of attribute numbers retrieved by RETURNING, if any
 */
enum FdwModifyPrivateIndex
{
	/* SQL statement to execute remotely (as a String node) */
	FdwModifyPrivateUpdateSql,
	/* Integer list of target attribute numbers for INSERT/UPDATE */
	FdwModifyPrivateTargetAttnums,
	/* has-returning flag (as an integer Value node) */
	FdwModifyPrivateHasReturning,
	/* Integer list of attribute numbers retrieved by RETURNING */
	FdwModifyPrivateRetrievedAttrs,
	/* The servers for the current chunk */
	FdwModifyPrivateServers,
	/* Insert state for the current chunk */
	FdwModifyPrivateChunkInsertState,
};

/*
 * Execution state of a foreign scan using timescaledb_fdw.
 */
typedef struct TsFdwScanState
{
	Relation rel;						  /* relcache entry for the foreign table. NULL
										   * for a foreign join scan. */
	TupleDesc tupdesc;					  /* tuple descriptor of scan */
	AttConvInMetadata *att_conv_metadata; /* attribute datatype conversion metadata */

	/* extracted fdw_private data */
	char *query;		   /* text of SELECT command */
	List *retrieved_attrs; /* list of retrieved attribute numbers */

	/* for remote query execution */
	TSConnection *conn;			/* connection for the scan */
	unsigned int cursor_number; /* quasi-unique ID for my cursor */
	bool cursor_exists;			/* have we created the cursor? */
	int num_params;				/* number of parameters passed to query */
	FmgrInfo *param_flinfo;		/* output conversion functions for them */
	List *param_exprs;			/* executable expressions for param values */
	const char **param_values;  /* textual values of query parameters */

	/* for storing result tuples */
	HeapTuple *tuples; /* array of currently-retrieved tuples */
	int num_tuples;	/* # of tuples in array */
	int next_tuple;	/* index of next one to return */

	/* batch-level state, for optimizing rewinds and avoiding useless fetch */
	int fetch_ct_2;   /* Min(# of fetches done, 2) */
	bool eof_reached; /* true if last fetch reached EOF */

	/* working memory contexts */
	MemoryContext batch_cxt; /* context holding current batch of tuples */
	MemoryContext temp_cxt;  /* context for per-tuple temporary data */

	int fetch_size; /* number of tuples per fetch */
	int row_counter;
} TsFdwScanState;

typedef struct TsFdwServerState
{
	Oid serverid;
	UserMapping *user;
	/* for remote query execution */
	TSConnection *conn;   /* connection for the scan */
	PreparedStmt *p_stmt; /* prepared statement handle, if created */
} TsFdwServerState;

/*
 * Execution state of a foreign insert/update/delete operation.
 */
typedef struct TsFdwModifyState
{
	Relation rel;						  /* relcache entry for the foreign table */
	AttConvInMetadata *att_conv_metadata; /* attribute datatype conversion metadata for converting
											 result to tuples */

	/* extracted fdw_private data */
	char *query;		   /* text of INSERT/UPDATE/DELETE command */
	List *target_attrs;	/* list of target attribute numbers */
	bool has_returning;	/* is there a RETURNING clause? */
	List *retrieved_attrs; /* attr numbers retrieved by RETURNING */

	AttrNumber ctid_attno; /* attnum of input resjunk ctid column */

	/* working memory context */
	MemoryContext temp_cxt; /* context for per-tuple temporary data */

	bool prepared;
	int num_servers;
	StmtParams *stmt_params; /* prepared statement paremeters */
	TsFdwServerState servers[FLEXIBLE_ARRAY_MEMBER];
} TsFdwModifyState;

#define TS_FDW_MODIFY_STATE_SIZE(num_servers)                                                      \
	(sizeof(TsFdwModifyState) + (sizeof(TsFdwServerState) * num_servers))

static FdwRoutine timescaledb_fdw_routine;

/*
 * Estimate costs of executing a SQL statement remotely.
 * The given "sql" must be an EXPLAIN command.
 */
static void
get_remote_estimate(const char *sql, TSConnection *conn, double *rows, int *width,
					Cost *startup_cost, Cost *total_cost)
{
	AsyncResponseResult *volatile rsp = NULL;

	/* PGresult must be released before leaving this function. */
	PG_TRY();
	{
		AsyncRequest *req;
		PGresult *res;
		char *line;
		char *p;
		int n;

		/*
		 * Execute EXPLAIN remotely.
		 */
		req = async_request_send(conn, sql);
		rsp = async_request_wait_any_result(req);
		res = async_response_result_get_pg_result(rsp);

		if (PQresultStatus(res) != PGRES_TUPLES_OK)
			async_response_report_error((AsyncResponse *) rsp, ERROR);

		/*
		 * Extract cost numbers for topmost plan node.  Note we search for a
		 * left paren from the end of the line to avoid being confused by
		 * other uses of parentheses.
		 */
		line = PQgetvalue(res, 0, 0);
		p = strrchr(line, '(');
		if (p == NULL)
			elog(ERROR, "could not interpret EXPLAIN output: \"%s\"", line);
		n = sscanf(p, "(cost=%lf..%lf rows=%lf width=%d)", startup_cost, total_cost, rows, width);
		if (n != 4)
			elog(ERROR, "could not interpret EXPLAIN output: \"%s\"", line);

		async_response_result_close(rsp);
	}
	PG_CATCH();
	{
		if (NULL != rsp)
			async_response_result_close(rsp);

		PG_RE_THROW();
	}
	PG_END_TRY();
}

/*
 * estimate_path_cost_size
 *		Get cost and size estimates for a foreign scan on given foreign
 *		relation either a base relation or an upper relation containing
 *		foreign relations.
 *
 * param_join_conds are the parameterization clauses with outer relations.
 * pathkeys specify the expected sort order if any for given path being costed.
 *
 * The function returns the cost and size estimates in p_row, p_width,
 * p_startup_cost and p_total_cost variables.
 */
static void
estimate_path_cost_size(PlannerInfo *root, RelOptInfo *foreignrel, List *param_join_conds,
						List *pathkeys, double *p_rows, int *p_width, Cost *p_startup_cost,
						Cost *p_total_cost)
{
	TsFdwRelationInfo *fpinfo = fdw_relation_info_get(foreignrel);
	double rows = 0;
	double retrieved_rows;
	int width = 0;
	Cost startup_cost = 0;
	Cost total_cost = 0;
	Cost cpu_per_tuple;

	/*
	 * If the table or the server is configured to use remote estimates,
	 * connect to the foreign server and execute EXPLAIN to estimate the
	 * number of rows selected by the restriction+join clauses.  Otherwise,
	 * estimate rows using whatever statistics we have locally, in a way
	 * similar to ordinary tables.
	 */
	if (fpinfo->use_remote_estimate)
	{
		List *remote_param_join_conds;
		List *local_param_join_conds;
		StringInfoData sql;
		TSConnection *conn;
		Selectivity local_sel;
		QualCost local_cost;
		List *fdw_scan_tlist = NIL;
		List *remote_conds;

		/* Required only to be passed to deparseSelectStmtForRel */
		List *retrieved_attrs;

		/*
		 * param_join_conds might contain both clauses that are safe to send
		 * across, and clauses that aren't.
		 */
		classifyConditions(root,
						   foreignrel,
						   param_join_conds,
						   &remote_param_join_conds,
						   &local_param_join_conds);

		if (IS_JOIN_REL(foreignrel))
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("foreign joins are not supported")));

		/* Build the list of columns to be fetched from the foreign server. */
		if (IS_UPPER_REL(foreignrel))
			fdw_scan_tlist = build_tlist_to_deparse(foreignrel);
		else
			fdw_scan_tlist = NIL;

		/*
		 * The complete list of remote conditions includes everything from
		 * baserestrictinfo plus any extra join_conds relevant to this
		 * particular path.
		 */
		remote_conds = list_concat(list_copy(remote_param_join_conds), fpinfo->remote_conds);

		/*
		 * Construct EXPLAIN query including the desired SELECT, FROM, and
		 * WHERE clauses. Params and other-relation Vars are replaced by dummy
		 * values, so don't request params_list.
		 */
		initStringInfo(&sql);
		appendStringInfoString(&sql, "EXPLAIN ");
		deparseSelectStmtForRel(&sql,
								root,
								foreignrel,
								fdw_scan_tlist,
								remote_conds,
								pathkeys,
								false,
								&retrieved_attrs,
								NULL,
								fpinfo->sca);

		/* Get the remote estimate */
		conn = remote_dist_txn_get_connection(fpinfo->user, REMOTE_TXN_NO_PREP_STMT);
		get_remote_estimate(sql.data, conn, &rows, &width, &startup_cost, &total_cost);

		retrieved_rows = rows;

		/* Factor in the selectivity of the locally-checked quals */
		local_sel = clauselist_selectivity(root,
										   local_param_join_conds,
										   foreignrel->relid,
										   JOIN_INNER,
										   NULL);
		local_sel *= fpinfo->local_conds_sel;

		rows = clamp_row_est(rows * local_sel);

		/* Add in the eval cost of the locally-checked quals */
		startup_cost += fpinfo->local_conds_cost.startup;
		total_cost += fpinfo->local_conds_cost.per_tuple * retrieved_rows;
		cost_qual_eval(&local_cost, local_param_join_conds, root);
		startup_cost += local_cost.startup;
		total_cost += local_cost.per_tuple * retrieved_rows;
	}
	else
	{
		Cost run_cost = 0;

		/*
		 * We don't support join conditions in this mode (hence, no
		 * parameterized paths can be made).
		 */
		Assert(param_join_conds == NIL);

		/*
		 * Use rows/width estimates made by set_baserel_size_estimates() for
		 * base foreign relations.
		 */
		rows = foreignrel->rows;
		width = foreignrel->reltarget->width;

		/* Back into an estimate of the number of retrieved rows. */
		retrieved_rows = clamp_row_est(rows / fpinfo->local_conds_sel);

		/*
		 * We will come here again and again with different set of pathkeys
		 * that caller wants to cost. We don't need to calculate the cost of
		 * bare scan each time. Instead, use the costs if we have cached them
		 * already.
		 */
		if (fpinfo->rel_startup_cost > 0 && fpinfo->rel_total_cost > 0)
		{
			startup_cost = fpinfo->rel_startup_cost;
			run_cost = fpinfo->rel_total_cost - fpinfo->rel_startup_cost;
		}
		else if (IS_JOIN_REL(foreignrel))
		{
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("foreign joins are not supported")));
		}
		else if (IS_UPPER_REL(foreignrel))
		{
			TsFdwRelationInfo *ofpinfo;
			PathTarget *ptarget = foreignrel->reltarget;
			AggClauseCosts aggcosts;
			double input_rows;
			int numGroupCols;
			double numGroups = 1;

			/* Make sure the core code set the pathtarget. */
			Assert(ptarget != NULL);

			/*
			 * This cost model is mixture of costing done for sorted and
			 * hashed aggregates in cost_agg().  We are not sure which
			 * strategy will be considered at remote side, thus for
			 * simplicity, we put all startup related costs in startup_cost
			 * and all finalization and run cost are added in total_cost.
			 *
			 * Also, core does not care about costing HAVING expressions and
			 * adding that to the costs.  So similarly, here too we are not
			 * considering remote and local conditions for costing.
			 */

			ofpinfo = fdw_relation_info_get(fpinfo->outerrel);

			/* Get rows and width from input rel */
			input_rows = ofpinfo->rows;
			width = ofpinfo->width;

			/* Collect statistics about aggregates for estimating costs. */
			MemSet(&aggcosts, 0, sizeof(AggClauseCosts));
			if (root->parse->hasAggs)
			{
				get_agg_clause_costs(root,
									 (Node *) fpinfo->grouped_tlist,
									 AGGSPLIT_SIMPLE,
									 &aggcosts);

				/*
				 * The cost of aggregates in the HAVING qual will be the same
				 * for each child as it is for the parent, so there's no need
				 * to use a translated version of havingQual.
				 */
				get_agg_clause_costs(root,
									 (Node *) root->parse->havingQual,
									 AGGSPLIT_SIMPLE,
									 &aggcosts);
			}

			/* Get number of grouping columns and possible number of groups */
			numGroupCols = list_length(root->parse->groupClause);
			numGroups = estimate_num_groups(root,
											get_sortgrouplist_exprs(root->parse->groupClause,
																	fpinfo->grouped_tlist),
											input_rows,
											NULL);

			/*
			 * Number of rows expected from foreign server will be same as
			 * that of number of groups.
			 */
			rows = retrieved_rows = numGroups;

			/*-----
			 * Startup cost includes:
			 *	  1. Startup cost for underneath input * relation
			 *	  2. Cost of performing aggregation, per cost_agg()
			 *	  3. Startup cost for PathTarget eval
			 *-----
			 */
			startup_cost = ofpinfo->rel_startup_cost;
			startup_cost += aggcosts.transCost.startup;
			startup_cost += aggcosts.transCost.per_tuple * input_rows;
			startup_cost += (cpu_operator_cost * numGroupCols) * input_rows;
			startup_cost += ptarget->cost.startup;

			/*-----
			 * Run time cost includes:
			 *	  1. Run time cost of underneath input relation
			 *	  2. Run time cost of performing aggregation, per cost_agg()
			 *	  3. PathTarget eval cost for each output row
			 *-----
			 */
			run_cost = ofpinfo->rel_total_cost - ofpinfo->rel_startup_cost;
			run_cost += aggcosts.finalCost * numGroups;
			run_cost += cpu_tuple_cost * numGroups;
			run_cost += ptarget->cost.per_tuple * numGroups;
		}
		else
		{
			/* Clamp retrieved rows estimates to at most foreignrel->tuples. */
			retrieved_rows = Min(retrieved_rows, foreignrel->tuples);

			/*
			 * Cost as though this were a seqscan, which is pessimistic.  We
			 * effectively imagine the local_conds are being evaluated
			 * remotely, too.
			 */
			startup_cost = 0;
			run_cost = 0;
			run_cost += seq_page_cost * foreignrel->pages;

			startup_cost += foreignrel->baserestrictcost.startup;
			cpu_per_tuple = cpu_tuple_cost + foreignrel->baserestrictcost.per_tuple;
			run_cost += cpu_per_tuple * foreignrel->tuples;
		}

		/*
		 * Without remote estimates, we have no real way to estimate the cost
		 * of generating sorted output.  It could be free if the query plan
		 * the remote side would have chosen generates properly-sorted output
		 * anyway, but in most cases it will cost something.  Estimate a value
		 * high enough that we won't pick the sorted path when the ordering
		 * isn't locally useful, but low enough that we'll err on the side of
		 * pushing down the ORDER BY clause when it's useful to do so.
		 */
		if (pathkeys != NIL)
		{
			startup_cost *= DEFAULT_FDW_SORT_MULTIPLIER;
			run_cost *= DEFAULT_FDW_SORT_MULTIPLIER;
		}

		total_cost = startup_cost + run_cost;
	}

	/*
	 * Cache the costs for scans without any pathkeys or parameterization
	 * before adding the costs for transferring data from the foreign server.
	 * These costs are useful for costing the join between this relation and
	 * another foreign relation or to calculate the costs of paths with
	 * pathkeys for this relation, when the costs can not be obtained from the
	 * foreign server. This function will be called at least once for every
	 * foreign relation without pathkeys and parameterization.
	 */
	if (pathkeys == NIL && param_join_conds == NIL)
	{
		fpinfo->rel_startup_cost = startup_cost;
		fpinfo->rel_total_cost = total_cost;
	}

	/*
	 * Add some additional cost factors to account for connection overhead
	 * (fdw_startup_cost), transferring data across the network
	 * (fdw_tuple_cost per retrieved row), and local manipulation of the data
	 * (cpu_tuple_cost per retrieved row).
	 */
	startup_cost += fpinfo->fdw_startup_cost;
	total_cost += fpinfo->fdw_startup_cost;
	total_cost += fpinfo->fdw_tuple_cost * retrieved_rows;
	total_cost += cpu_tuple_cost * retrieved_rows;

	/* Return results. */
	*p_rows = rows;
	*p_width = width;
	*p_startup_cost = startup_cost;
	*p_total_cost = total_cost;
}

/*
 * Parse options from foreign server and apply them to fpinfo.
 *
 * New options might also require tweaking merge_fdw_options().
 */
static void
apply_server_options(TsFdwRelationInfo *fpinfo)
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
			fpinfo->shippable_extensions = option_extract_extension_list(defGetString(def), false);
		else if (strcmp(def->defname, "fetch_size") == 0)
			fpinfo->fetch_size = strtol(defGetString(def), NULL, 10);
	}
}

/*
 * Parse options from foreign table and apply them to fpinfo.
 *
 * New options might also require tweaking merge_fdw_options().
 */
static void
apply_table_options(ForeignTable *table, TsFdwRelationInfo *fpinfo)
{
	ListCell *lc;

	foreach (lc, table->options)
	{
		DefElem *def = (DefElem *) lfirst(lc);

		if (strcmp(def->defname, "use_remote_estimate") == 0)
			fpinfo->use_remote_estimate = defGetBoolean(def);
		else if (strcmp(def->defname, "fetch_size") == 0)
			fpinfo->fetch_size = strtol(defGetString(def), NULL, 10);
	}
}

TsFdwRelationInfo *
fdw_relation_info_get(RelOptInfo *baserel)
{
	TimescaleDBPrivate *rel_private = baserel->fdw_private;

	Assert(rel_private != NULL);
	Assert(rel_private->fdw_relation_info != NULL);

	return (TsFdwRelationInfo *) rel_private->fdw_relation_info;
}

static void
fdw_relation_info_create(PlannerInfo *root, RelOptInfo *baserel, Oid server_oid, Oid local_table_id,
						 TsFdwRelationInfoType type)
{
	TimescaleDBPrivate *rel_private = baserel->fdw_private;
	TsFdwRelationInfo *fpinfo;
	ListCell *lc;
	RangeTblEntry *rte = planner_rt_fetch(baserel->relid, root);
	const char *namespace;
	const char *relname;
	const char *refname;

	/*
	 * We use TsFdwRelationInfo to pass various information to subsequent
	 * functions.
	 */
	if (baserel->fdw_private == NULL)
		baserel->fdw_private = palloc0(sizeof(*rel_private));
	rel_private = baserel->fdw_private;

	fpinfo = (TsFdwRelationInfo *) palloc0(sizeof(*fpinfo));
	rel_private->fdw_relation_info = (void *) fpinfo;

	fpinfo->type = type;

	if (type == TS_FDW_RELATION_INFO_HYPERTABLE)
	{
		/* nothing more to do for hypertables */
		Assert(!OidIsValid(server_oid));
		return;
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
	fpinfo->shippable_extensions = NIL;
	fpinfo->fetch_size = 100;

	apply_server_options(fpinfo);

	/*
	 * If the table or the server is configured to use remote estimates,
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
	 * Identify which baserestrictinfo clauses can be sent to the remote
	 * server and which can't.
	 */
	classifyConditions(root,
					   baserel,
					   baserel->baserestrictinfo,
					   &fpinfo->remote_conds,
					   &fpinfo->local_conds);

	/*
	 * Identify which attributes will need to be retrieved from the remote
	 * server.  These include all attrs needed for joins or final output, plus
	 * all attrs used in the local_conds.  (Note: if we end up using a
	 * parameterized scan, it's possible that some of the join clauses will be
	 * sent to the remote and thus we wouldn't really need to retrieve the
	 * columns used in them.  Doesn't seem worth detecting that case though.)
	 */
	fpinfo->attrs_used = NULL;
	pull_varattnos((Node *) baserel->reltarget->exprs, baserel->relid, &fpinfo->attrs_used);
	foreach (lc, fpinfo->local_conds)
	{
		RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc);

		pull_varattnos((Node *) rinfo->clause, baserel->relid, &fpinfo->attrs_used);
	}

	/*
	 * Compute the selectivity and cost of the local_conds, so we don't have
	 * to do it over again for each path.  The best we can do for these
	 * conditions is to estimate selectivity on the basis of local statistics.
	 */
	fpinfo->local_conds_sel =
		clauselist_selectivity(root, fpinfo->local_conds, baserel->relid, JOIN_INNER, NULL);

	cost_qual_eval(&fpinfo->local_conds_cost, fpinfo->local_conds, root);

	/*
	 * Set cached relation costs to some negative value, so that we can detect
	 * when they are set to some sensible costs during one (usually the first)
	 * of the calls to estimate_path_cost_size().
	 */
	fpinfo->rel_startup_cost = -1;
	fpinfo->rel_total_cost = -1;

	/*
	 * If the table or the server is configured to use remote estimates,
	 * connect to the foreign server and execute EXPLAIN to estimate the
	 * number of rows selected by the restriction clauses, as well as the
	 * average row width.  Otherwise, estimate using whatever statistics we
	 * have locally, in a way similar to ordinary tables.
	 */
	if (fpinfo->use_remote_estimate)
	{
		/*
		 * Get cost/size estimates with help of remote server.  Save the
		 * values in fpinfo so we don't need to do it again to generate the
		 * basic foreign path.
		 */
		estimate_path_cost_size(root,
								baserel,
								NIL,
								NIL,
								&fpinfo->rows,
								&fpinfo->width,
								&fpinfo->startup_cost,
								&fpinfo->total_cost);

		/* Report estimated baserel size to planner. */
		baserel->rows = fpinfo->rows;
		baserel->reltarget->width = fpinfo->width;
	}
	else
	{
		/*
		 * If the foreign table has never been ANALYZEd, it will have relpages
		 * and reltuples equal to zero, which most likely has nothing to do
		 * with reality.  We can't do a whole lot about that if we're not
		 * allowed to consult the remote server, but we can use a hack similar
		 * to plancat.c's treatment of empty relations: use a minimum size
		 * estimate of 10 pages, and divide by the column-datatype-based width
		 * estimate to get the corresponding number of tuples.
		 */
		if (baserel->pages == 0 && baserel->tuples == 0)
		{
			baserel->pages = 10;
			baserel->tuples =
				(10 * BLCKSZ) / (baserel->reltarget->width + MAXALIGN(SizeofHeapTupleHeader));
		}

		/* Estimate baserel size as best we can with local statistics. */
		set_baserel_size_estimates(root, baserel);

		/* Fill in basically-bogus cost estimates for use later. */
		estimate_path_cost_size(root,
								baserel,
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
	fpinfo->relation_index = baserel->relid;
	return;
}

/* This creates the fdw_relation_info object for hypertables and foreign table type objects. */
static void
get_foreign_rel_size(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid)
{
	RangeTblEntry *rte = planner_rt_fetch(baserel->relid, root);
	/* A base hypertable is a regular table and not a foreign table. It is the only
	 * kind of regular table that will ever have this callback called on it. */
	if (RELKIND_RELATION == rte->relkind)
	{
		fdw_relation_info_create(root,
								 baserel,
								 InvalidOid,
								 foreigntableid,
								 TS_FDW_RELATION_INFO_HYPERTABLE);
	}
	else
	{
		ForeignTable *table = GetForeignTable(foreigntableid);
		fdw_relation_info_create(root,
								 baserel,
								 table->serverid,
								 foreigntableid,
								 TS_FDW_RELATION_INFO_FOREIGN_TABLE);
		apply_table_options(table, fdw_relation_info_get(baserel));
	}
}
/*
 * get_useful_ecs_for_relation
 *		Determine which EquivalenceClasses might be involved in useful
 *		orderings of this relation.
 *
 * This function is in some respects a mirror image of the core function
 * pathkeys_useful_for_merging: for a regular table, we know what indexes
 * we have and want to test whether any of them are useful.  For a foreign
 * table, we don't know what indexes are present on the remote side but
 * want to speculate about which ones we'd like to use if they existed.
 *
 * This function returns a list of potentially-useful equivalence classes,
 * but it does not guarantee that an EquivalenceMember exists which contains
 * Vars only from the given relation.  For example, given ft1 JOIN t1 ON
 * ft1.x + t1.x = 0, this function will say that the equivalence class
 * containing ft1.x + t1.x is potentially useful.  Supposing ft1 is remote and
 * t1 is local (or on a different server), it will turn out that no useful
 * ORDER BY clause can be generated.  It's not our job to figure that out
 * here; we're only interested in identifying relevant ECs.
 */
static List *
get_useful_ecs_for_relation(PlannerInfo *root, RelOptInfo *rel)
{
	List *useful_eclass_list = NIL;
	ListCell *lc;
	Relids relids;

	/*
	 * First, consider whether any active EC is potentially useful for a merge
	 * join against this relation.
	 */
	if (rel->has_eclass_joins)
	{
		foreach (lc, root->eq_classes)
		{
			EquivalenceClass *cur_ec = (EquivalenceClass *) lfirst(lc);

			if (eclass_useful_for_merging(root, cur_ec, rel))
				useful_eclass_list = lappend(useful_eclass_list, cur_ec);
		}
	}

	/*
	 * Next, consider whether there are any non-EC derivable join clauses that
	 * are merge-joinable.  If the joininfo list is empty, we can exit
	 * quickly.
	 */
	if (rel->joininfo == NIL)
		return useful_eclass_list;

	/* If this is a child rel, we must use the topmost parent rel to search. */
	if (IS_OTHER_REL(rel))
	{
		Assert(!bms_is_empty(rel->top_parent_relids));
		relids = rel->top_parent_relids;
	}
	else
		relids = rel->relids;

	/* Check each join clause in turn. */
	foreach (lc, rel->joininfo)
	{
		RestrictInfo *restrictinfo = (RestrictInfo *) lfirst(lc);

		/* Consider only mergejoinable clauses */
		if (restrictinfo->mergeopfamilies == NIL)
			continue;

		/* Make sure we've got canonical ECs. */
		update_mergeclause_eclasses(root, restrictinfo);

		/*
		 * restrictinfo->mergeopfamilies != NIL is sufficient to guarantee
		 * that left_ec and right_ec will be initialized, per comments in
		 * distribute_qual_to_rels.
		 *
		 * We want to identify which side of this merge-joinable clause
		 * contains columns from the relation produced by this RelOptInfo. We
		 * test for overlap, not containment, because there could be extra
		 * relations on either side.  For example, suppose we've got something
		 * like ((A JOIN B ON A.x = B.x) JOIN C ON A.y = C.y) LEFT JOIN D ON
		 * A.y = D.y.  The input rel might be the joinrel between A and B, and
		 * we'll consider the join clause A.y = D.y. relids contains a
		 * relation not involved in the join class (B) and the equivalence
		 * class for the left-hand side of the clause contains a relation not
		 * involved in the input rel (C).  Despite the fact that we have only
		 * overlap and not containment in either direction, A.y is potentially
		 * useful as a sort column.
		 *
		 * Note that it's even possible that relids overlaps neither side of
		 * the join clause.  For example, consider A LEFT JOIN B ON A.x = B.x
		 * AND A.x = 1.  The clause A.x = 1 will appear in B's joininfo list,
		 * but overlaps neither side of B.  In that case, we just skip this
		 * join clause, since it doesn't suggest a useful sort order for this
		 * relation.
		 */
		if (bms_overlap(relids, restrictinfo->right_ec->ec_relids))
			useful_eclass_list = list_append_unique_ptr(useful_eclass_list, restrictinfo->right_ec);
		else if (bms_overlap(relids, restrictinfo->left_ec->ec_relids))
			useful_eclass_list = list_append_unique_ptr(useful_eclass_list, restrictinfo->left_ec);
	}

	return useful_eclass_list;
}

/*
 * get_useful_pathkeys_for_relation
 *		Determine which orderings of a relation might be useful.
 *
 * Getting data in sorted order can be useful either because the requested
 * order matches the final output ordering for the overall query we're
 * planning, or because it enables an efficient merge join.  Here, we try
 * to figure out which pathkeys to consider.
 */
static List *
get_useful_pathkeys_for_relation(PlannerInfo *root, RelOptInfo *rel)
{
	List *useful_pathkeys_list = NIL;
	List *useful_eclass_list;
	TsFdwRelationInfo *fpinfo = fdw_relation_info_get(rel);
	EquivalenceClass *query_ec = NULL;
	ListCell *lc;

	/*
	 * Pushing the query_pathkeys to the remote server is always worth
	 * considering, because it might let us avoid a local sort.
	 */
	if (root->query_pathkeys)
	{
		bool query_pathkeys_ok = true;

		foreach (lc, root->query_pathkeys)
		{
			PathKey *pathkey = (PathKey *) lfirst(lc);
			EquivalenceClass *pathkey_ec = pathkey->pk_eclass;
			Expr *em_expr;

			/*
			 * The planner and executor don't have any clever strategy for
			 * taking data sorted by a prefix of the query's pathkeys and
			 * getting it to be sorted by all of those pathkeys. We'll just
			 * end up resorting the entire data set.  So, unless we can push
			 * down all of the query pathkeys, forget it.
			 *
			 * is_foreign_expr would detect volatile expressions as well, but
			 * checking ec_has_volatile here saves some cycles.
			 */
			if (pathkey_ec->ec_has_volatile || !(em_expr = find_em_expr_for_rel(pathkey_ec, rel)) ||
				!is_foreign_expr(root, rel, em_expr))
			{
				query_pathkeys_ok = false;
				break;
			}
		}

		if (query_pathkeys_ok)
			useful_pathkeys_list = list_make1(list_copy(root->query_pathkeys));
	}

	/*
	 * Even if we're not using remote estimates, having the remote side do the
	 * sort generally won't be any worse than doing it locally, and it might
	 * be much better if the remote side can generate data in the right order
	 * without needing a sort at all.  However, what we're going to do next is
	 * try to generate pathkeys that seem promising for possible merge joins,
	 * and that's more speculative.  A wrong choice might hurt quite a bit, so
	 * bail out if we can't use remote estimates.
	 */
	if (!fpinfo->use_remote_estimate)
		return useful_pathkeys_list;

	/* Get the list of interesting EquivalenceClasses. */
	useful_eclass_list = get_useful_ecs_for_relation(root, rel);

	/* Extract unique EC for query, if any, so we don't consider it again. */
	if (list_length(root->query_pathkeys) == 1)
	{
		PathKey *query_pathkey = linitial(root->query_pathkeys);

		query_ec = query_pathkey->pk_eclass;
	}

	/*
	 * As a heuristic, the only pathkeys we consider here are those of length
	 * one.  It's surely possible to consider more, but since each one we
	 * choose to consider will generate a round-trip to the remote side, we
	 * need to be a bit cautious here.  It would sure be nice to have a local
	 * cache of information about remote index definitions...
	 */
	foreach (lc, useful_eclass_list)
	{
		EquivalenceClass *cur_ec = lfirst(lc);
		Expr *em_expr;
		PathKey *pathkey;

		/* If redundant with what we did above, skip it. */
		if (cur_ec == query_ec)
			continue;

		/* If no pushable expression for this rel, skip it. */
		em_expr = find_em_expr_for_rel(cur_ec, rel);
		if (em_expr == NULL || !is_foreign_expr(root, rel, em_expr))
			continue;

		/* Looks like we can generate a pathkey, so let's do it. */
		pathkey = make_canonical_pathkey(root,
										 cur_ec,
										 linitial_oid(cur_ec->ec_opfamilies),
										 BTLessStrategyNumber,
										 false);
		useful_pathkeys_list = lappend(useful_pathkeys_list, list_make1(pathkey));
	}

	return useful_pathkeys_list;
}

static void
add_paths_with_pathkeys_for_rel(PlannerInfo *root, RelOptInfo *rel, Path *epq_path)
{
	List *useful_pathkeys_list = NIL; /* List of all pathkeys */
	ListCell *lc;

	useful_pathkeys_list = get_useful_pathkeys_for_relation(root, rel);

	/* Create one path for each set of pathkeys we found above. */
	foreach (lc, useful_pathkeys_list)
	{
		double rows;
		int width;
		Cost startup_cost;
		Cost total_cost;
		List *useful_pathkeys = lfirst(lc);
		Path *sorted_epq_path;

		estimate_path_cost_size(root,
								rel,
								NIL,
								useful_pathkeys,
								&rows,
								&width,
								&startup_cost,
								&total_cost);

		/*
		 * The EPQ path must be at least as well sorted as the path itself, in
		 * case it gets used as input to a mergejoin.
		 */
		sorted_epq_path = epq_path;
		if (sorted_epq_path != NULL &&
			!pathkeys_contained_in(useful_pathkeys, sorted_epq_path->pathkeys))
			sorted_epq_path =
				(Path *) create_sort_path(root, rel, sorted_epq_path, useful_pathkeys, -1.0);

		add_path(rel,
				 (Path *) create_foreignscan_path(root,
												  rel,
												  NULL,
												  rows,
												  startup_cost,
												  total_cost,
												  useful_pathkeys,
												  NULL,
												  sorted_epq_path,
												  NIL));
	}
}

/*
 * This function adds hypertable-server paths for hypertables when the per-server queries
 * optimization is enabled. In this case, the base hypertable will not have its chunk's expanded and
 * so will be a plain relation. Here, we determine server-chunk assignment and create an append
 * relation with children corresponding to each hypertable-server. In the future, different
 * assignments can create their own append paths and have the cost optimizer pick the best one.
 */
static void
add_remote_hypertable_paths(PlannerInfo *root, RelOptInfo *hypertable_rel, Oid hypertable_relid)
{
	ListCell *lc;
	List *subpaths = NIL;
	AppendPath *appendpath;
	List *server_chunk_assignment_list;
	TimescaleDBPrivate *rel_info = hypertable_rel->fdw_private;
	List *chunk_oids = rel_info->chunk_oids;

	/* nothing to do for empty hypertables */
	if (chunk_oids == NIL)
		return;

	/* remove the parent if it is in there */
	chunk_oids = list_delete_oid(chunk_oids, hypertable_relid);
	server_chunk_assignment_list = server_chunk_assignment_by_using_attached_server(chunk_oids);

	/* remove all existing paths, they are not valid because the contain the un-expanded parent */
	hypertable_rel->pathlist = NIL;
	foreach (lc, server_chunk_assignment_list)
	{
		ForeignPath *subpath;
		RelOptInfo *hypertable_server_rel;
		TsFdwRelationInfo *hypertable_server_fpinfo;
		ServerChunkAssignment *sca = lfirst(lc);

		Assert(list_length(sca->chunk_oids) == list_length(sca->remote_chunk_ids));

		hypertable_server_rel = palloc(sizeof(RelOptInfo));
		memcpy(hypertable_server_rel, hypertable_rel, sizeof(*hypertable_server_rel));
		hypertable_server_rel->serverid = sca->server_oid;

		/* set the fdwroutine, which should always be the timescale_fdw routine because
		 * hypertables should always be associated with the timescale_fdw */
		hypertable_server_rel->fdwroutine = GetFdwRoutineByServerId(sca->server_oid);
		Assert(hypertable_server_rel->fdwroutine == &timescaledb_fdw_routine);

		hypertable_server_rel->fdw_private = NULL;
		fdw_relation_info_create(root,
								 hypertable_server_rel,
								 sca->server_oid,
								 hypertable_relid,
								 TS_FDW_RELATION_INFO_HYPERTABLE_SERVER);
		hypertable_server_fpinfo = fdw_relation_info_get(hypertable_server_rel);
		hypertable_server_fpinfo->sca = sca;

		subpath = create_foreignscan_path(root,
										  hypertable_server_rel,
										  NULL, /* default pathtarget */
										  hypertable_server_fpinfo->rows,
										  hypertable_server_fpinfo->startup_cost,
										  hypertable_server_fpinfo->total_cost,
										  NIL,  /* no pathkeys */
										  NULL, /* no outer rel either */
										  NULL, /* no extra plan */
										  NIL); /* no fdw_private list */
		subpaths = lappend(subpaths, subpath);
	}
	appendpath =
		create_append_path_compat(root, hypertable_rel, subpaths, NIL, NULL, 0, false, NIL, -1);
	add_path(hypertable_rel, (Path *) appendpath);
}

static void
get_foreign_paths(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid)
{
	TsFdwRelationInfo *fpinfo = fdw_relation_info_get(baserel);
	ForeignPath *path;

	if (fpinfo->type == TS_FDW_RELATION_INFO_HYPERTABLE)
		return add_remote_hypertable_paths(root, baserel, foreigntableid);

	if (baserel->reloptkind == RELOPT_JOINREL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("foreign joins are not supported")));

	/*
	 * Create simplest ForeignScan path node and add it to baserel.  This path
	 * corresponds to SeqScan path of regular tables (though depending on what
	 * baserestrict conditions we were able to send to remote, there might
	 * actually be an indexscan happening there).  We already did all the work
	 * to estimate cost and size of this path.
	 */
	path = create_foreignscan_path(root,
								   baserel,
								   NULL, /* default pathtarget */
								   fpinfo->rows,
								   fpinfo->startup_cost,
								   fpinfo->total_cost,
								   NIL,  /* no pathkeys */
								   NULL, /* no outer rel either */
								   NULL, /* no extra plan */
								   NIL); /* no fdw_private list */
	add_path(baserel, (Path *) path);

	/* Add paths with pathkeys */
	add_paths_with_pathkeys_for_rel(root, baserel, NULL);
}

static ForeignScan *
get_foreign_plan(PlannerInfo *root, RelOptInfo *foreignrel, Oid foreigntableid,
				 ForeignPath *best_path, List *tlist, List *scan_clauses, Plan *outer_plan)
{
	TsFdwRelationInfo *fpinfo = fdw_relation_info_get(foreignrel);
	Index scan_relid;
	List *fdw_private;
	List *remote_exprs = NIL;
	List *local_exprs = NIL;
	List *params_list = NIL;
	List *fdw_scan_tlist = NIL;
	List *fdw_recheck_quals = NIL;
	List *retrieved_attrs;
	StringInfoData sql;
	ListCell *lc;

	if (IS_SIMPLE_REL(foreignrel))
	{
		/*
		 * For base relations, set scan_relid as the relid of the relation.
		 */
		scan_relid = foreignrel->relid;

		/*
		 * In a base-relation scan, we must apply the given scan_clauses.
		 *
		 * Separate the scan_clauses into those that can be executed remotely
		 * and those that can't.  baserestrictinfo clauses that were
		 * previously determined to be safe or unsafe by classifyConditions
		 * are found in fpinfo->remote_conds and fpinfo->local_conds. Anything
		 * else in the scan_clauses list will be a join clause, which we have
		 * to check for remote-safety.
		 *
		 * Note: the join clauses we see here should be the exact same ones
		 * previously examined by postgresGetForeignPaths.  Possibly it'd be
		 * worth passing forward the classification work done then, rather
		 * than repeating it here.
		 *
		 * This code must match "extract_actual_clauses(scan_clauses, false)"
		 * except for the additional decision about remote versus local
		 * execution.
		 */
		foreach (lc, scan_clauses)
		{
			RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc);

			/* Ignore any pseudoconstants, they're dealt with elsewhere */
			if (rinfo->pseudoconstant)
				continue;

			if (list_member_ptr(fpinfo->remote_conds, rinfo))
				remote_exprs = lappend(remote_exprs, rinfo->clause);
			else if (list_member_ptr(fpinfo->local_conds, rinfo))
				local_exprs = lappend(local_exprs, rinfo->clause);
			else if (is_foreign_expr(root, foreignrel, rinfo->clause))
				remote_exprs = lappend(remote_exprs, rinfo->clause);
			else
				local_exprs = lappend(local_exprs, rinfo->clause);
		}

		/*
		 * For a base-relation scan, we have to support EPQ recheck, which
		 * should recheck all the remote quals.
		 */
		fdw_recheck_quals = remote_exprs;

		/* check if this is a hypertable-server relation with multi-chunk assignment */
		if (fpinfo->sca != NULL)
		{
			/* Mark as a relation with no RTE. This is usally only done for joins,
			 * but we do that here because we can't use any real relation:
			 * - if we use the hypertable than various parts of the planner and executor will try to
			 *   look up the foreign table information for this but it's not a foreign server
			 * - if we use a chunk the system will try to use the foreign server associated with the
			 * chunk relation. But, this is not necessarily the server we want to use (remember,
			 * there can be several associated with the chunk).
			 */
			scan_relid = 0;
			/* since the relation is 0, have to build the tlist explicitly */
			fdw_scan_tlist = build_tlist_to_deparse(foreignrel);
		}
	}
	else if (IS_JOIN_REL(foreignrel))
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("foreign joins are not supported")));
	}
	else
	{
		/*
		 * Upper relation - set scan_relid to 0.
		 */
		scan_relid = 0;

		/*
		 * For a join rel, baserestrictinfo is NIL and we are not considering
		 * parameterization right now, so there should be no scan_clauses for
		 * a joinrel or an upper rel either.
		 */
		Assert(!scan_clauses);

		/*
		 * Instead we get the conditions to apply from the fdw_private
		 * structure.
		 */
		remote_exprs = extract_actual_clauses(fpinfo->remote_conds, false);
		local_exprs = extract_actual_clauses(fpinfo->local_conds, false);

		/*
		 * We leave fdw_recheck_quals empty in this case, since we never need
		 * to apply EPQ recheck clauses.  In the case of a joinrel, EPQ
		 * recheck is handled elsewhere --- see postgresGetForeignJoinPaths().
		 * If we're planning an upperrel (ie, remote grouping or aggregation)
		 * then there's no EPQ to do because SELECT FOR UPDATE wouldn't be
		 * allowed, and indeed we *can't* put the remote clauses into
		 * fdw_recheck_quals because the unaggregated Vars won't be available
		 * locally.
		 */

		/* Build the list of columns to be fetched from the foreign server. */
		fdw_scan_tlist = build_tlist_to_deparse(foreignrel);
	}

	/*
	 * Build the query string to be sent for execution, and identify
	 * expressions to be sent as parameters.
	 */
	initStringInfo(&sql);
	deparseSelectStmtForRel(&sql,
							root,
							foreignrel,
							fdw_scan_tlist,
							remote_exprs,
							best_path->path.pathkeys,
							false,
							&retrieved_attrs,
							&params_list,
							fpinfo->sca);

	/* Remember remote_exprs for possible use by postgresPlanDirectModify */
	fpinfo->final_remote_exprs = remote_exprs;

	/*
	 * Build the fdw_private list that will be available to the executor.
	 * Items in the list must match order in enum FdwScanPrivateIndex.
	 */
	fdw_private = list_make5(makeString(sql.data),
							 retrieved_attrs,
							 makeInteger(fpinfo->fetch_size),
							 makeInteger(fpinfo->server->serverid),
							 (fpinfo->sca != NULL ? list_copy(fpinfo->sca->chunk_oids) : NIL));
	Assert(!IS_JOIN_REL(foreignrel));

	if (IS_UPPER_REL(foreignrel))
		fdw_private = lappend(fdw_private, makeString(fpinfo->relation_name->data));

	/*
	 * Create the ForeignScan node for the given relation.
	 *
	 * Note that the remote parameter expressions are stored in the fdw_exprs
	 * field of the finished plan node; we can't keep them in private state
	 * because then they wouldn't be subject to later planner processing.
	 */
	return make_foreignscan(tlist,
							local_exprs,
							scan_relid,
							params_list,
							fdw_private,
							fdw_scan_tlist,
							fdw_recheck_quals,
							outer_plan);
}

static void
initialize_fdw_server_state(TsFdwServerState *fdw_server, UserMapping *um)
{
	fdw_server->user = um;
	fdw_server->serverid = um->serverid;
	fdw_server->conn = remote_dist_txn_get_connection(fdw_server->user, REMOTE_TXN_USE_PREP_STMT);
	fdw_server->p_stmt = NULL;
}

/*
 * create_foreign_modify
 *		Construct an execution state of a foreign insert/update/delete
 *		operation
 */
static TsFdwModifyState *
create_foreign_modify(EState *estate, RangeTblEntry *rte, ResultRelInfo *rri, CmdType operation,
					  Plan *subplan, char *query, List *target_attrs, bool has_returning,
					  List *retrieved_attrs, List *usermappings)
{
	TsFdwModifyState *fmstate;
	Relation rel = rri->ri_RelationDesc;
	TupleDesc tupdesc = RelationGetDescr(rel);
	ListCell *lc;
	int i = 0;
	int num_servers = usermappings == NIL ? 1 : list_length(usermappings);

	/* Begin constructing TsFdwModifyState. */
	fmstate = (TsFdwModifyState *) palloc0(TS_FDW_MODIFY_STATE_SIZE(num_servers));
	fmstate->rel = rel;

	/*
	 * Identify which user to do the remote access as.  This should match what
	 * ExecCheckRTEPerms() does.
	 */

	if (NIL != usermappings)
	{
		/*
		 * This is either (1) an INSERT on a hypertable chunk, or (2) an
		 * UPDATE or DELETE on a chunk. In the former case (1), the servers
		 * were passed on from the INSERT path via the chunk insert state, and
		 * in the latter case (2), the servers were resolved at planning time
		 * in the FDW planning callback.
		 */

		foreach (lc, usermappings)
		{
			UserMapping *um = lfirst(lc);

			initialize_fdw_server_state(&fmstate->servers[i++], um);
		}
	}
	else
	{
		/*
		 * If there is no chunk insert state and no servers from planning,
		 * this is an INSERT, UPDATE, or DELETE on a standalone foreign table.
		 * We must get the server from the foreign table's metadata.
		 */
		ForeignTable *table = GetForeignTable(rri->ri_RelationDesc->rd_id);
		Oid userid = rte->checkAsUser ? rte->checkAsUser : GetUserId();
		UserMapping *um = GetUserMapping(userid, table->serverid);

		initialize_fdw_server_state(&fmstate->servers[0], um);
	}

	/* Set up remote query information. */
	fmstate->query = query;
	fmstate->target_attrs = target_attrs;
	fmstate->has_returning = has_returning;
	fmstate->retrieved_attrs = retrieved_attrs;
	fmstate->prepared = false; /* PREPARE will happen later */
	fmstate->num_servers = num_servers;

	/* Create context for per-tuple temp workspace. */
	fmstate->temp_cxt = AllocSetContextCreate(estate->es_query_cxt,
											  "timescaledb_fdw temporary data",
											  ALLOCSET_SMALL_SIZES);

	/* Prepare for input conversion of RETURNING results. */
	if (fmstate->has_returning)
		fmstate->att_conv_metadata = data_format_create_att_conv_in_metadata(tupdesc);

	if (operation == CMD_UPDATE || operation == CMD_DELETE)
	{
		Assert(subplan != NULL);

		/* Find the ctid resjunk column in the subplan's result */
		fmstate->ctid_attno = ExecFindJunkAttributeInTlist(subplan->targetlist, "ctid");
		if (!AttributeNumberIsValid(fmstate->ctid_attno))
			elog(ERROR, "could not find junk ctid column");
	}

	fmstate->stmt_params = stmt_params_create(fmstate->target_attrs,
											  operation == CMD_UPDATE || operation == CMD_DELETE,
											  tupdesc,
											  1);

	return fmstate;
}

/*
 * Prepare for processing of parameters used in remote query.
 */
static void
prepare_query_params(PlanState *node, List *fdw_exprs, int num_params, FmgrInfo **param_flinfo,
					 List **param_exprs, const char ***param_values)
{
	int i;
	ListCell *lc;

	Assert(num_params > 0);

	/* Prepare for output conversion of parameters used in remote query. */
	*param_flinfo = (FmgrInfo *) palloc0(sizeof(FmgrInfo) * num_params);

	i = 0;
	foreach (lc, fdw_exprs)
	{
		Node *param_expr = (Node *) lfirst(lc);
		Oid typefnoid;
		bool isvarlena;

		getTypeOutputInfo(exprType(param_expr), &typefnoid, &isvarlena);
		fmgr_info(typefnoid, &(*param_flinfo)[i]);
		i++;
	}

	/*
	 * Prepare remote-parameter expressions for evaluation.  (Note: in
	 * practice, we expect that all these expressions will be just Params, so
	 * we could possibly do something more efficient than using the full
	 * expression-eval machinery for this.  But probably there would be little
	 * benefit, and it'd require postgres_fdw to know more than is desirable
	 * about Param evaluation.)
	 */
	*param_exprs = ExecInitExprList(fdw_exprs, node);

	/* Allocate buffer for text form of query parameters. */
	*param_values = (const char **) palloc0(num_params * sizeof(char *));
}

/*
 * Fill an array with query parameter values in text format.
 */
static void
fill_query_params_array(ExprContext *econtext, FmgrInfo *param_flinfo, List *param_exprs,
						const char **param_values)
{
	int nestlevel;
	int i;
	ListCell *lc;

	nestlevel = set_transmission_modes();

	i = 0;
	foreach (lc, param_exprs)
	{
		ExprState *expr_state = (ExprState *) lfirst(lc);
		Datum expr_value;
		bool is_null;

		/* Evaluate the parameter expression */
		expr_value = ExecEvalExpr(expr_state, econtext, &is_null);

		/*
		 * Get string representation of each parameter value by invoking
		 * type-specific output function, unless the value is null.
		 */
		if (is_null)
			param_values[i] = NULL;
		else
			param_values[i] = OutputFunctionCall(&param_flinfo[i], expr_value);

		i++;
	}

	reset_transmission_modes(nestlevel);
}

/*
 * Create cursor for node's query with current parameter values.
 */
static void
create_cursor(ForeignScanState *node)
{
	TsFdwScanState *fsstate = (TsFdwScanState *) node->fdw_state;
	ExprContext *econtext = node->ss.ps.ps_ExprContext;
	int num_params = fsstate->num_params;
	const char **values = fsstate->param_values;
	TSConnection *conn = fsstate->conn;
	StringInfoData buf;
	AsyncRequest *req;

	/*
	 * Construct array of query parameter values in text format.  We do the
	 * conversions in the short-lived per-tuple context, so as not to cause a
	 * memory leak over repeated scans.
	 */
	if (num_params > 0)
	{
		MemoryContext oldcontext;
		oldcontext = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);
		fill_query_params_array(econtext, fsstate->param_flinfo, fsstate->param_exprs, values);
		MemoryContextSwitchTo(oldcontext);
	}

	/* Assign a unique ID for my cursor */
	fsstate->cursor_number = remote_connection_get_cursor_number();

	/* Construct the DECLARE CURSOR command */
	initStringInfo(&buf);
	appendStringInfo(&buf, "DECLARE c%u CURSOR FOR\n%s", fsstate->cursor_number, fsstate->query);

	/*
	 * Notice that we pass NULL for paramTypes, thus forcing the remote server
	 * to infer types for all parameters.  Since we explicitly cast every
	 * parameter (see deparse.c), the "inference" is trivial and will produce
	 * the desired result.  This allows us to avoid assuming that the remote
	 * server has the same OIDs we do for the parameters' types.
	 */
	req = async_request_send_with_params(conn,
										 buf.data,
										 stmt_params_create_from_values(values, num_params),
										 FORMAT_TEXT);

	Assert(NULL != req);

	async_request_wait_ok_command(req);
	pfree(req);

	/* Mark the cursor as created, and show no tuples have been retrieved */
	fsstate->cursor_exists = true;
	fsstate->tuples = NULL;
	fsstate->num_tuples = 0;
	fsstate->next_tuple = 0;
	fsstate->fetch_ct_2 = 0;
	fsstate->eof_reached = false;

	/* Clean up */
	pfree(buf.data);
}

static void
begin_foreign_scan(ForeignScanState *node, int eflags)
{
	ForeignScan *fsplan = (ForeignScan *) node->ss.ps.plan;
	EState *estate = node->ss.ps.state;
	TsFdwScanState *fsstate;
	RangeTblEntry *rte;
	Oid userid;
	UserMapping *user;
	int rtindex;
	int num_params;
	int server_id;

	/*
	 * Do nothing in EXPLAIN (no ANALYZE) case.  node->fdw_state stays NULL.
	 */
	if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
		return;

	/*
	 * We'll save private state in node->fdw_state.
	 */
	fsstate = (TsFdwScanState *) palloc0(sizeof(TsFdwScanState));
	node->fdw_state = (void *) fsstate;

	/*
	 * Identify which user to do the remote access as.  This should match what
	 * ExecCheckRTEPerms() does.  In case of a join or aggregate, use the
	 * lowest-numbered member RTE as a representative; we would get the same
	 * result from any.
	 */
	if (fsplan->scan.scanrelid > 0)
		rtindex = fsplan->scan.scanrelid;
	else
		rtindex = bms_next_member(fsplan->fs_relids, -1);
	rte = rt_fetch(rtindex, estate->es_range_table);
	userid = rte->checkAsUser ? rte->checkAsUser : GetUserId();

	/* Get info about foreign table. */
	server_id = intVal(list_nth(fsplan->fdw_private, FdwScanPrivateServerId));
	user = GetUserMapping(userid, server_id);

	/*
	 * Get connection to the foreign server.  Connection manager will
	 * establish new connection if necessary.
	 */
	fsstate->conn = remote_dist_txn_get_connection(user,
												   list_length(fsplan->fdw_exprs) > 0 ?
													   REMOTE_TXN_USE_PREP_STMT :
													   REMOTE_TXN_NO_PREP_STMT);

	/* Get private info created by planner functions. */
	fsstate->query = strVal(list_nth(fsplan->fdw_private, FdwScanPrivateSelectSql));
	fsstate->retrieved_attrs = (List *) list_nth(fsplan->fdw_private, FdwScanPrivateRetrievedAttrs);
	fsstate->fetch_size = intVal(list_nth(fsplan->fdw_private, FdwScanPrivateFetchSize));

	/* Create contexts for batches of tuples and per-tuple temp workspace. */
	fsstate->batch_cxt = AllocSetContextCreate(estate->es_query_cxt,
											   "timescaledb_fdw tuple data",
											   ALLOCSET_DEFAULT_SIZES);
	fsstate->temp_cxt = AllocSetContextCreate(estate->es_query_cxt,
											  "timescaledb_fdw temporary data",
											  ALLOCSET_SMALL_SIZES);

	/*
	 * Get info we'll need for converting data fetched from the foreign server
	 * into local representation and error reporting during that process.
	 */
	if (fsplan->scan.scanrelid > 0)
	{
		fsstate->rel = node->ss.ss_currentRelation;
		fsstate->tupdesc = RelationGetDescr(fsstate->rel);
	}
	else
	{
		fsstate->rel = NULL;
		fsstate->tupdesc = node->ss.ss_ScanTupleSlot->tts_tupleDescriptor;
	}

	fsstate->att_conv_metadata = data_format_create_att_conv_in_metadata(fsstate->tupdesc);

	/*
	 * Prepare for processing of parameters used in remote query, if any.
	 */
	num_params = list_length(fsplan->fdw_exprs);
	fsstate->num_params = num_params;
	if (num_params > 0)
		prepare_query_params((PlanState *) node,
							 fsplan->fdw_exprs,
							 num_params,
							 &fsstate->param_flinfo,
							 &fsstate->param_exprs,
							 &fsstate->param_values);

	fsstate->cursor_exists = false;
}

/*
 * Fetch some more rows from the node's cursor.
 */
static void
fetch_more_data(ForeignScanState *node)
{
	TsFdwScanState *fsstate = (TsFdwScanState *) node->fdw_state;
	AsyncRequest *volatile req = NULL;
	AsyncResponseResult *volatile rsp = NULL;
	MemoryContext oldcontext;

	/*
	 * We'll store the tuples in the batch_cxt.  First, flush the previous
	 * batch.
	 */
	fsstate->tuples = NULL;
	MemoryContextReset(fsstate->batch_cxt);
	oldcontext = MemoryContextSwitchTo(fsstate->batch_cxt);

	/* PGresult must be released before leaving this function. */
	PG_TRY();
	{
		TSConnection *conn = fsstate->conn;
		PGresult *res;
		char sql[64];
		int numrows;
		int i;

		snprintf(sql,
				 sizeof(sql),
				 "FETCH %d FROM c%u",
				 fsstate->fetch_size,
				 fsstate->cursor_number);

		if (fsstate->att_conv_metadata->binary)
			req = async_request_send_binary(conn, sql);
		else
			req = async_request_send(conn, sql);

		Assert(NULL != req);

		rsp = async_request_wait_any_result(req);

		Assert(NULL != rsp);

		res = async_response_result_get_pg_result(rsp);

		/* On error, report the original query, not the FETCH. */
		if (PQresultStatus(res) != PGRES_TUPLES_OK)
			remote_connection_report_error(ERROR, res, fsstate->conn, false, fsstate->query);

		/* Convert the data into HeapTuples */
		numrows = PQntuples(res);
		fsstate->tuples = (HeapTuple *) palloc0(numrows * sizeof(HeapTuple));
		fsstate->num_tuples = numrows;
		fsstate->next_tuple = 0;

		for (i = 0; i < numrows; i++)
		{
			Assert(IsA(node->ss.ps.plan, ForeignScan));

			fsstate->tuples[i] = make_tuple_from_result_row(res,
															i,
															fsstate->rel,
															fsstate->att_conv_metadata,
															fsstate->retrieved_attrs,
															node,
															fsstate->temp_cxt);
		}

		/* Update fetch_ct_2 */
		if (fsstate->fetch_ct_2 < 2)
			fsstate->fetch_ct_2++;

		/* Must be EOF if we didn't get as many tuples as we asked for. */
		fsstate->eof_reached = (numrows < fsstate->fetch_size);

		pfree(req);
		async_response_result_close(rsp);
		req = NULL;
		rsp = NULL;
	}
	PG_CATCH();
	{
		if (NULL != req)
			pfree(req);

		if (NULL != rsp)
			async_response_result_close(rsp);

		PG_RE_THROW();
	}
	PG_END_TRY();

	MemoryContextSwitchTo(oldcontext);
}

static TupleTableSlot *
iterate_foreign_scan(ForeignScanState *node)
{
	TsFdwScanState *fsstate = (TsFdwScanState *) node->fdw_state;
	TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;

	if (!fsstate->cursor_exists)
		create_cursor(node);

	/*
	 * Get some more tuples, if we've run out.
	 */
	if (fsstate->next_tuple >= fsstate->num_tuples)
	{
		/* No point in another fetch if we already detected EOF, though. */
		if (!fsstate->eof_reached)
			fetch_more_data(node);
		/* If we didn't get any tuples, must be end of data. */
		if (fsstate->next_tuple >= fsstate->num_tuples)
			return ExecClearTuple(slot);
	}

	/*
	 * Return the next tuple.
	 */
	ExecStoreTuple(fsstate->tuples[fsstate->next_tuple++], slot, InvalidBuffer, false);

	return slot;
}

static void
rescan_foreign_scan(ForeignScanState *node)
{
	TsFdwScanState *fsstate = (TsFdwScanState *) node->fdw_state;
	char sql[64];
	AsyncRequest *req;

	/* If we haven't created the cursor yet, nothing to do. */
	if (!fsstate->cursor_exists)
		return;

	/*
	 * If any internal parameters affecting this node have changed, we'd
	 * better destroy and recreate the cursor.  Otherwise, rewinding it should
	 * be good enough.  If we've only fetched zero or one batch, we needn't
	 * even rewind the cursor, just rescan what we have.
	 */
	if (node->ss.ps.chgParam != NULL)
	{
		fsstate->cursor_exists = false;
		snprintf(sql, sizeof(sql), "CLOSE c%u", fsstate->cursor_number);
	}
	else if (fsstate->fetch_ct_2 > 1)
	{
		snprintf(sql, sizeof(sql), "MOVE BACKWARD ALL IN c%u", fsstate->cursor_number);
	}
	else
	{
		/* Easy: just rescan what we already have in memory, if anything */
		fsstate->next_tuple = 0;
		return;
	}

	/*
	 * We don't use a PG_TRY block here, so be careful not to throw error
	 * without releasing the PGresult.
	 */
	req = async_request_send(fsstate->conn, sql);

	Assert(NULL != req);

	async_request_wait_ok_command(req);

	pfree(req);

	/* Now force a fresh FETCH. */
	fsstate->tuples = NULL;
	fsstate->num_tuples = 0;
	fsstate->next_tuple = 0;
	fsstate->fetch_ct_2 = 0;
	fsstate->eof_reached = false;
}

/*
 * Utility routine to close a cursor.
 */
static void
close_cursor(TSConnection *conn, unsigned int cursor_number)
{
	char sql[64];
	AsyncRequest *req;

	snprintf(sql, sizeof(sql), "CLOSE c%u", cursor_number);

	/*
	 * We don't use a PG_TRY block here, so be careful not to throw error
	 * without releasing the PGresult.
	 */

	req = async_request_send(conn, sql);

	Assert(NULL != req);

	async_request_wait_ok_command(req);

	pfree(req);
}

static void
end_foreign_scan(ForeignScanState *node)
{
	TsFdwScanState *fsstate = (TsFdwScanState *) node->fdw_state;

	/* if fsstate is NULL, we are in EXPLAIN; nothing to do */
	if (fsstate == NULL)
		return;

	/* Close the cursor if open, to prevent accumulation of cursors */
	if (fsstate->cursor_exists)
		close_cursor(fsstate->conn, fsstate->cursor_number);

	/* Release remote connection */
	fsstate->conn = NULL;

	/* MemoryContexts will be deleted automatically. */
}

/*
 * Add resjunk column(s) needed for update/delete on a foreign table
 */
static void
add_foreign_update_targets(Query *parsetree, RangeTblEntry *target_rte, Relation target_relation)
{
	Var *var;
	const char *attrname;
	TargetEntry *tle;

	/*
	 * In timescaledb_fdw, what we need is the ctid, same as for a regular
	 * table.
	 */

	/* Make a Var representing the desired value */
	var = makeVar(parsetree->resultRelation,
				  SelfItemPointerAttributeNumber,
				  TIDOID,
				  -1,
				  InvalidOid,
				  0);

	/* Wrap it in a resjunk TLE with the right name ... */
	attrname = "ctid";

	tle = makeTargetEntry((Expr *) var,
						  list_length(parsetree->targetList) + 1,
						  pstrdup(attrname),
						  true);

	/* ... and add it to the query's targetlist */
	parsetree->targetList = lappend(parsetree->targetList, tle);
}

static List *
get_insert_attrs(Relation rel)
{
	TupleDesc tupdesc = RelationGetDescr(rel);
	List *attrs = NIL;
	int i;

	for (i = 0; i < tupdesc->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc, i);

		if (!attr->attisdropped)
			attrs = lappend_int(attrs, AttrOffsetGetAttrNumber(i));
	}

	return attrs;
}

static List *
get_update_attrs(RangeTblEntry *rte)
{
	List *attrs = NIL;
	int col = -1;

	while ((col = bms_next_member(rte->updatedCols, col)) >= 0)
	{
		/* bit numbers are offset by FirstLowInvalidHeapAttributeNumber */
		AttrNumber attno = col + FirstLowInvalidHeapAttributeNumber;

		if (attno <= InvalidAttrNumber) /* shouldn't happen */
			elog(ERROR, "system-column update is not supported");

		attrs = lappend_int(attrs, attno);
	}

	return attrs;
}

static List *
get_chunk_servers(Oid relid)
{
	Chunk *chunk = ts_chunk_get_by_relid(relid, 0, false);
	List *serveroids = NIL;
	ListCell *lc;

	if (NULL == chunk)
		return NIL;

	foreach (lc, chunk->servers)
	{
		ChunkServer *cs = lfirst(lc);

		serveroids = lappend_oid(serveroids, cs->foreign_server_oid);
	}

	return serveroids;
}

/*
 * Plan INSERT, UPDATE, and DELETE.
 *
 * The main task of this function is to generate (deparse) the SQL statement
 * for the corresponding tables on remote servers.
 *
 * If the planning involves a hypertable, the function is called differently
 * depending on the command:
 *
 * 1. INSERT - called only once during hypertable planning and the given
 * result relation is the hypertable root relation. This is due to
 * TimescaleDBs unique INSERT path. We'd like to plan the INSERT as if it
 * would happen on the root of the hypertable. This is useful because INSERTs
 * should occur via the top-level hypertables on the remote servers
 * (preferrably batched), and not once per individual remote chunk
 * (inefficient and won't go through the standard INSERT path on the remote
 * server).
 *
 * 2. UPDATE and DELETE - called once per chunk and the given result relation
 * is the chunk relation.
 *
 * For non-hypertables, which are foreign tables using the timescaledb_fdw,
 * this function is called the way it normally would be for the FDW API, i.e.,
 * once during planning.
 *
 * For the TimescaleDB insert path, we actually call
 * this function only once on the hypertable's root table instead of once per
 * chunk. This is because we want to send INSERT statements to each remote
 * hypertable rather than each remote chunk.
 *
 * UPDATEs and DELETEs work slightly different since we have no "optimized"
 * path for such operations. Instead, they happen once per chunk.
 */
static List *
plan_foreign_modify(PlannerInfo *root, ModifyTable *plan, Index result_relation, int subplan_index)
{
	CmdType operation = plan->operation;
	RangeTblEntry *rte = planner_rt_fetch(result_relation, root);
	Relation rel;
	StringInfoData sql;
	List *returning_list = NIL;
	List *retrieved_attrs = NIL;
	List *target_attrs = NIL;
	List *servers = NIL;
	bool do_nothing = false;

	initStringInfo(&sql);

	/*
	 * Extract the relevant RETURNING list if any.
	 */
	if (plan->returningLists)
		returning_list = (List *) list_nth(plan->returningLists, subplan_index);

	/*
	 * ON CONFLICT DO UPDATE and DO NOTHING case with inference specification
	 * should have already been rejected in the optimizer, as presently there
	 * is no way to recognize an arbiter index on a foreign table.  Only DO
	 * NOTHING is supported without an inference specification.
	 */
	if (plan->onConflictAction == ONCONFLICT_NOTHING)
		do_nothing = true;
	else if (plan->onConflictAction != ONCONFLICT_NONE)
		elog(ERROR, "unexpected ON CONFLICT specification: %d", (int) plan->onConflictAction);

	/*
	 * Core code already has some lock on each rel being planned, so we can
	 * use NoLock here.
	 */
	rel = heap_open(rte->relid, NoLock);

	/*
	 * Construct the SQL command string
	 *
	 * In an INSERT, we transmit all columns that are defined in the foreign
	 * table.  In an UPDATE, we transmit only columns that were explicitly
	 * targets of the UPDATE, so as to avoid unnecessary data transmission.
	 * (We can't do that for INSERT since we would miss sending default values
	 * for columns not listed in the source statement.)
	 */
	switch (operation)
	{
		case CMD_INSERT:
			target_attrs = get_insert_attrs(rel);
			deparseInsertSql(&sql,
							 rte,
							 result_relation,
							 rel,
							 target_attrs,
							 1,
							 do_nothing,
							 returning_list,
							 &retrieved_attrs);
			break;
		case CMD_UPDATE:
			target_attrs = get_update_attrs(rte);
			deparseUpdateSql(&sql,
							 rte,
							 result_relation,
							 rel,
							 target_attrs,
							 returning_list,
							 &retrieved_attrs);
			servers = get_chunk_servers(rel->rd_id);
			break;
		case CMD_DELETE:
			deparseDeleteSql(&sql, rte, result_relation, rel, returning_list, &retrieved_attrs);
			servers = get_chunk_servers(rel->rd_id);
			break;
		default:
			elog(ERROR, "unexpected operation: %d", (int) operation);
			break;
	}

	heap_close(rel, NoLock);

	/*
	 * Build the fdw_private list that will be available to the executor.
	 * Items in the list must match enum FdwModifyPrivateIndex, above.
	 */
	return list_make5(makeString(sql.data),
					  target_attrs,
					  makeInteger((retrieved_attrs != NIL)),
					  retrieved_attrs,
					  servers);
}

/*
 * Convert a relation's attribute numbers to the corresponding numbers for
 * another relation.
 *
 * Conversions are necessary when, e.g., a (new) chunk's attribute numbers do
 * not match the root table's numbers after a column has been removed.
 */
static List *
convert_attrs(TupleConversionMap *map, List *attrs)
{
	List *new_attrs = NIL;
	ListCell *lc;

	foreach (lc, attrs)
	{
		AttrNumber attnum = lfirst_int(lc);
		int i;

		for (i = 0; i < map->outdesc->natts; i++)
		{
			if (map->attrMap[i] == attnum)
			{
				new_attrs = lappend_int(new_attrs, AttrOffsetGetAttrNumber(i));
				break;
			}
		}

		/* Assert that we found the attribute */
		Assert(i != map->outdesc->natts);
	}

	Assert(list_length(attrs) == list_length(new_attrs));

	return new_attrs;
}

static void
begin_foreign_modify(ModifyTableState *mtstate, ResultRelInfo *rri, List *fdw_private,
					 int subplan_index, int eflags)
{
	TsFdwModifyState *fmstate;
	char *query;
	List *target_attrs;
	bool has_returning;
	List *retrieved_attrs;
	List *usermappings = NIL;
	ChunkInsertState *cis = NULL;
	RangeTblEntry *rte;

	/*
	 * Do nothing in EXPLAIN (no ANALYZE) case.  rri->ri_FdwState stays NULL.
	 */
	if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
		return;

	/* Deconstruct fdw_private data. */
	query = strVal(list_nth(fdw_private, FdwModifyPrivateUpdateSql));
	target_attrs = (List *) list_nth(fdw_private, FdwModifyPrivateTargetAttnums);
	has_returning = intVal(list_nth(fdw_private, FdwModifyPrivateHasReturning));
	retrieved_attrs = (List *) list_nth(fdw_private, FdwModifyPrivateRetrievedAttrs);

	/* Find RTE. */
	rte = rt_fetch(rri->ri_RangeTableIndex, mtstate->ps.state->es_range_table);

	Assert(NULL != rte);

	if (list_length(fdw_private) > FdwModifyPrivateServers)
	{
		List *servers = (List *) list_nth(fdw_private, FdwModifyPrivateServers);
		ListCell *lc;

		foreach (lc, servers)
		{
			Oid serverid = lfirst_oid(lc);
			Oid userid = OidIsValid(rte->checkAsUser) ? rte->checkAsUser : GetUserId();
			UserMapping *um = GetUserMapping(userid, serverid);

			usermappings = lappend(usermappings, um);
		}
	}

	if (list_length(fdw_private) > FdwModifyPrivateChunkInsertState)
	{
		cis = (ChunkInsertState *) list_nth(fdw_private, FdwModifyPrivateChunkInsertState);

		/*
		 * A chunk may have different attribute numbers than the root relation
		 * that we planned the attribute lists for
		 */
		if (NULL != cis->tup_conv_map)
		{
			/*
			 * Convert the target attributes (the inserted or updated
			 * attributes)
			 */
			target_attrs = convert_attrs(cis->tup_conv_map, target_attrs);

			/*
			 * Convert the retrieved attributes, if there is a RETURNING
			 * statement
			 */
			if (NIL != retrieved_attrs)
				retrieved_attrs = convert_attrs(cis->tup_conv_map, retrieved_attrs);
		}

		/*
		 * If there's a chunk insert state, then it has the authoritative
		 * server list.
		 */
		usermappings = cis->usermappings;
	}

	/* Construct an execution state. */
	fmstate = create_foreign_modify(mtstate->ps.state,
									rte,
									rri,
									mtstate->operation,
									mtstate->mt_plans[subplan_index]->plan,
									query,
									target_attrs,
									has_returning,
									retrieved_attrs,
									usermappings);

	rri->ri_FdwState = fmstate;
}

/*
 * store_returning_result
 *		Store the result of a RETURNING clause
 *
 * On error, be sure to release the PGresult on the way out.  Callers do not
 * have PG_TRY blocks to ensure this happens.
 */
static void
store_returning_result(TsFdwModifyState *fmstate, TupleTableSlot *slot, PGresult *res)
{
	PG_TRY();
	{
		HeapTuple newtup;

		newtup = make_tuple_from_result_row(res,
											0,
											fmstate->rel,
											fmstate->att_conv_metadata,
											fmstate->retrieved_attrs,
											NULL,
											fmstate->temp_cxt);
		/* tuple will be deleted when it is cleared from the slot */
		ExecStoreTuple(newtup, slot, InvalidBuffer, true);
	}
	PG_CATCH();
	{
		if (res)
			PQclear(res);
		PG_RE_THROW();
	}
	PG_END_TRY();
}

static PreparedStmt *
prepare_foreign_modify_server(TsFdwModifyState *fmstate, TsFdwServerState *fdw_server)
{
	AsyncRequest *req;

	Assert(NULL == fdw_server->p_stmt);

	req = async_request_send_prepare(fdw_server->conn,
									 fmstate->query,
									 stmt_params_num_params(fmstate->stmt_params));

	Assert(NULL != req);

	/*
	 * Async request interface doesn't seem to allow waiting for multiple
	 * prepared statements in an AsyncRequestSet. Should fix async API
	 */
	return async_request_wait_prepared_statement(req);
}

/*
 * prepare_foreign_modify
 *		Establish a prepared statement for execution of INSERT/UPDATE/DELETE
 */
static void
prepare_foreign_modify(TsFdwModifyState *fmstate)
{
	int i;

	for (i = 0; i < fmstate->num_servers; i++)
	{
		TsFdwServerState *fdw_server = &fmstate->servers[i];

		fdw_server->p_stmt = prepare_foreign_modify_server(fmstate, fdw_server);
	}

	fmstate->prepared = true;
}

static int
response_type(AttConvInMetadata *att_conv_metadata)
{
	if (!ts_guc_enable_connection_binary_data)
		return FORMAT_TEXT;
	return att_conv_metadata == NULL || att_conv_metadata->binary ? FORMAT_BINARY : FORMAT_TEXT;
}

static TupleTableSlot *
exec_foreign_insert(EState *estate, ResultRelInfo *rri, TupleTableSlot *slot,
					TupleTableSlot *planSlot)
{
	TsFdwModifyState *fmstate = (TsFdwModifyState *) rri->ri_FdwState;
	StmtParams *params = fmstate->stmt_params;
	AsyncRequestSet *reqset;
	AsyncResponseResult *rsp;
	int n_rows = -1;
	int i;

	if (!fmstate->prepared)
		prepare_foreign_modify(fmstate);

	reqset = async_request_set_create();

	stmt_params_convert_values(params, slot, NULL);

	for (i = 0; i < fmstate->num_servers; i++)
	{
		TsFdwServerState *fdw_server = &fmstate->servers[i];
		AsyncRequest *req = NULL;
		int type = response_type(fmstate->att_conv_metadata);
		req = async_request_send_prepared_stmt_with_params(fdw_server->p_stmt, params, type);
		Assert(NULL != req);
		async_request_set_add(reqset, req);
	}

	while ((rsp = async_request_set_wait_any_result(reqset)))
	{
		PGresult *res = async_response_result_get_pg_result(rsp);

		if (PQresultStatus(res) != (fmstate->has_returning ? PGRES_TUPLES_OK : PGRES_COMMAND_OK))
			async_response_report_error((AsyncResponse *) rsp, ERROR);

		/*
		 * If we insert into multiple replica chunks, we should only return
		 * the results from the first one
		 */
		if (n_rows == -1)
		{
			/* Check number of rows affected, and fetch RETURNING tuple if any */
			if (fmstate->has_returning)
			{
				n_rows = PQntuples(res);

				if (n_rows > 0)
					store_returning_result(fmstate, slot, res);
			}
			else
				n_rows = atoi(PQcmdTuples(res));
		}

		/* And clean up */
		async_response_result_close(rsp);
		stmt_params_reset(params);
	}

	/*
	 * Currently no way to do a deep cleanup of all request in the request
	 * set. The worry here is that since this runs in a per-chunk insert state
	 * memory context, the async API will accumulate a lot of cruft during
	 * inserts
	 */
	pfree(reqset);

	MemoryContextReset(fmstate->temp_cxt);

	/* Return NULL if nothing was inserted on the remote end */
	return (n_rows > 0) ? slot : NULL;
}

typedef enum ModifyCommand
{
	UPDATE_CMD,
	DELETE_CMD,
} ModifyCommand;

/*
 * Execute either an UPDATE or DELETE.
 */
static TupleTableSlot *
exec_foreign_update_or_delete(EState *estate, ResultRelInfo *rri, TupleTableSlot *slot,
							  TupleTableSlot *plan_slot, ModifyCommand cmd)
{
	TsFdwModifyState *fmstate = (TsFdwModifyState *) rri->ri_FdwState;
	StmtParams *params = fmstate->stmt_params;
	AsyncRequestSet *reqset;
	AsyncResponseResult *rsp;
	Datum datum;
	bool is_null;
	int n_rows = -1;
	int i;

	/* Set up the prepared statement on the remote server, if we didn't yet */
	if (!fmstate->prepared)
		prepare_foreign_modify(fmstate);

	/* Get the ctid that was passed up as a resjunk column */
	datum = ExecGetJunkAttribute(plan_slot, fmstate->ctid_attno, &is_null);

	/* shouldn't ever get a null result... */
	if (is_null)
		elog(ERROR, "ctid is NULL");

	stmt_params_convert_values(params,
							   (cmd == UPDATE_CMD ? slot : NULL),
							   (ItemPointer) DatumGetPointer(datum));
	reqset = async_request_set_create();

	for (i = 0; i < fmstate->num_servers; i++)
	{
		AsyncRequest *req = NULL;
		TsFdwServerState *fdw_server = &fmstate->servers[i];
		int type = response_type(fmstate->att_conv_metadata);
		req = async_request_send_prepared_stmt_with_params(fdw_server->p_stmt, params, type);

		Assert(NULL != req);

		async_request_attach_user_data(req, fdw_server);
		async_request_set_add(reqset, req);
	}

	while ((rsp = async_request_set_wait_any_result(reqset)))
	{
		PGresult *res = async_response_result_get_pg_result(rsp);
		TsFdwServerState *fdw_server = async_response_result_get_user_data(rsp);

		if (PQresultStatus(res) != (fmstate->has_returning ? PGRES_TUPLES_OK : PGRES_COMMAND_OK))
			remote_connection_report_error(ERROR, res, fdw_server->conn, false, fmstate->query);

		/*
		 * If we update multiple replica chunks, we should only return the
		 * results from the first one.
		 */
		if (n_rows == -1)
		{
			/* Check number of rows affected, and fetch RETURNING tuple if any */
			if (fmstate->has_returning)
			{
				n_rows = PQntuples(res);

				if (n_rows > 0)
					store_returning_result(fmstate, slot, res);
			}
			else
				n_rows = atoi(PQcmdTuples(res));
		}

		/* And clean up */
		async_response_result_close(rsp);
	}

	/*
	 * Currently no way to do a deep cleanup of all request in the request
	 * set. The worry here is that since this runs in a per-chunk insert state
	 * memory context, the async API will accumulate a lot of cruft during
	 * inserts
	 */
	pfree(reqset);
	stmt_params_reset(params);

	MemoryContextReset(fmstate->temp_cxt);

	/* Return NULL if nothing was updated on the remote end */
	return (n_rows > 0) ? slot : NULL;
}

static TupleTableSlot *
exec_foreign_update(EState *estate, ResultRelInfo *rri, TupleTableSlot *slot,
					TupleTableSlot *plan_slot)
{
	return exec_foreign_update_or_delete(estate, rri, slot, plan_slot, UPDATE_CMD);
}

static TupleTableSlot *
exec_foreign_delete(EState *estate, ResultRelInfo *rri, TupleTableSlot *slot,
					TupleTableSlot *plan_slot)
{
	return exec_foreign_update_or_delete(estate, rri, slot, plan_slot, DELETE_CMD);
}

/*
 * finish_foreign_modify
 *		Release resources for a foreign insert/update/delete operation
 */
static void
finish_foreign_modify(TsFdwModifyState *fmstate)
{
	int i;

	Assert(fmstate != NULL);

	for (i = 0; i < fmstate->num_servers; i++)
	{
		TsFdwServerState *fdw_server = &fmstate->servers[i];

		/* If we created a prepared statement, destroy it */
		if (NULL != fdw_server->p_stmt)
		{
			prepared_stmt_close(fdw_server->p_stmt);
			fdw_server->p_stmt = NULL;
		}

		fdw_server->conn = NULL;
	}

	stmt_params_free(fmstate->stmt_params);
}

static void
end_foreign_modify(EState *estate, ResultRelInfo *rri)
{
	TsFdwModifyState *fmstate = (TsFdwModifyState *) rri->ri_FdwState;

	/* If fmstate is NULL, we are in EXPLAIN; nothing to do */
	if (fmstate == NULL)
		return;

	/* Destroy the execution state */
	finish_foreign_modify(fmstate);
}

static int
is_foreign_rel_updatable(Relation rel)
{
	return (1 << CMD_INSERT) | (1 << CMD_DELETE) | (1 << CMD_UPDATE);
}

static void
explain_foreign_scan(ForeignScanState *node, struct ExplainState *es)
{
	List *fdw_private;
	char *sql;
	char *relations;

	fdw_private = ((ForeignScan *) node->ss.ps.plan)->fdw_private;

	/*
	 * Add names of relation handled by the foreign scan when the scan is an
	 * upper rel.
	 */
	if (list_length(fdw_private) > FdwScanPrivateRelations)
	{
		relations = strVal(list_nth(fdw_private, FdwScanPrivateRelations));
		ExplainPropertyText("Relations", relations, es);
	}

	/*
	 * Add remote query, server name, and chunks when VERBOSE option is specified.
	 */
	if (es->verbose)
	{
		Oid server_id = intVal(list_nth(fdw_private, FdwScanPrivateServerId));
		ForeignServer *server = GetForeignServer(server_id);
		List *chunk_oids = (List *) list_nth(fdw_private, FdwScanPrivateChunkOids);

		ExplainPropertyText("Server", server->servername, es);

		if (chunk_oids != NIL)
		{
			StringInfoData chunk_names;
			ListCell *lc;
			bool first = true;

			initStringInfo(&chunk_names);
			foreach (lc, chunk_oids)
			{
				if (!first)
					appendStringInfoString(&chunk_names, ", ");
				else
					first = false;
				appendStringInfoString(&chunk_names, get_rel_name(lfirst_oid(lc)));
			}
			ExplainPropertyText("Chunks", chunk_names.data, es);
		}

		sql = strVal(list_nth(fdw_private, FdwScanPrivateSelectSql));
		ExplainPropertyText("Remote SQL", sql, es);
	}
}

static void
explain_foreign_modify(ModifyTableState *mtstate, ResultRelInfo *rri, List *fdw_private,
					   int subplan_index, struct ExplainState *es)
{
	if (es->verbose)
	{
		char *sql = strVal(list_nth(fdw_private, FdwModifyPrivateUpdateSql));

		ExplainPropertyText("Remote SQL", sql, es);
	}
}

static bool
analyze_foreign_table(Relation relation, AcquireSampleRowsFunc *func, BlockNumber *totalpages)
{
	return false;
}

static FdwRoutine timescaledb_fdw_routine = {
	.type = T_FdwRoutine,
	/* scan (mandatory) */
	.GetForeignPaths = get_foreign_paths,
	.GetForeignRelSize = get_foreign_rel_size,
	.GetForeignPlan = get_foreign_plan,
	.BeginForeignScan = begin_foreign_scan,
	.IterateForeignScan = iterate_foreign_scan,
	.EndForeignScan = end_foreign_scan,
	.ReScanForeignScan = rescan_foreign_scan,
	/* update */
	.IsForeignRelUpdatable = is_foreign_rel_updatable,
	.PlanForeignModify = plan_foreign_modify,
	.BeginForeignModify = begin_foreign_modify,
	.ExecForeignInsert = exec_foreign_insert,
	.ExecForeignDelete = exec_foreign_delete,
	.ExecForeignUpdate = exec_foreign_update,
	.EndForeignModify = end_foreign_modify,
	.AddForeignUpdateTargets = add_foreign_update_targets,
	/* explain/analyze */
	.ExplainForeignScan = explain_foreign_scan,
	.ExplainForeignModify = explain_foreign_modify,
	.AnalyzeForeignTable = analyze_foreign_table,
};

TS_FUNCTION_INFO_V1(timescaledb_fdw_handler);

Datum
timescaledb_fdw_handler(PG_FUNCTION_ARGS)
{
	PG_RETURN_POINTER(&timescaledb_fdw_routine);
}

PG_FUNCTION_INFO_V1(timescaledb_fdw_validator);

Datum
timescaledb_fdw_validator(PG_FUNCTION_ARGS)
{
	List *options_list = untransformRelOptions(PG_GETARG_DATUM(0));
	Oid catalog = PG_GETARG_OID(1);

	option_validate(options_list, catalog);

	PG_RETURN_VOID();
}
