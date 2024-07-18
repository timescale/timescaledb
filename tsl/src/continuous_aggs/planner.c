/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>

#include <nodes/makefuncs.h>
#include <nodes/nodeFuncs.h>
#include <nodes/nodes.h>
#include <nodes/pg_list.h>
#include <parser/parse_func.h>

#include "continuous_aggs/common.h"
#include "planner.h"
#include "ts_catalog/continuous_aggs_watermark.h"

/*
 * The watermark function of a CAgg query is embedded into further functions. It
 * has the following structure:
 *
 * 1. a coalesce expression
 * 2. an optional to timestamp conversion function (date, timestamp, timestamptz)
 * 3. the actual watermark function
 *
 * For example:
 *        ... COALESCE(to_timestamp(cagg_watermark(59)), XXX) ...
 *        ... COALESCE(cagg_watermark(59), XXX) ...
 *
 * We use the following data structure while walking to the query to analyze the query
 * and collect references to the needed functions. The data structure contains:
 *
 *   (a) values (e.g., function Oids) which are needed to analyze the query
 *   (b) values that are changed during walking through the query
 *       (e.g., references to parent functions)
 *   (c) result data like the watermark functions and their parent functions
 */
typedef struct
{
	/* (a) Values initialized after creating the context */
	List *to_timestamp_func_oids; // List of Oids of the timestamp conversion functions

	/* (b) Values changed while walking through the query */
	CoalesceExpr *parent_coalesce_expr; // the current parent coalesce_expr
	FuncExpr *parent_to_timestamp_func; // the current parent timestamp function

	/* (c) Result values */
	List *watermark_parent_functions; // List of parent functions of a watermark (1) and (2)
	List *watermark_functions;		  // List of watermark functions (3)
	List *relids;					  // List of used relids by the query
	bool valid_query;				  // Is the query valid a valid CAgg query or not
} ConstifyWatermarkContext;

/* Oid of the watermark function. It can be stored into a static variable because it will not
 * change over the lifetime of a backend session, so we can lookup it only once.
 */
static Oid watermark_function_oid = InvalidOid;

/*
 * Walk through the elements of the query and detect the watermark functions and their
 * parent functions.
 */
static bool
constify_cagg_watermark_walker(Node *node, ConstifyWatermarkContext *context)
{
	if (node == NULL)
		return false;

	if (IsA(node, FuncExpr))
	{
		FuncExpr *funcExpr = castNode(FuncExpr, node);

		/* Handle watermark function */
		if (watermark_function_oid == funcExpr->funcid)
		{
			/* The watermark function takes exactly one argument */
			Assert(list_length(funcExpr->args) == 1);

			/* No coalesce expression found so far or function parameter is not constant, we are not
			 * interested in this expression */
			if (context->parent_coalesce_expr == NULL || !IsA(linitial(funcExpr->args), Const) ||
				(castNode(Const, linitial(funcExpr->args))->constisnull))
			{
				context->valid_query = false;
				return false;
			}

			context->watermark_functions = lappend(context->watermark_functions, funcExpr);

			/* Only on time based hypertables, we have a to_timestamp function */
			if (context->parent_to_timestamp_func != NULL)
			{
				/* to_timestamp functions take only one parameter. This should be a reference to our
				 * function */
				Assert(linitial(context->parent_to_timestamp_func->args) == node);

				context->watermark_parent_functions =
					lappend(context->watermark_parent_functions, context->parent_to_timestamp_func);
			}
			else
			{
				/* For non int64 partitioned tables, the watermark function is wrapped into a cast
				 * for example: COALESCE((_timescaledb_functions.cagg_watermark(11))::integer,
				 * '-2147483648'::integer))
				 */
				Node *coalesce_arg = linitial(context->parent_coalesce_expr->args);
				if (coalesce_arg != node)
				{
					/* Check if the watermark function is wrapped into a cast function */
					if (!IsA(coalesce_arg, FuncExpr) || ((FuncExpr *) coalesce_arg)->args == NIL ||
						linitial(((FuncExpr *) coalesce_arg)->args) != node)
					{
						context->valid_query = false;
						return false;
					}

					context->watermark_parent_functions =
						lappend(context->watermark_parent_functions, coalesce_arg);
				}
				else
				{
					context->watermark_parent_functions =
						lappend(context->watermark_parent_functions, context->parent_coalesce_expr);
				}
			}
		}

		/* Capture the timestamp conversion function */
		if (list_member_oid(context->to_timestamp_func_oids, funcExpr->funcid))
		{
			FuncExpr *old_func_expr = context->parent_to_timestamp_func;
			context->parent_to_timestamp_func = funcExpr;
			bool result = expression_tree_walker(node, constify_cagg_watermark_walker, context);
			context->parent_to_timestamp_func = old_func_expr;

			return result;
		}
	}
	else if (IsA(node, Query))
	{
		/* Recurse into subselects */
		Query *query = castNode(Query, node);
		return query_tree_walker(query,
								 constify_cagg_watermark_walker,
								 context,
								 QTW_EXAMINE_RTES_BEFORE);
	}
	else if (IsA(node, CoalesceExpr))
	{
		/* Capture the CoalesceExpr */
		CoalesceExpr *parent_coalesce_expr = context->parent_coalesce_expr;
		context->parent_coalesce_expr = castNode(CoalesceExpr, node);
		bool result = expression_tree_walker(node, constify_cagg_watermark_walker, context);
		context->parent_coalesce_expr = parent_coalesce_expr;

		return result;
	}
	else if (IsA(node, RangeTblEntry))
	{
		/* Collect the Oid of the used range tables */
		RangeTblEntry *rte = (RangeTblEntry *) node;

		if (rte->rtekind == RTE_RELATION)
		{
			context->relids = list_append_unique_oid(context->relids, rte->relid);
		}

		/* allow range_table_walker to continue */
		return false;
	}

	return expression_tree_walker(node, constify_cagg_watermark_walker, context);
}

/*
 * The entry of the watermark HTAB.
 */
typedef struct WatermarkConstEntry
{
	int32 key;
	Const *watermark_constant;
} WatermarkConstEntry;

/* The query can contain multiple watermarks (i.e., two hierarchal real-time CAggs)
 * We maintain a hash map (hypertable id -> constant) to ensure we use the same constant
 * for the same watermark across the while query.
 */
static HTAB *pg_nodiscard
init_watermark_map()
{
	struct HASHCTL hctl = {
		.keysize = sizeof(int32),
		.entrysize = sizeof(WatermarkConstEntry),
		.hcxt = CurrentMemoryContext,
	};

	/* Use 4 initial elements to have enough space for normal and hierarchical CAggs */
	return hash_create("Watermark const values", 4, &hctl, HASH_ELEM | HASH_CONTEXT | HASH_BLOBS);
}

/*
 * Get a constant value for our watermark function. The constant is cached
 * in a hash map to ensure we use the same constant for invocations of the
 * watermark function with the same parameter across the whole query.
 */
static Const *
get_watermark_const(HTAB *watermarks, int32 watermark_hypertable_id, List *range_table_oids)
{
	bool found;
	WatermarkConstEntry *watermark_const =
		hash_search(watermarks, &watermark_hypertable_id, HASH_ENTER, &found);

	if (!found)
	{
		/*
		 * Check that the argument of the watermark function is also a range table of the query. We
		 * only constify the value when this condition is true. Only in this case, the query will be
		 * removed from the query cache by PostgreSQL when an invalidation for the watermark
		 * hypertable is processed (see CacheInvalidateRelcacheByRelid).
		 */
		Oid ht_relid = ts_hypertable_id_to_relid(watermark_hypertable_id, false);

		/* Given table is not a part of our range tables */
		if (!list_member_oid(range_table_oids, ht_relid))
		{
			watermark_const->watermark_constant = NULL;
			return NULL;
		}

		/* Not found, create a new constant */
		int64 watermark = ts_cagg_watermark_get(watermark_hypertable_id);
		Const *const_watermark = makeConst(INT8OID,
										   -1,
										   InvalidOid,
										   sizeof(int64),
										   Int64GetDatum(watermark),
										   false,
										   FLOAT8PASSBYVAL);
		watermark_const->watermark_constant = const_watermark;
	}

	return watermark_const->watermark_constant;
}

/*
 * Replace the collected references to the watermark function in the context variable
 * with constant values.
 */
static void
replace_watermark_with_const(ConstifyWatermarkContext *context)
{
	Assert(context != NULL);
	Assert(context->valid_query);

	/* We need to have at least one watermark value */
	if (list_length(context->watermark_functions) < 1)
		return;

	HTAB *watermarks = init_watermark_map();

	/* The list of watermark function should have the same length as the parent functions. In
	 * other words, each watermark function should have exactly one parent function. */
	Assert(list_length(context->watermark_parent_functions) ==
		   list_length(context->watermark_functions));

	/* Iterate over the function parents and the actual watermark functions. Get a
	 * const value for each function and replace the reference to the watermark function
	 * in the function parent.
	 */
	ListCell *parent_lc, *watermark_lc;
	forboth (parent_lc,
			 context->watermark_parent_functions,
			 watermark_lc,
			 context->watermark_functions)
	{
		FuncExpr *watermark_function = lfirst(watermark_lc);
		Assert(watermark_function_oid == watermark_function->funcid);
		Const *arg = (Const *) linitial(watermark_function->args);
		int32 watermark_hypertable_id = DatumGetInt32(arg->constvalue);

		Const *watermark_const =
			get_watermark_const(watermarks, watermark_hypertable_id, context->relids);

		/* No constant created, it means the hypertable id used by the watermark function is not a
		 * range table and no invalidations would be processed. So, not replacing the function
		 * invocation. */
		if (watermark_const == NULL)
			continue;

		/* Replace cagg_watermark FuncExpr node by a Const node */
		if (IsA(lfirst(parent_lc), FuncExpr))
		{
			FuncExpr *parent_func_expr = castNode(FuncExpr, lfirst(parent_lc));
			linitial(parent_func_expr->args) = (Node *) watermark_const;
		}
		else
		{
			/* Check that the assumed parent function is our parent function */
			CoalesceExpr *parent_coalesce_expr = castNode(CoalesceExpr, lfirst(parent_lc));
			linitial(parent_coalesce_expr->args) = (Node *) watermark_const;
		}
	}

	/* Clean up the hash map */
	hash_destroy(watermarks);
}

/*
 * Constify all references to the CAgg watermark function if the query is a union query on a CAgg
 */
void
constify_cagg_watermark(Query *parse)
{
	if (parse == NULL)
		return;

	/* process only SELECT queries */
	if (parse->commandType != CMD_SELECT)
		return;

	Node *node = (Node *) parse;

	ConstifyWatermarkContext context = { 0 };
	context.valid_query = true;

	if (!OidIsValid(watermark_function_oid))
	{
		watermark_function_oid = get_watermark_function_oid();

		Ensure(OidIsValid(watermark_function_oid), "unable to determine watermark function Oid");
	}

	/* Get Oid of all used timestamp converter functions.
	 *
	 * The watermark function can be invoked by a timestamp conversion function.
	 * For example: to_timestamp(cagg_watermark(XX)). We collect the Oid of all these
	 * converter functions in the list to_timestamp_func_oids.
	 */
	context.to_timestamp_func_oids = NIL;

	context.to_timestamp_func_oids =
		lappend_oid(context.to_timestamp_func_oids, cagg_get_boundary_converter_funcoid(DATEOID));

	context.to_timestamp_func_oids = lappend_oid(context.to_timestamp_func_oids,
												 cagg_get_boundary_converter_funcoid(TIMESTAMPOID));

	context.to_timestamp_func_oids =
		lappend_oid(context.to_timestamp_func_oids,
					cagg_get_boundary_converter_funcoid(TIMESTAMPTZOID));

	/* Walk through the query and collect function information */
	constify_cagg_watermark_walker(node, &context);

	/* Replace watermark functions with const value if the query might belong to the CAgg query */
	if (context.valid_query)
		replace_watermark_with_const(&context);
}
