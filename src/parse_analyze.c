#include <postgres.h>
#include <nodes/nodeFuncs.h>
#include <parser/analyze.h>

#include "cache.h"
#include "hypertable.h"
#include "hypertable_cache.h"
#include "extension.h"
#include "parse_rewrite.h"

void		_parse_analyze_init(void);
void		_parse_analyze_fini(void);

static post_parse_analyze_hook_type prev_post_parse_analyze_hook = NULL;

typedef struct HypertableQueryCtx
{
	Query	   *parse;
	Query	   *parent;
	CmdType		cmdtype;
	Cache	   *hcache;
	Hypertable *hentry;
} HypertableQueryCtx;

/*
 * Identify queries on a hypertable by walking the query tree. If the query is
 * indeed on a hypertable, setup the necessary state and/or make modifications
 * to the query tree.
 */
static bool
hypertable_query_walker(Node *node, void *context)
{
	if (node == NULL)
		return false;

	if (IsA(node, RangeTblEntry))
	{
		RangeTblEntry *rte = (RangeTblEntry *) node;
		HypertableQueryCtx *ctx = (HypertableQueryCtx *) context;

		if (rte->rtekind == RTE_RELATION)
		{
			Hypertable *hentry = hypertable_cache_get_entry(ctx->hcache, rte->relid);

			if (hentry != NULL)
				ctx->hentry = hentry;
		}

		return false;
	}

	if (IsA(node, Query))
	{
		bool		result;
		HypertableQueryCtx *ctx = (HypertableQueryCtx *) context;
		CmdType		old = ctx->cmdtype;
		Query	   *query = (Query *) node;
		Query	   *oldparent = ctx->parent;

		/* adjust context */
		ctx->cmdtype = query->commandType;
		ctx->parent = query;

		result = query_tree_walker(ctx->parent, hypertable_query_walker,
								   context, QTW_EXAMINE_RTES);

		/* restore context */
		ctx->cmdtype = old;
		ctx->parent = oldparent;

		return result;
	}

	return expression_tree_walker(node, hypertable_query_walker, context);
}

static void
timescaledb_post_parse_analyze(ParseState *pstate, Query *query)
{
	if (NULL != prev_post_parse_analyze_hook)
		/* Call any earlier hooks */
		prev_post_parse_analyze_hook(pstate, query);

	if (extension_is_loaded())
	{
		HypertableQueryCtx context = {
			.parse = query,
			.parent = query,
			.cmdtype = query->commandType,
			.hcache = hypertable_cache_pin(),
			.hentry = NULL,
		};

		/* The query for explains is in the utility statement */
		if (query->commandType == CMD_UTILITY &&
			IsA(query->utilityStmt, ExplainStmt))
			query = (Query *) ((ExplainStmt *) query->utilityStmt)->query;

		hypertable_query_walker((Node *) query, &context);

		/* note assumes 1 hypertable per query */
		if (NULL != context.hentry)
			parse_rewrite_query(pstate, query, context.hentry);

		cache_release(context.hcache);
	}
}

void
_parse_analyze_init(void)
{
	prev_post_parse_analyze_hook = post_parse_analyze_hook;
	post_parse_analyze_hook = timescaledb_post_parse_analyze;
}

void
_parse_analyze_fini(void)
{
	post_parse_analyze_hook = prev_post_parse_analyze_hook;
	prev_post_parse_analyze_hook = NULL;
}
