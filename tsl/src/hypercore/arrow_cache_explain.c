/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <postgres.h>

#include <commands/defrem.h>
#include <commands/explain.h>
#include <tcop/tcopprot.h>
#include <utils/varlena.h>

#include <compat/compat.h>
#include "arrow_cache_explain.h"

bool decompress_cache_print = false;
struct DecompressCacheStats decompress_cache_stats;
static ExplainOneQuery_hook_type prev_ExplainOneQuery_hook = NULL;

#if PG17_LT
/*
 * Copied from backend/commands/explain.c since there is no such function for
 * ExplainOneQuery. Also using the naming convention for other similar
 * functions, such as standard_ExecutorStart, hence the weird case usage.
 */
static void
standard_ExplainOneQuery(Query *query, int cursorOptions, IntoClause *into, ExplainState *es,
						 const char *queryString, ParamListInfo params, QueryEnvironment *queryEnv)
{
	PlannedStmt *plan;
	instr_time planstart, planduration;
	BufferUsage bufusage_start, bufusage;

	if (es->buffers)
		bufusage_start = pgBufferUsage;
	INSTR_TIME_SET_CURRENT(planstart);

	/* plan the query */
	plan = pg_plan_query(query, queryString, cursorOptions, params);

	INSTR_TIME_SET_CURRENT(planduration);
	INSTR_TIME_SUBTRACT(planduration, planstart);

	/* calc differences of buffer counters. */
	if (es->buffers)
	{
		memset(&bufusage, 0, sizeof(BufferUsage));
		BufferUsageAccumDiff(&bufusage, &pgBufferUsage, &bufusage_start);
	}

	/* run it (if needed) and produce output */
	ExplainOnePlan(plan,
				   into,
				   es,
				   queryString,
				   params,
				   queryEnv,
				   &planduration,
				   (es->buffers ? &bufusage : NULL));
}
#endif

static struct
{
	const char *hits_text;			   /* Number of cache hits */
	const char *miss_text;			   /* Number of cache misses */
	const char *evict_text;			   /* Number of cache evictions */
	const char *decompress_text;	   /* Number of arrays decompressed */
	const char *decompress_calls_text; /* Number of calls to decompress an array */
} format_texts[] = {
	[EXPLAIN_FORMAT_TEXT] = {
		.hits_text = "Array Cache Hits",
		.miss_text = "Array Cache Misses",
		.evict_text = "Array Cache Evictions",
		.decompress_text = "Array Decompressions",
		.decompress_calls_text = "Array Decompression Calls",
	},
	[EXPLAIN_FORMAT_XML]= {
		.hits_text = "hits",
		.miss_text = "misses",
		.evict_text = "evictions",
		.decompress_text = "decompressions",
		.decompress_calls_text = "decompression calls",
	},
	[EXPLAIN_FORMAT_JSON] = {
		.hits_text = "hits",
		.miss_text = "misses",
		.evict_text = "evictions",
		.decompress_text = "decompressions",
		.decompress_calls_text = "decompression calls",
	},
	[EXPLAIN_FORMAT_YAML] = {
		.hits_text = "hits",
		.miss_text = "misses",
		.evict_text = "evictions",
		.decompress_text = "decompressions",
		.decompress_calls_text = "decompression calls",
	},
};

static void
explain_decompression(Query *query, int cursorOptions, IntoClause *into, ExplainState *es,
					  const char *queryString, ParamListInfo params, QueryEnvironment *queryEnv)
{
	standard_ExplainOneQuery(query, cursorOptions, into, es, queryString, params, queryEnv);
	if (decompress_cache_print)
	{
		Assert(es->format < sizeof(format_texts) / sizeof(*format_texts));

		ExplainOpenGroup("Array cache", "Arrow Array Cache", true, es);
		ExplainPropertyInteger(format_texts[es->format].hits_text,
							   NULL,
							   decompress_cache_stats.hits,
							   es);
		ExplainPropertyInteger(format_texts[es->format].miss_text,
							   NULL,
							   decompress_cache_stats.misses,
							   es);
		ExplainPropertyInteger(format_texts[es->format].evict_text,
							   NULL,
							   decompress_cache_stats.evictions,
							   es);
		ExplainPropertyInteger(format_texts[es->format].decompress_text,
							   NULL,
							   decompress_cache_stats.decompressions,
							   es);

		if (es->verbose)
			ExplainPropertyInteger(format_texts[es->format].decompress_calls_text,
								   NULL,
								   decompress_cache_stats.decompress_calls,
								   es);

		ExplainCloseGroup("Array cache", "Arrow Array Cache", true, es);

		decompress_cache_print = false;
		memset(&decompress_cache_stats, 0, sizeof(struct DecompressCacheStats));
	}
}

bool
tsl_process_explain_def(DefElem *opt)
{
	if (strcmp(opt->defname, "decompress_cache_stats") == 0)
	{
		decompress_cache_print = defGetBoolean(opt);
		return true; /* Remove this option as processed and used */
	}
	return false; /* Keep this option  */
}

void
_arrow_cache_explain_init(void)
{
	prev_ExplainOneQuery_hook = ExplainOneQuery_hook;
	ExplainOneQuery_hook = explain_decompression;
}
