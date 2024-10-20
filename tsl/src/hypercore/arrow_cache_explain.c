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

#define APPEND_IF_POSITIVE(INFO, FMT, VAL)                                                         \
	do                                                                                             \
	{                                                                                              \
		if ((VAL) > 0)                                                                             \
			appendStringInfo((INFO), " " FMT, (long long) (VAL));                                  \
	} while (0)

static void
explain_decompression(Query *query, int cursorOptions, IntoClause *into, ExplainState *es,
					  const char *queryString, ParamListInfo params, QueryEnvironment *queryEnv)
{
	standard_ExplainOneQuery(query, cursorOptions, into, es, queryString, params, queryEnv);
	if (decompress_cache_print)
	{
		const bool has_decompress_data = decompress_cache_stats.decompressions > 0 ||
										 decompress_cache_stats.decompress_calls > 0;
		const bool has_cache_data = decompress_cache_stats.hits > 0 ||
									decompress_cache_stats.misses > 0 ||
									decompress_cache_stats.evictions > 0;
		if (has_decompress_data || has_cache_data)
		{
			if (es->format == EXPLAIN_FORMAT_TEXT)
			{
				appendStringInfoString(es->str, "Array:");
				if (has_cache_data)
					appendStringInfoString(es->str, " cache");
				APPEND_IF_POSITIVE(es->str, "hits=%lld", decompress_cache_stats.hits);
				APPEND_IF_POSITIVE(es->str, "misses=%lld", decompress_cache_stats.misses);
				APPEND_IF_POSITIVE(es->str, "evictions=%lld", decompress_cache_stats.evictions);
				if (has_decompress_data)
					appendStringInfoString(es->str, ", decompress");
				APPEND_IF_POSITIVE(es->str, "count=%lld", decompress_cache_stats.decompressions);
				APPEND_IF_POSITIVE(es->str, "calls=%lld", decompress_cache_stats.decompress_calls);
				appendStringInfoChar(es->str, '\n');
			}
			else
			{
				ExplainOpenGroup("Array Cache", "Arrow Array Cache", true, es);
				ExplainPropertyInteger("hits", NULL, decompress_cache_stats.hits, es);
				ExplainPropertyInteger("misses", NULL, decompress_cache_stats.misses, es);
				ExplainPropertyInteger("evictions", NULL, decompress_cache_stats.evictions, es);
				ExplainCloseGroup("Array Cache", "Arrow Array Cache", true, es);

				ExplainOpenGroup("Array Decompress", "Arrow Array Decompress", true, es);
				ExplainPropertyInteger("count", NULL, decompress_cache_stats.decompressions, es);
				if (es->verbose)
					ExplainPropertyInteger("calls",
										   NULL,
										   decompress_cache_stats.decompress_calls,
										   es);
				ExplainCloseGroup("Array Decompress", "Arrow Array Decompress", true, es);
			}
		}

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
