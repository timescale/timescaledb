/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include "analyze.h"

#include <postgres.h>

#include <access/htup_details.h>
#include <commands/vacuum.h>
#include <nodes/nodeFuncs.h>
#include <parser/parse_relation.h>
#include <utils/rel.h>
#include <utils/sampling.h>
#include <utils/syscache.h>

#include "compat/compat.h"

/*
 * examine_attribute -- pre-analysis of a single column
 *
 * Determine whether the column is analyzable; if so, create and initialize
 * a VacAttrStats struct for it.  If not, return NULL.
 *
 * If index_expr isn't NULL, then we're trying to analyze an expression index,
 * and index_expr is the expression tree representing the column's data.
 *
 * This is copied from analyze.c in PG17 since neither of the functions in
 * PostgreSQL 17 have external linkage and are needed to set up a ReadStream
 * for the non-compressed and compressed relations, hence cannot be referred
 * to from this code. They have been slightly simplified since we do not need
 * the same functionality as PostgreSQL uses for tables.
 */
static VacAttrStats *
examine_attribute(Relation onerel, int attnum, Node *index_expr, MemoryContext anl_context)
{
	Form_pg_attribute attr = TupleDescAttr(onerel->rd_att, attnum - 1);
	int attstattarget;
	HeapTuple atttuple;
	Datum dat;
	bool isnull;
	HeapTuple typtuple;
	VacAttrStats *stats;
	int i;
	bool ok;

	/* Never analyze dropped columns */
	if (attr->attisdropped)
		return NULL;

	/*
	 * Get attstattarget value.  Set to -1 if null.  (Analyze functions expect
	 * -1 to mean use default_statistics_target; see for example
	 * std_typanalyze.)
	 */
	atttuple =
		SearchSysCache2(ATTNUM, ObjectIdGetDatum(RelationGetRelid(onerel)), Int16GetDatum(attnum));
	if (!HeapTupleIsValid(atttuple))
		elog(ERROR,
			 "cache lookup failed for attribute %d of relation %u",
			 attnum,
			 RelationGetRelid(onerel));
	dat = SysCacheGetAttr(ATTNUM, atttuple, Anum_pg_attribute_attstattarget, &isnull);
	attstattarget = isnull ? -1 : DatumGetInt16(dat);
	ReleaseSysCache(atttuple);

	/* Don't analyze column if user has specified not to */
	if (attstattarget == 0)
		return NULL;

	/*
	 * Create the VacAttrStats struct.
	 */
	stats = (VacAttrStats *) palloc0(sizeof(VacAttrStats));
	stats->attstattarget = attstattarget;

	/*
	 * When analyzing an expression index, believe the expression tree's type
	 * not the column datatype --- the latter might be the opckeytype storage
	 * type of the opclass, which is not interesting for our purposes.  (Note:
	 * if we did anything with non-expression index columns, we'd need to
	 * figure out where to get the correct type info from, but for now that's
	 * not a problem.)	It's not clear whether anyone will care about the
	 * typmod, but we store that too just in case.
	 */
	if (index_expr)
	{
		stats->attrtypid = exprType(index_expr);
		stats->attrtypmod = exprTypmod(index_expr);

		/*
		 * If a collation has been specified for the index column, use that in
		 * preference to anything else; but if not, fall back to whatever we
		 * can get from the expression.
		 */
		if (OidIsValid(onerel->rd_indcollation[attnum - 1]))
			stats->attrcollid = onerel->rd_indcollation[attnum - 1];
		else
			stats->attrcollid = exprCollation(index_expr);
	}
	else
	{
		stats->attrtypid = attr->atttypid;
		stats->attrtypmod = attr->atttypmod;
		stats->attrcollid = attr->attcollation;
	}

	typtuple = SearchSysCacheCopy1(TYPEOID, ObjectIdGetDatum(stats->attrtypid));
	if (!HeapTupleIsValid(typtuple))
		elog(ERROR, "cache lookup failed for type %u", stats->attrtypid);
	stats->attrtype = (Form_pg_type) GETSTRUCT(typtuple);
	stats->anl_context = anl_context;
	stats->tupattnum = attnum;

	/*
	 * The fields describing the stats->stavalues[n] element types default to
	 * the type of the data being analyzed, but the type-specific typanalyze
	 * function can change them if it wants to store something else.
	 */
	for (i = 0; i < STATISTIC_NUM_SLOTS; i++)
	{
		stats->statypid[i] = stats->attrtypid;
		stats->statyplen[i] = stats->attrtype->typlen;
		stats->statypbyval[i] = stats->attrtype->typbyval;
		stats->statypalign[i] = stats->attrtype->typalign;
	}

	/*
	 * Call the type-specific typanalyze function.  If none is specified, use
	 * std_typanalyze().
	 */
	if (OidIsValid(stats->attrtype->typanalyze))
		ok = DatumGetBool(OidFunctionCall1(stats->attrtype->typanalyze, PointerGetDatum(stats)));
	else
		ok = std_typanalyze(stats);

	if (!ok || stats->compute_stats == NULL || stats->minrows <= 0)
	{
		heap_freetuple(typtuple);
		pfree(stats);
		return NULL;
	}

	return stats;
}

/*
 * Determine which columns to analyze.
 *
 * Note that system attributes are never analyzed, so we just reject them
 * at the lookup stage.  We also reject duplicate column mentions.  (We
 * could alternatively ignore duplicates, but analyzing a column twice
 * won't work; we'd end up making a conflicting update in pg_statistic.)
 *
 * This is copied from analyze.c and only collect attribute statistics for the
 * table attributes, not for index attributes.
 */
int
hypercore_analyze_compute_vacattrstats(Relation onerel, VacAttrStats ***vacattrstats_out,
									   MemoryContext mcxt)
{
	int tcnt, i, attr_cnt;
	VacAttrStats **vacattrstats;

	MemoryContext old_context = MemoryContextSwitchTo(mcxt);

	attr_cnt = onerel->rd_att->natts;
	vacattrstats = (VacAttrStats **) palloc(attr_cnt * sizeof(VacAttrStats *));
	tcnt = 0;
	for (i = 1; i <= attr_cnt; i++)
	{
		vacattrstats[tcnt] = examine_attribute(onerel, i, NULL, mcxt);
		if (vacattrstats[tcnt] != NULL)
			tcnt++;
	}
	attr_cnt = tcnt;

	*vacattrstats_out = vacattrstats;
	MemoryContextSwitchTo(old_context);
	return attr_cnt;
}

/* This function is copied from src/backend/commands/analyze.c */
BlockNumber
hypercore_block_sampling_read_stream_next(ReadStream *stream, void *callback_private_data,
										  void *per_buffer_data)
{
	BlockSamplerData *bs = callback_private_data;

	return BlockSampler_HasMore(bs) ? BlockSampler_Next(bs) : InvalidBlockNumber;
}
