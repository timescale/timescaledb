/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

#include <postgres.h>
#include <fmgr.h>
#include <access/htup_details.h>
#include <catalog/dependency.h>
#include <catalog/namespace.h>
#include <catalog/pg_type.h>
#include <catalog/pg_trigger.h>
#include <commands/trigger.h>
#include <storage/lmgr.h>
#include <utils/builtins.h>
#include <utils/lsyscache.h>
#include <parser/parser.h>

#include "compat/compat.h"

#include "compression_with_clause.h"

static const WithClauseDefinition compress_hypertable_with_clause_def[] = {
		[CompressEnabled] = {
			.arg_name = "compress",
			.type_id = BOOLOID,
			.default_val = (Datum)false,
		},
		[CompressSegmentBy] = {
			 .arg_name = "compress_segmentby",
			 .type_id = TEXTOID,
		},
		[CompressOrderBy] = {
			 .arg_name = "compress_orderby",
			 .type_id = TEXTOID,
		},
		[CompressChunkTimeInterval] = {
			 .arg_name = "compress_chunk_time_interval",
			 .type_id = INTERVALOID,
		},
};

WithClauseResult *
ts_compress_hypertable_set_clause_parse(const List *defelems)
{
	return ts_with_clauses_parse(defelems,
								 compress_hypertable_with_clause_def,
								 TS_ARRAY_LEN(compress_hypertable_with_clause_def));
}

static inline void
throw_segment_by_error(char *segment_by)
{
	ereport(ERROR,
			(errcode(ERRCODE_SYNTAX_ERROR),
			 errmsg("unable to parse segmenting option \"%s\"", segment_by),
			 errhint("The option timescaledb.compress_segmentby must"
					 " be a set of columns separated by commas.")));
}

static bool
select_stmt_as_expected(SelectStmt *stmt)
{
	/* The only parts of the select stmt that are allowed to be set are the order by or group by.
	 * Check that no other fields are set */
	if (stmt->distinctClause != NIL || stmt->intoClause != NULL || stmt->targetList != NIL ||
		stmt->whereClause != NULL || stmt->havingClause != NULL || stmt->windowClause != NIL ||
		stmt->valuesLists != NULL || stmt->limitOffset != NULL || stmt->limitCount != NULL ||
		stmt->lockingClause != NIL || stmt->withClause != NULL || stmt->op != 0 ||
		stmt->all != false || stmt->larg != NULL || stmt->rarg != NULL)
		return false;
	return true;
}

static List *
parse_segment_collist(char *inpstr, Hypertable *hypertable)
{
	StringInfoData buf;
	List *parsed;
	ListCell *lc;
	SelectStmt *select;
	RawStmt *raw;

	if (strlen(inpstr) == 0)
		return NIL;

	initStringInfo(&buf);

	/* parse the segment by list exactly how you would a group by */
	appendStringInfo(&buf,
					 "SELECT FROM %s.%s GROUP BY %s",
					 quote_identifier(hypertable->fd.schema_name.data),
					 quote_identifier(hypertable->fd.table_name.data),
					 inpstr);

	PG_TRY();
	{
		parsed = raw_parser_compat(buf.data);
	}
	PG_CATCH();
	{
		throw_segment_by_error(inpstr);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (list_length(parsed) != 1)
		throw_segment_by_error(inpstr);
	if (!IsA(linitial(parsed), RawStmt))
		throw_segment_by_error(inpstr);
	raw = linitial(parsed);

	if (!IsA(raw->stmt, SelectStmt))
		throw_segment_by_error(inpstr);
	select = (SelectStmt *) raw->stmt;

	if (!select_stmt_as_expected(select))
		throw_segment_by_error(inpstr);

	if (select->sortClause != NIL)
		throw_segment_by_error(inpstr);

	List *collist = NIL;
	short index = 0;
	foreach (lc, select->groupClause)
	{
		ColumnRef *cf;
		CompressedParsedCol *col = (CompressedParsedCol *) palloc(sizeof(*col));

		if (!IsA(lfirst(lc), ColumnRef))
			throw_segment_by_error(inpstr);
		cf = lfirst(lc);
		if (list_length(cf->fields) != 1)
			throw_segment_by_error(inpstr);

		if (!IsA(linitial(cf->fields), String))
			throw_segment_by_error(inpstr);

		col->index = index;
		index++;
		namestrcpy(&col->colname, strVal(linitial(cf->fields)));
		collist = lappend(collist, (void *) col);
	}

	return collist;
}

static inline void
throw_order_by_error(char *order_by)
{
	ereport(ERROR,
			(errcode(ERRCODE_SYNTAX_ERROR),
			 errmsg("unable to parse ordering option \"%s\"", order_by),
			 errhint("The timescaledb.compress_orderby option must be a set of column"
					 " names with sort options, separated by commas."
					 " It is the same format as an ORDER BY clause.")));
}

/* compress_orderby is parsed same as order by in select queries */
static List *
parse_order_collist(char *inpstr, Hypertable *hypertable)
{
	StringInfoData buf;
	List *parsed;
	ListCell *lc;
	SelectStmt *select;
	RawStmt *raw;

	if (strlen(inpstr) == 0)
		return NIL;

	initStringInfo(&buf);

	/* parse the segment by list exactly how you would a order by by */
	appendStringInfo(&buf,
					 "SELECT FROM %s.%s ORDER BY %s",
					 quote_identifier(hypertable->fd.schema_name.data),
					 quote_identifier(hypertable->fd.table_name.data),
					 inpstr);

	PG_TRY();
	{
		parsed = raw_parser_compat(buf.data);
	}
	PG_CATCH();
	{
		throw_order_by_error(inpstr);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (list_length(parsed) != 1)
		throw_order_by_error(inpstr);
	if (!IsA(linitial(parsed), RawStmt))
		throw_order_by_error(inpstr);
	raw = linitial(parsed);
	if (!IsA(raw->stmt, SelectStmt))
		throw_order_by_error(inpstr);
	select = (SelectStmt *) raw->stmt;

	if (!select_stmt_as_expected(select))
		throw_order_by_error(inpstr);

	if (select->groupClause != NIL)
		throw_order_by_error(inpstr);

	List *collist = NIL;
	short index = 0;
	foreach (lc, select->sortClause)
	{
		SortBy *sort_by;
		ColumnRef *cf;
		CompressedParsedCol *col = (CompressedParsedCol *) palloc(sizeof(*col));

		if (!IsA(lfirst(lc), SortBy))
			throw_order_by_error(inpstr);
		sort_by = lfirst(lc);

		if (!IsA(sort_by->node, ColumnRef))
			throw_order_by_error(inpstr);
		cf = (ColumnRef *) sort_by->node;

		if (list_length(cf->fields) != 1)
			throw_order_by_error(inpstr);

		if (!IsA(linitial(cf->fields), String))
			throw_order_by_error(inpstr);

		col->index = index;
		index++;
		namestrcpy(&col->colname, strVal(linitial(cf->fields)));

		if (sort_by->sortby_dir != SORTBY_ASC && sort_by->sortby_dir != SORTBY_DESC &&
			sort_by->sortby_dir != SORTBY_DEFAULT)
			throw_order_by_error(inpstr);
		col->asc = sort_by->sortby_dir == SORTBY_ASC || sort_by->sortby_dir == SORTBY_DEFAULT;

		if (sort_by->sortby_nulls == SORTBY_NULLS_DEFAULT)
		{
			/* default null ordering is LAST for ASC, FIRST for DESC */
			col->nullsfirst = !col->asc;
		}
		else
		{
			col->nullsfirst = sort_by->sortby_nulls == SORTBY_NULLS_FIRST;
		}

		collist = lappend(collist, (void *) col);
	}

	return collist;
}

/* returns List of CompressedParsedCol
 * compress_segmentby = `col1,col2,col3`
 */
List *
ts_compress_hypertable_parse_segment_by(WithClauseResult *parsed_options, Hypertable *hypertable)
{
	if (parsed_options[CompressSegmentBy].is_default == false)
	{
		Datum textarg = parsed_options[CompressSegmentBy].parsed;
		return parse_segment_collist(TextDatumGetCString(textarg), hypertable);
	}
	else
		return NIL;
}

/* returns List of CompressedParsedCol
 * E.g. timescaledb.compress_orderby = 'col1 asc nulls first,col2 desc,col3'
 */
List *
ts_compress_hypertable_parse_order_by(WithClauseResult *parsed_options, Hypertable *hypertable)
{
	if (parsed_options[CompressOrderBy].is_default == false)
	{
		Datum textarg = parsed_options[CompressOrderBy].parsed;
		return parse_order_collist(TextDatumGetCString(textarg), hypertable);
	}
	else
		return NIL;
}

/* returns List of CompressedParsedCol
 * E.g. timescaledb.compress_orderby = 'col1 asc nulls first,col2 desc,col3'
 */
Interval *
ts_compress_hypertable_parse_chunk_time_interval(WithClauseResult *parsed_options,
												 Hypertable *hypertable)
{
	if (parsed_options[CompressChunkTimeInterval].is_default == false)
	{
		Datum textarg = parsed_options[CompressChunkTimeInterval].parsed;
		return DatumGetIntervalP(textarg);
	}
	else
		return NULL;
}
