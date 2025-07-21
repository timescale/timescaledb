/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

#include <postgres.h>
#include <access/htup_details.h>
#include <catalog/dependency.h>
#include <catalog/namespace.h>
#include <catalog/pg_trigger.h>
#include <catalog/pg_type.h>
#include <commands/trigger.h>
#include <fmgr.h>
#include <parser/parser.h>
#include <storage/lmgr.h>
#include <utils/builtins.h>
#include <utils/lsyscache.h>
#include <utils/typcache.h>

#include "compat/compat.h"
#include "ts_catalog/array_utils.h"

#include "alter_table_with_clause.h"

static const WithClauseDefinition alter_table_with_clause_def[] = {
		[AlterTableFlagChunkTimeInterval] = {
			.arg_names = {"chunk_interval", NULL},
			 .type_id = TEXTOID,
		},
		[AlterTableFlagColumnstore] = {
			.arg_names = {"compress", "columnstore", "enable_columnstore", NULL},
			.type_id = BOOLOID,
			.default_val = (Datum)false,
		},
		[AlterTableFlagSegmentBy] = {
			.arg_names = {"compress_segmentby", "segmentby", "segment_by", NULL},
			 .type_id = TEXTOID,
		},
		[AlterTableFlagOrderBy] = {
			.arg_names = {"compress_orderby", "orderby", "order_by", NULL},
			 .type_id = TEXTOID,
		},
		[AlterTableFlagCompressChunkTimeInterval] = {
			.arg_names = {"compress_chunk_interval", "compress_chunk_time_interval", NULL},
			 .type_id = INTERVALOID,
		},
};

WithClauseResult *
ts_alter_table_with_clause_parse(const List *defelems)
{
	return ts_with_clauses_parse(defelems,
								 alter_table_with_clause_def,
								 TS_ARRAY_LEN(alter_table_with_clause_def));
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

static ArrayType *
parse_segment_collist(char *inpstr, Hypertable *hypertable)
{
	StringInfoData buf;
	List *parsed;
	ListCell *lc;
	SelectStmt *select;
	RawStmt *raw;

	/* segmentby can have empty array */
	if (strlen(inpstr) == 0)
		return ts_array_add_element_text(NULL, NULL);

	initStringInfo(&buf);

	/* parse the segment by list exactly how you would a group by */
	appendStringInfo(&buf,
					 "SELECT FROM %s.%s GROUP BY %s",
					 quote_identifier(NameStr(hypertable->fd.schema_name)),
					 quote_identifier(NameStr(hypertable->fd.table_name)),
					 inpstr);

	PG_TRY();
	{
		parsed = raw_parser(buf.data, RAW_PARSE_DEFAULT);
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

	ArrayType *segmentby = NULL;
	foreach (lc, select->groupClause)
	{
		if (!IsA(lfirst(lc), ColumnRef))
			throw_segment_by_error(inpstr);

		ColumnRef *cf = lfirst(lc);
		if (list_length(cf->fields) != 1)
			throw_segment_by_error(inpstr);

		if (!IsA(linitial(cf->fields), String))
			throw_segment_by_error(inpstr);

		char *colname = strVal(linitial(cf->fields));
		AttrNumber col_attno = get_attnum(hypertable->main_table_relid, colname);
		if (col_attno == InvalidAttrNumber)
		{
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("column \"%s\" does not exist", colname),
					 errhint("The timescaledb.compress_segmentby option must reference a valid "
							 "column.")));
		}

		/* get normalized column name */
		colname = get_attname(hypertable->main_table_relid, col_attno, false);

		/* check if segmentby columns are distinct. */
		if (ts_array_is_member(segmentby, colname))
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("duplicate column name \"%s\"", colname),
					 errhint("The timescaledb.compress_segmentby option must reference distinct "
							 "column.")));

		segmentby = ts_array_add_element_text(segmentby, pstrdup(colname));
	}

	return segmentby;
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
OrderBySettings
ts_compress_parse_order_collist(char *inpstr, Hypertable *hypertable)
{
	StringInfoData buf;
	List *parsed;
	ListCell *lc;
	SelectStmt *select;
	RawStmt *raw;
	OrderBySettings settings = { 0 };

	if (strlen(inpstr) == 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("ordering column can not be empty"),
				 errhint("timescaledb.compress_orderby option must reference a valid "
						 "column or be removed to use default settings.")));

	initStringInfo(&buf);

	/* parse the segment by list exactly how you would a order by by */
	appendStringInfo(&buf,
					 "SELECT FROM %s.%s ORDER BY %s",
					 quote_identifier(NameStr(hypertable->fd.schema_name)),
					 quote_identifier(NameStr(hypertable->fd.table_name)),
					 inpstr);

	PG_TRY();
	{
		parsed = raw_parser(buf.data, RAW_PARSE_DEFAULT);
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

	foreach (lc, select->sortClause)
	{
		SortBy *sort_by;
		ColumnRef *cf;
		CompressedParsedCol *col = (CompressedParsedCol *) palloc(sizeof(*col));
		bool desc, nullsfirst;

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

		namestrcpy(&col->colname, strVal(linitial(cf->fields)));
		char *colname = strVal(linitial(cf->fields));

		AttrNumber col_attno = get_attnum(hypertable->main_table_relid, colname);
		if (col_attno == InvalidAttrNumber)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("column \"%s\" does not exist", NameStr(col->colname)),
					 errhint("The timescaledb.compress_orderby option must reference a valid "
							 "column.")));

		Oid col_type = get_atttype(hypertable->main_table_relid, col_attno);
		TypeCacheEntry *type = lookup_type_cache(col_type, TYPECACHE_LT_OPR);

		if (!OidIsValid(type->lt_opr))
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_FUNCTION),
					 errmsg("invalid ordering column type %s", format_type_be(col_type)),
					 errdetail("Could not identify a less-than operator for the type.")));

		/* get normalized column name */
		colname = get_attname(hypertable->main_table_relid, col_attno, false);

		/* check if orderby columns are distinct. */
		if (ts_array_is_member(settings.orderby, colname))
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("duplicate column name \"%s\"", colname),
					 errhint("The timescaledb.compress_orderby option must reference distinct "
							 "column.")));

		if (sort_by->sortby_dir != SORTBY_ASC && sort_by->sortby_dir != SORTBY_DESC &&
			sort_by->sortby_dir != SORTBY_DEFAULT)
			throw_order_by_error(inpstr);

		desc = sort_by->sortby_dir == SORTBY_DESC;

		if (sort_by->sortby_nulls == SORTBY_NULLS_DEFAULT)
		{
			/* default null ordering is LAST for ASC, FIRST for DESC */
			nullsfirst = desc;
		}
		else
		{
			nullsfirst = sort_by->sortby_nulls == SORTBY_NULLS_FIRST;
		}

		settings.orderby = ts_array_add_element_text(settings.orderby, pstrdup(colname));
		settings.orderby_desc = ts_array_add_element_bool(settings.orderby_desc, desc);
		settings.orderby_nullsfirst =
			ts_array_add_element_bool(settings.orderby_nullsfirst, nullsfirst);
	}

	Ensure(settings.orderby, "orderby setting is NULL after parsing");

	return settings;
}

/* returns List of CompressedParsedCol
 * compress_segmentby = `col1,col2,col3`
 */
ArrayType *
ts_compress_hypertable_parse_segment_by(WithClauseResult segmentby, Hypertable *hypertable)
{
	if (!segmentby.is_default)
	{
		return parse_segment_collist(TextDatumGetCString(segmentby.parsed), hypertable);
	}
	else
		return NULL;
}

/* returns List of CompressedParsedCol
 * E.g. timescaledb.compress_orderby = 'col1 asc nulls first,col2 desc,col3'
 */
OrderBySettings
ts_compress_hypertable_parse_order_by(WithClauseResult orderby, Hypertable *hypertable)
{
	Ensure(!orderby.is_default, "with clause is not default");
	return ts_compress_parse_order_collist(TextDatumGetCString(orderby.parsed), hypertable);
}

/* returns List of CompressedParsedCol
 * E.g. timescaledb.compress_orderby = 'col1 asc nulls first,col2 desc,col3'
 */
Interval *
ts_compress_hypertable_parse_chunk_time_interval(WithClauseResult *parsed_options,
												 Hypertable *hypertable)
{
	if (parsed_options[AlterTableFlagCompressChunkTimeInterval].is_default == false)
	{
		Datum textarg = parsed_options[AlterTableFlagCompressChunkTimeInterval].parsed;
		return DatumGetIntervalP(textarg);
	}
	else
		return NULL;
}
