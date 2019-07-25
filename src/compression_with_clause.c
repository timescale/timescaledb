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

#include "compat.h"

#include "compression_with_clause.h"

static const WithClauseDefinition compress_hypertable_with_clause_def[] = {
		[CompressEnabled] = {
			.arg_name = "compress",
			.type_id = BOOLOID,
			.default_val = BoolGetDatum(false),
		},
		[CompressSegmentBy] = {
			 .arg_name = "compress_segmentby",
			 .type_id = TEXTOID,
		},
		[CompressOrderBy] = {
			 .arg_name = "compress_orderby",
			 .type_id = TEXTOID,
		},
};

WithClauseResult *
ts_compress_hypertable_set_clause_parse(const List *defelems)
{
	return ts_with_clauses_parse(defelems,
								 compress_hypertable_with_clause_def,
								 TS_ARRAY_LEN(compress_hypertable_with_clause_def));
}
/* strip double quotes from tokens that are names of columns */
static char *
strip_name_token(char *token)
{
	int len;
	if (token == NULL)
		return NULL;
	len = strlen(token);
	if (token[0] == '"' && (len > 1) && (token[len - 1] == '"'))
	{
		char *newtok = palloc0(sizeof(char) * (len - 1));
		strncpy(newtok, &token[1], len - 2);
		return newtok;
	}
	return token;
}

static List *
parse_segment_collist(char *inpstr, const char *delim)
{
	List *collist = NIL;
	char *saveptr = NULL;
	char *token = strtok_r(inpstr, delim, &saveptr);
	short index = 0;
	while (token)
	{
		char *namtoken = NULL;
		CompressedParsedCol *col = (CompressedParsedCol *) palloc(sizeof(CompressedParsedCol));
		col->index = index;
		namtoken = strip_name_token(token);
		namestrcpy(&col->colname, namtoken);
		index++;
		// elog(INFO, "colname is %s %d", col->colname, col->index);
		collist = lappend(collist, (void *) col);
		token = strtok_r(NULL, delim, &saveptr);
	}
	return collist;
}

#define CHKTOKEN(token, str)                                                                       \
	(token && (strncmp(token, str, strlen(str)) == 0) && (strlen(str) == strlen(token)))
#define PRINT_UNEXPECTED_TOKEN_MSG(token2)                                                         \
	ereport(ERROR,                                                                                 \
			(errcode(ERRCODE_SYNTAX_ERROR),                                                        \
			 errmsg("unexpected token %s in compress_orderby list ", token2)))

static CompressedParsedCol *
parse_orderelement(char *elttoken, const char *spcdelim, short index)
{
	bool getnext = false;
	bool neednullstok = false;
	CompressedParsedCol *col = (CompressedParsedCol *) palloc(sizeof(CompressedParsedCol));
	char *saveptr2 = NULL;
	char *namtoken;
	char *token2 = strtok_r(elttoken, spcdelim, &saveptr2);
	col->index = index;
	namtoken = strip_name_token(token2);
	namestrcpy(&col->colname, namtoken);
	/* default for sort is asc and nulls first */
	col->asc = true;
	col->nullsfirst = true;

	token2 = strtok_r(NULL, spcdelim, &saveptr2);
	if (CHKTOKEN(token2, "asc"))
	{
		col->asc = true;
		getnext = true;
	}
	else if (CHKTOKEN(token2, "desc"))
	{
		col->asc = false;
		/* if we have desceneding then nulls last is default unless user specifies otherwise */
		col->nullsfirst = false;
		getnext = true;
	}
	if (getnext)
	{
		token2 = strtok_r(NULL, spcdelim, &saveptr2);
	}
	if (CHKTOKEN(token2, "nulls"))
	{
		token2 = strtok_r(NULL, spcdelim, &saveptr2);
		neednullstok = true;
	}
	else if (token2) // we have a token but not nay of the expected ones
	{
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("unexpected token %s in compress_orderby list ", token2)));
	}
	if (CHKTOKEN(token2, "first"))
	{
		col->nullsfirst = true;
	}
	else if (CHKTOKEN(token2, "last"))
	{
		col->nullsfirst = false;
	}
	else if (neednullstok)
	{
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("expect first/last after nulls  in compress_orderby list ")));
	}
	else if (token2) // we have a token but not nay of the expected ones
	{
		PRINT_UNEXPECTED_TOKEN_MSG(token2);
	}
	// any more tokens left?
	token2 = strtok_r(NULL, spcdelim, &saveptr2);
	if (token2)
	{
		PRINT_UNEXPECTED_TOKEN_MSG(token2);
	}

	return col;
}

/* compress_orderby = `<elt>,<elt>:...'
   <elt> = <col_name> [asc|desc] [nulls (first|last)]
 */
static List *
parse_order_collist(char *inpstr, const char *delim)
{
	List *collist = NIL;
	char *saveptr = NULL;
	char *elttoken = strtok_r(inpstr, delim, &saveptr);
	short index = 0;
	char spcdelim = ' ';
	while (elttoken)
	{
		CompressedParsedCol *col = parse_orderelement(elttoken, &spcdelim, index);
		collist = lappend(collist, (void *) col);
		elttoken = strtok_r(NULL, delim, &saveptr);
		index++;
	}
	return collist;
}

/* returns List of CompressedParsedCol
 * compress_segmentby = `col1,col2,col3`
 */
List *
ts_compress_hypertable_parse_segment_by(WithClauseResult *parsed_options)
{
	if (parsed_options[CompressSegmentBy].is_default == false)
	{
		Datum textarg = parsed_options[CompressSegmentBy].parsed;
		return parse_segment_collist(TextDatumGetCString(textarg), ",");
	}
	else
		return NIL;
}

/* returns List of CompressedParsedCol
 * E.g. timescaledb.compress_orderby = 'col1 asc nulls first,col2 desc,col3'
 */
List *
ts_compress_hypertable_parse_order_by(WithClauseResult *parsed_options)
{
	if (parsed_options[CompressOrderBy].is_default == false)
	{
		Datum textarg = parsed_options[CompressOrderBy].parsed;
		return parse_order_collist(TextDatumGetCString(textarg), ",");
	}
	else
		return NIL;
}
