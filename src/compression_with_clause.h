/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#pragma once

#include <postgres.h>
#include <catalog/pg_type.h>

#include "chunk.h"
#include "ts_catalog/catalog.h"

#include "with_clause_parser.h"

typedef enum CompressHypertableOption
{
	CompressEnabled = 0,
	CompressSegmentBy,
	CompressOrderBy,
	CompressChunkTimeInterval,
	CompressOptionMax
} CompressHypertableOption;

typedef struct
{
	NameData colname;
	bool nullsfirst;
	bool desc;
} CompressedParsedCol;

typedef struct
{
	ArrayType *orderby;
	ArrayType *orderby_desc;
	ArrayType *orderby_nullsfirst;
} OrderBySettings;

extern TSDLLEXPORT WithClauseResult *ts_compress_hypertable_set_clause_parse(const List *defelems);
extern TSDLLEXPORT ArrayType *
ts_compress_hypertable_parse_segment_by(WithClauseResult *parsed_options, Hypertable *hypertable);
extern TSDLLEXPORT OrderBySettings
ts_compress_hypertable_parse_order_by(WithClauseResult *parsed_options, Hypertable *hypertable);
extern TSDLLEXPORT Interval *
ts_compress_hypertable_parse_chunk_time_interval(WithClauseResult *parsed_options,
												 Hypertable *hypertable);
extern TSDLLEXPORT OrderBySettings ts_compress_parse_order_collist(char *inpstr,
																   Hypertable *hypertable);
