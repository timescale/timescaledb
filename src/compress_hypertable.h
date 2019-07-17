/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_COMPRESS_HYPERTABLE_H
#define TIMESCALEDB_COMPRESS_HYPERTABLE_H
#include <postgres.h>
#include <catalog/pg_type.h>

#include <catalog.h>
#include <chunk.h>

#include "with_clause_parser.h"

typedef enum CompressHypertableOption
{
	CompressEnabled = 0,
	CompressSegmentBy,
	CompressOrderBy,
} CompressHypertableOption;

WithClauseResult *ts_compress_hypertable_set_clause_parse(const List *defelems);

#endif
