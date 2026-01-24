/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#pragma once

#include <postgres.h>

#include "with_clause_parser.h"

typedef enum CreateTableFlags
{
	CreateTableFlagHypertable = 0,
	CreateTableFlagColumnstore,
	CreateTableFlagTimeColumn,
	CreateTableFlagChunkTimeInterval,
	CreateTableFlagCreateDefaultIndexes,
	CreateTableFlagAssociatedSchema,
	CreateTableFlagAssociatedTablePrefix,
	CreateTableFlagOrderBy,
	CreateTableFlagSegmentBy,
	CreateTableFlagIndex,
	CreateTableFlagOrigin
} CreateTableFlags;

WithClauseResult *ts_create_table_with_clause_parse(const List *defelems);

Datum ts_create_table_parse_chunk_time_interval(WithClauseResult option, Oid column_type,
												Oid *interval_type);

Datum ts_create_table_parse_origin(WithClauseResult option, Oid column_type, Oid *origin_type);
