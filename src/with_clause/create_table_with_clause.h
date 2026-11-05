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
	CreateTableFlagDirectCompress,
	CreateTableFlagDirectCompressScheduleInterval,
	CreateTableFlagCompressAfter
} CreateTableFlags;

WithClauseResult *ts_create_table_with_clause_parse(const List *defelems);

/*
 * Combine the options of a statement with those of a storage policy. A slot set explicitly by
 * the statement wins, any other slot takes the policy's value.
 */
WithClauseResult *ts_create_table_with_clause_overlay(const WithClauseResult *explicit_clauses,
													  const WithClauseResult *policy_clauses);
