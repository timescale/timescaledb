/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#pragma once

#include <postgres.h>

#include "with_clause_parser.h"

typedef enum CreateMaterializedViewFlags
{
	CreateMaterializedViewFlagContinuous = 0,
	CreateMaterializedViewFlagCreateGroupIndexes,
	CreateMaterializedViewFlagMaterializedOnly,
	CreateMaterializedViewFlagColumnstore,
	CreateMaterializedViewFlagFinalized,
	CreateMaterializedViewFlagChunkTimeInterval,
	CreateMaterializedViewFlagSegmentBy,
	CreateMaterializedViewFlagOrderBy,
	CreateMaterializedViewFlagCompressChunkTimeInterval,
	CreateMaterializedViewFlagInvalidateUsing,
} CreateMaterializedViewFlags;

extern TSDLLEXPORT WithClauseResult *
ts_create_materialized_view_with_clause_parse(const List *defelems);

extern TSDLLEXPORT List *
ts_continuous_agg_get_compression_defelems(const WithClauseResult *with_clauses);
