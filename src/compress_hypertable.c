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

#include "compress_hypertable.h"

static const WithClauseDefinition compress_hypertable_with_clause_def[] = {
		[CompressEnabled] = {
			.arg_name = "compress",
			.type_id = BOOLOID,
			.default_val = BoolGetDatum(false),
		},
		[CompressSegmentBy] = {
			 .arg_name = "segment_by",
			 .type_id = TEXTARRAYOID,
		},
		[CompressOrderBy] = {
			 .arg_name = "order_by",
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
