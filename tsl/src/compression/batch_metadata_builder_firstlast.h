/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

#include <postgres.h>

#include "batch_metadata_builder.h"

typedef struct BatchMetadataBuilderFirstLast
{
	BatchMetadataBuilder functions;

	Oid type_oid;
	AttrNumber attnum;
	bool empty;

	bool type_by_val;
	int16 type_len;

	Datum first;
	bool first_is_null;

	Datum last;
	bool last_is_null;

	int16 first_metadata_attr_offset;
	int16 last_metadata_attr_offset;
} BatchMetadataBuilderFirstLast;

typedef struct RowCompressor RowCompressor;
