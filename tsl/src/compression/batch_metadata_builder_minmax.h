/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

#include <postgres.h>
#include <fmgr.h>
#include <lib/stringinfo.h>
#include <utils/sortsupport.h>

#include "batch_metadata_builder.h"

typedef struct BatchMetadataBuilderMinMax
{
	BatchMetadataBuilder functions;

	Oid type_oid;
	bool empty;
	bool has_null;

	SortSupportData ssup;
	bool type_by_val;
	int16 type_len;
	Datum min;
	Datum max;

	int16 min_metadata_attr_offset;
	int16 max_metadata_attr_offset;
} BatchMetadataBuilderMinMax;

typedef struct BatchMetadataBuilderMinMax BatchMetadataBuilderMinMax;

typedef struct RowCompressor RowCompressor;

/*
 * This is exposed only for the old unit tests. Ideally they should be replaced
 * with functional tests inspecting the compressed chunk table, and this
 * test-only interface should be removed.
 */
Datum batch_metadata_builder_minmax_min(void *builder_);
Datum batch_metadata_builder_minmax_max(void *builder_);
bool batch_metadata_builder_minmax_empty(void *builder_);
