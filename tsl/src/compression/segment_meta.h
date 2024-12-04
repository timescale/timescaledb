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

typedef struct SegmentMetaMinMaxBuilder
{
	Oid type_oid;
	bool empty;
	bool has_null;

	SortSupportData ssup;
	bool type_by_val;
	int16 type_len;

	/* Per-segment min and max */
	Datum min;
	Datum max;

	/* Per-relation min and max. Updated on every segment processed (builder
	 * reset). */
	Datum relation_min;
	Datum relation_max;
	Datum has_relation_stats;
} SegmentMetaMinMaxBuilder;

typedef struct SegmentMetaMinMaxBuilder SegmentMetaMinMaxBuilder;

SegmentMetaMinMaxBuilder *segment_meta_min_max_builder_create(Oid type, Oid collation);
void segment_meta_min_max_builder_update_val(SegmentMetaMinMaxBuilder *builder, Datum val);
void segment_meta_min_max_builder_update_null(SegmentMetaMinMaxBuilder *builder);

Datum segment_meta_min_max_builder_min(SegmentMetaMinMaxBuilder *builder);
Datum segment_meta_min_max_builder_max(SegmentMetaMinMaxBuilder *builder);

Datum segment_meta_min_max_builder_relation_min(SegmentMetaMinMaxBuilder *builder);
Datum segment_meta_min_max_builder_relation_max(SegmentMetaMinMaxBuilder *builder);

void segment_meta_min_max_builder_reset(SegmentMetaMinMaxBuilder *builder);

bool segment_meta_min_max_builder_empty(const SegmentMetaMinMaxBuilder *builder);
bool segment_meta_has_relation_stats(const SegmentMetaMinMaxBuilder *builder);
