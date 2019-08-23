/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_TSL_COMPRESSION_SEGMENT_META_H
#define TIMESCALEDB_TSL_COMPRESSION_SEGMENT_META_H

#include <postgres.h>
#include <lib/stringinfo.h>

typedef struct SegmentMetaMinMax SegmentMetaMinMax;
typedef struct SegmentMetaMinMaxBuilder SegmentMetaMinMaxBuilder;

SegmentMetaMinMaxBuilder *segment_meta_min_max_builder_create(Oid type, Oid collation);
void segment_meta_min_max_builder_update_val(SegmentMetaMinMaxBuilder *builder, Datum val);
void segment_meta_min_max_builder_update_null(SegmentMetaMinMaxBuilder *builder);
SegmentMetaMinMax *segment_meta_min_max_builder_finish(SegmentMetaMinMaxBuilder *builder);

Datum tsl_segment_meta_min_max_get_min(Datum meta, Oid type);
Datum tsl_segment_meta_min_max_get_max(Datum meta, Oid type);
bool tsl_segment_meta_min_max_has_null(Datum meta);

bytea *segment_meta_min_max_to_binary_string(SegmentMetaMinMax *meta);

SegmentMetaMinMax *segment_meta_min_max_from_binary_string(StringInfo buf);

static inline bytea *
tsl_segment_meta_min_max_send(Datum arg1)
{
	return segment_meta_min_max_to_binary_string((SegmentMetaMinMax *) DatumGetPointer(arg1));
}

static inline Datum
tsl_segment_meta_min_max_recv(StringInfo buf)
{
	return PointerGetDatum(segment_meta_min_max_from_binary_string(buf));
}
#endif
