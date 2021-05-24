
/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <utils/sortsupport.h>
#include <utils/typcache.h>
#include <utils/builtins.h>
#include <utils/datum.h>
#include <libpq/pqformat.h>

#include "segment_meta.h"
#include "datum_serialize.h"

typedef struct SegmentMetaMinMaxBuilder
{
	Oid type_oid;
	bool empty;
	bool has_null;

	SortSupportData ssup;
	bool type_by_val;
	int16 type_len;
	Datum min;
	Datum max;
} SegmentMetaMinMaxBuilder;

SegmentMetaMinMaxBuilder *
segment_meta_min_max_builder_create(Oid type_oid, Oid collation)
{
	SegmentMetaMinMaxBuilder *builder = palloc(sizeof(*builder));
	TypeCacheEntry *type = lookup_type_cache(type_oid, TYPECACHE_LT_OPR);

	if (!OidIsValid(type->lt_opr))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_FUNCTION),
				 errmsg("could not identify an less-than operator for type %s",
						format_type_be(type_oid))));

	*builder = (SegmentMetaMinMaxBuilder){
		.type_oid = type_oid,
		.empty = true,
		.has_null = false,
		.type_by_val = type->typbyval,
		.type_len = type->typlen,
	};

	builder->ssup.ssup_cxt = CurrentMemoryContext;
	builder->ssup.ssup_collation = collation;
	builder->ssup.ssup_nulls_first = false;

	PrepareSortSupportFromOrderingOp(type->lt_opr, &builder->ssup);

	return builder;
}

void
segment_meta_min_max_builder_update_val(SegmentMetaMinMaxBuilder *builder, Datum val)
{
	int cmp;

	if (builder->empty)
	{
		builder->min = datumCopy(val, builder->type_by_val, builder->type_len);
		builder->max = datumCopy(val, builder->type_by_val, builder->type_len);
		builder->empty = false;
		return;
	}

	cmp = ApplySortComparator(builder->min, false, val, false, &builder->ssup);
	if (cmp > 0)
	{
		if (!builder->type_by_val)
			pfree(DatumGetPointer(builder->min));
		builder->min = datumCopy(val, builder->type_by_val, builder->type_len);
	}

	cmp = ApplySortComparator(builder->max, false, val, false, &builder->ssup);
	if (cmp < 0)
	{
		if (!builder->type_by_val)
			pfree(DatumGetPointer(builder->max));
		builder->max = datumCopy(val, builder->type_by_val, builder->type_len);
	}
}

void
segment_meta_min_max_builder_update_null(SegmentMetaMinMaxBuilder *builder)
{
	builder->has_null = true;
}

void
segment_meta_min_max_builder_reset(SegmentMetaMinMaxBuilder *builder)
{
	if (!builder->empty)
	{
		if (!builder->type_by_val)
		{
			pfree(DatumGetPointer(builder->min));
			pfree(DatumGetPointer(builder->max));
		}
		builder->min = 0;
		builder->max = 0;
	}
	builder->empty = true;
	builder->has_null = false;
}

Datum
segment_meta_min_max_builder_min(SegmentMetaMinMaxBuilder *builder)
{
	if (builder->empty)
		elog(ERROR, "trying to get min from an empty builder");
	if (builder->type_len == -1)
	{
		Datum unpacked = PointerGetDatum(PG_DETOAST_DATUM_PACKED(builder->min));
		if (builder->min != unpacked)
			pfree(DatumGetPointer(builder->min));
		builder->min = unpacked;
	}
	return builder->min;
}

Datum
segment_meta_min_max_builder_max(SegmentMetaMinMaxBuilder *builder)
{
	if (builder->empty)
		elog(ERROR, "trying to get max from an empty builder");
	if (builder->type_len == -1)
	{
		Datum unpacked = PointerGetDatum(PG_DETOAST_DATUM_PACKED(builder->max));
		if (builder->max != unpacked)
			pfree(DatumGetPointer(builder->max));
		builder->max = unpacked;
	}
	return builder->max;
}

bool
segment_meta_min_max_builder_empty(SegmentMetaMinMaxBuilder *builder)
{
	return builder->empty;
}
