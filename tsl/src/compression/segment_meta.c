
/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <libpq/pqformat.h>
#include <utils/builtins.h>
#include <utils/datum.h>
#include <utils/sortsupport.h>
#include <utils/typcache.h>

#include "debug_assert.h"
#include "segment_meta.h"

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

static Datum
compare_values(SegmentMetaMinMaxBuilder *builder, Datum old_val, Datum val, bool is_min)
{
	int cmp;

	cmp = ApplySortComparator(old_val, false, val, false, &builder->ssup);

	if ((is_min && cmp > 0) || (!is_min && cmp < 0))
	{
		if (!builder->type_by_val)
			pfree(DatumGetPointer(old_val));

		return datumCopy(val, builder->type_by_val, builder->type_len);
	}

	return old_val;
}

void
segment_meta_min_max_builder_update_val(SegmentMetaMinMaxBuilder *builder, Datum val)
{
	if (builder->empty)
	{
		builder->min = datumCopy(val, builder->type_by_val, builder->type_len);
		builder->max = datumCopy(val, builder->type_by_val, builder->type_len);
		builder->empty = false;
		return;
	}

	builder->min = compare_values(builder, builder->min, val, true);
	builder->max = compare_values(builder, builder->max, val, false);
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
		/* Update the relation min and max. Those values need to live on a
		 * memory context that has the same lifetime as the builder itself. */
		MemoryContext oldcxt = MemoryContextSwitchTo(builder->ssup.ssup_cxt);

		if (!builder->has_relation_stats)
		{
			builder->relation_min =
				datumCopy(builder->min, builder->type_by_val, builder->type_len);
			builder->relation_max =
				datumCopy(builder->max, builder->type_by_val, builder->type_len);
			builder->has_relation_stats = true;
		}
		else
		{
			builder->relation_min =
				compare_values(builder, builder->relation_min, builder->min, true);
			builder->relation_max =
				compare_values(builder, builder->relation_max, builder->max, false);
		}

		MemoryContextSwitchTo(oldcxt);

		if (!builder->type_by_val)
		{
			pfree(DatumGetPointer(builder->min));
			pfree(DatumGetPointer(builder->max));
		}
		builder->min = 0;
		builder->max = 0;
		builder->empty = true;
	}

	builder->has_null = false;
}

static Datum
get_unpacked_value(SegmentMetaMinMaxBuilder *builder, Datum *value, bool valid_condition,
				   const char *valuetype)
{
	Datum unpacked = *value;

	Ensure(valid_condition, "no data for %s stats", valuetype);

	if (builder->type_len == -1)
	{
		unpacked = PointerGetDatum(PG_DETOAST_DATUM_PACKED(*value));

		if (*value != unpacked)
			pfree(DatumGetPointer(*value));

		*value = unpacked;
	}

	return unpacked;
}

Datum
segment_meta_min_max_builder_min(SegmentMetaMinMaxBuilder *builder)
{
	return get_unpacked_value(builder, &builder->min, !builder->empty, "min");
}

Datum
segment_meta_min_max_builder_max(SegmentMetaMinMaxBuilder *builder)
{
	return get_unpacked_value(builder, &builder->max, !builder->empty, "max");
}

Datum
segment_meta_min_max_builder_relation_min(SegmentMetaMinMaxBuilder *builder)
{
	return get_unpacked_value(builder,
							  &builder->relation_min,
							  builder->has_relation_stats,
							  "relation min");
}

Datum
segment_meta_min_max_builder_relation_max(SegmentMetaMinMaxBuilder *builder)
{
	return get_unpacked_value(builder,
							  &builder->relation_max,
							  builder->has_relation_stats,
							  "relation max");
}

bool
segment_meta_min_max_builder_empty(const SegmentMetaMinMaxBuilder *builder)
{
	return builder->empty;
}

bool
segment_meta_has_relation_stats(const SegmentMetaMinMaxBuilder *builder)
{
	return builder->has_relation_stats;
}
