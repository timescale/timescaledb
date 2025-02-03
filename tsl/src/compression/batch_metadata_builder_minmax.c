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

#include "batch_metadata_builder_minmax.h"

#include "compression.h"

static void minmax_update_val(void *builder_, Datum val);
static void minmax_update_null(void *builder_);
static void minmax_insert_to_compressed_row(void *builder_, RowCompressor *compressor);
static void minmax_reset(void *builder_, RowCompressor *compressor);

BatchMetadataBuilder *
batch_metadata_builder_minmax_create(Oid type_oid, Oid collation, int min_attr_offset,
									 int max_attr_offset)
{
	BatchMetadataBuilderMinMax *builder = palloc(sizeof(*builder));
	TypeCacheEntry *type = lookup_type_cache(type_oid, TYPECACHE_LT_OPR);

	if (!OidIsValid(type->lt_opr))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_FUNCTION),
				 errmsg("could not identify an less-than operator for type %s",
						format_type_be(type_oid))));

	*builder = (BatchMetadataBuilderMinMax){
		.functions =
			(BatchMetadataBuilder){
				.update_val = minmax_update_val,
				.update_null = minmax_update_null,
				.insert_to_compressed_row = minmax_insert_to_compressed_row,
				.reset = minmax_reset,
			},
		.type_oid = type_oid,
		.empty = true,
		.has_null = false,
		.type_by_val = type->typbyval,
		.type_len = type->typlen,
		.min_metadata_attr_offset = min_attr_offset,
		.max_metadata_attr_offset = max_attr_offset,
	};

	builder->ssup.ssup_cxt = CurrentMemoryContext;
	builder->ssup.ssup_collation = collation;
	builder->ssup.ssup_nulls_first = false;

	PrepareSortSupportFromOrderingOp(type->lt_opr, &builder->ssup);

	return &builder->functions;
}

void
minmax_update_val(void *builder_, Datum val)
{
	BatchMetadataBuilderMinMax *builder = (BatchMetadataBuilderMinMax *) builder_;

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
minmax_update_null(void *builder_)
{
	BatchMetadataBuilderMinMax *builder = (BatchMetadataBuilderMinMax *) builder_;
	builder->has_null = true;
}

static void
minmax_reset(void *builder_, RowCompressor *compressor)
{
	BatchMetadataBuilderMinMax *builder = (BatchMetadataBuilderMinMax *) builder_;
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

	compressor->compressed_is_null[builder->max_metadata_attr_offset] = true;
	compressor->compressed_is_null[builder->min_metadata_attr_offset] = true;
	compressor->compressed_values[builder->min_metadata_attr_offset] = 0;
	compressor->compressed_values[builder->max_metadata_attr_offset] = 0;
}

Datum
batch_metadata_builder_minmax_min(void *builder_)
{
	BatchMetadataBuilderMinMax *builder = (BatchMetadataBuilderMinMax *) builder_;
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
batch_metadata_builder_minmax_max(void *builder_)
{
	BatchMetadataBuilderMinMax *builder = (BatchMetadataBuilderMinMax *) builder_;
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
batch_metadata_builder_minmax_empty(void *builder_)
{
	BatchMetadataBuilderMinMax *builder = (BatchMetadataBuilderMinMax *) builder_;
	return builder->empty;
}

static void
minmax_insert_to_compressed_row(void *builder_, RowCompressor *compressor)
{
	BatchMetadataBuilderMinMax *builder = (BatchMetadataBuilderMinMax *) builder_;
	Assert(builder->min_metadata_attr_offset >= 0);
	Assert(builder->max_metadata_attr_offset >= 0);

	if (!batch_metadata_builder_minmax_empty(builder))
	{
		compressor->compressed_is_null[builder->min_metadata_attr_offset] = false;
		compressor->compressed_is_null[builder->max_metadata_attr_offset] = false;

		compressor->compressed_values[builder->min_metadata_attr_offset] =
			batch_metadata_builder_minmax_min(builder);
		compressor->compressed_values[builder->max_metadata_attr_offset] =
			batch_metadata_builder_minmax_max(builder);
	}
	else
	{
		compressor->compressed_is_null[builder->min_metadata_attr_offset] = true;
		compressor->compressed_is_null[builder->max_metadata_attr_offset] = true;
	}
}
