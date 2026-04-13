/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <utils/datum.h>
#include <utils/typcache.h>

#include "batch_metadata_builder_firstlast.h"

#include "compression.h"

static void firstlast_update_row(void *builder_, TupleTableSlot *slot);
static void firstlast_insert_to_compressed_row(void *builder_, RowCompressor *compressor);
static void firstlast_reset(void *builder_, RowCompressor *compressor);

BatchMetadataBuilder *
batch_metadata_builder_firstlast_create(Oid type_oid, AttrNumber attnum, int first_attr_offset,
										int last_attr_offset)
{
	BatchMetadataBuilderFirstLast *builder = palloc(sizeof(*builder));
	TypeCacheEntry *type = lookup_type_cache(type_oid, 0);

	*builder = (BatchMetadataBuilderFirstLast){
		.functions =
			(BatchMetadataBuilder){
				.update_row = firstlast_update_row,
				.insert_to_compressed_row = firstlast_insert_to_compressed_row,
				.reset = firstlast_reset,
				.builder_type = METADATA_BUILDER_FIRSTLAST,
			},
		.type_oid = type_oid,
		.attnum = attnum,
		.empty = true,
		.type_by_val = type->typbyval,
		.type_len = type->typlen,
		.first_is_null = true,
		.last_is_null = true,
		.first_metadata_attr_offset = first_attr_offset,
		.last_metadata_attr_offset = last_attr_offset,
	};

	return &builder->functions;
}

static void
firstlast_update_row(void *builder_, TupleTableSlot *slot)
{
	BatchMetadataBuilderFirstLast *builder = (BatchMetadataBuilderFirstLast *) builder_;
	Assert(builder->functions.builder_type == METADATA_BUILDER_FIRSTLAST);

	bool is_null;
	Datum val = slot_getattr(slot, builder->attnum, &is_null);

	if (builder->empty)
	{
		if (is_null)
		{
			builder->first_is_null = true;
			builder->first = (Datum) 0;
		}
		else
		{
			builder->first_is_null = false;
			builder->first = datumCopy(val, builder->type_by_val, builder->type_len);
		}
		builder->empty = false;
	}

	/* Always update last to the current row */
	if (!builder->last_is_null && !builder->type_by_val)
	{
		pfree(DatumGetPointer(builder->last));
	}

	if (is_null)
	{
		builder->last_is_null = true;
		builder->last = (Datum) 0;
	}
	else
	{
		builder->last_is_null = false;
		builder->last = datumCopy(val, builder->type_by_val, builder->type_len);
	}
}

static void
firstlast_reset(void *builder_, RowCompressor *compressor)
{
	BatchMetadataBuilderFirstLast *builder = (BatchMetadataBuilderFirstLast *) builder_;

	if (!builder->empty)
	{
		if (!builder->first_is_null && !builder->type_by_val)
		{
			pfree(DatumGetPointer(builder->first));
		}
		if (!builder->last_is_null && !builder->type_by_val)
		{
			pfree(DatumGetPointer(builder->last));
		}
		builder->first = (Datum) 0;
		builder->last = (Datum) 0;
	}
	builder->empty = true;
	builder->first_is_null = true;
	builder->last_is_null = true;

	compressor->compressed_is_null[builder->first_metadata_attr_offset] = true;
	compressor->compressed_is_null[builder->last_metadata_attr_offset] = true;
	compressor->compressed_values[builder->first_metadata_attr_offset] = (Datum) 0;
	compressor->compressed_values[builder->last_metadata_attr_offset] = (Datum) 0;
}

static void
firstlast_insert_to_compressed_row(void *builder_, RowCompressor *compressor)
{
	BatchMetadataBuilderFirstLast *builder = (BatchMetadataBuilderFirstLast *) builder_;
	Assert(builder->first_metadata_attr_offset >= 0);
	Assert(builder->last_metadata_attr_offset >= 0);

	if (builder->empty)
	{
		compressor->compressed_is_null[builder->first_metadata_attr_offset] = true;
		compressor->compressed_is_null[builder->last_metadata_attr_offset] = true;
		return;
	}

	/* First value */
	if (builder->first_is_null)
	{
		compressor->compressed_is_null[builder->first_metadata_attr_offset] = true;
	}
	else
	{
		Datum first = builder->first;
		if (builder->type_len == -1)
		{
			Datum unpacked = PointerGetDatum(PG_DETOAST_DATUM_PACKED(first));
			if (first != unpacked)
			{
				pfree(DatumGetPointer(first));
			}
			first = unpacked;
			builder->first = first;
		}
		compressor->compressed_is_null[builder->first_metadata_attr_offset] = false;
		compressor->compressed_values[builder->first_metadata_attr_offset] = first;
	}

	/* Last value */
	if (builder->last_is_null)
	{
		compressor->compressed_is_null[builder->last_metadata_attr_offset] = true;
	}
	else
	{
		Datum last = builder->last;
		if (builder->type_len == -1)
		{
			Datum unpacked = PointerGetDatum(PG_DETOAST_DATUM_PACKED(last));
			if (last != unpacked)
			{
				pfree(DatumGetPointer(last));
			}
			last = unpacked;
			builder->last = last;
		}
		compressor->compressed_is_null[builder->last_metadata_attr_offset] = false;
		compressor->compressed_values[builder->last_metadata_attr_offset] = last;
	}
}
