/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include "batch_metadata_builder.h"
#include "city_combine.h" /* for city_hash_combine */
#include "compression.h"
#include "postgres.h"
#include "sparse_index_bloom1.h"			 /* for bloom1_get_hash_function */
#include "ts_catalog/compression_settings.h" /* for MAX_BLOOM_FILTER_COLUMNS */

typedef struct Bloom1CompositeCommon
{
	/* the number of columns in the composite bloom filter */
	int num_columns;

	/* the number of times the update_val or update_null function has been called
	 * for this row. this is used to determine if we should finalize the bloom filter. */
	int update_call_count;

	/* the accumulated hash for the current row */
	uint64 accumulated_hash;

	/* the hash functions for each column */
	PGFunction hash_functions[MAX_BLOOM_FILTER_COLUMNS];

	/* the function infos for each column */
	FmgrInfo *hash_finfos[MAX_BLOOM_FILTER_COLUMNS];

} Bloom1CompositeCommon;

typedef struct Bloom1CompositeMetadataBuilder
{
	Bloom1CompositeCommon common;
	BatchMetadataBuilder functions;

	/* the number of times the reset function has been called.
	 * this is used to ensure reset is only performed once, even if
	 * the same builder is present in multiple columns.
	 */
	int reset_call_count;

	/* the number of times the insert_to_compressed_row function has been called
	 * for this row. this is used to ensure insert_to_compressed_row is only called
	 * once, even if the same builder is present in multiple columns. */
	int insert_call_count;

	/* where to store the bloom filter in the compressed tuple */
	int16 bloom_attr_offset;

	/* the size of the bloom filter in bytes */
	int allocated_varlena_bytes;

	/* the bloom filter itself */
	struct varlena *bloom_varlena;
} Bloom1CompositeMetadataBuilder;

typedef struct Bloom1CompositeHasher
{
	Bloom1CompositeCommon common;
	Bloom1Hasher functions;
} Bloom1CompositeHasher;

static void composite_bloom_update_val(void *builder, Datum val);
static void composite_bloom_update_null(void *builder);
static void composite_bloom_insert_to_compressed_row(void *builder, RowCompressor *compressor);
static void composite_bloom_reset(void *builder, RowCompressor *compressor);
static uint64 composite_bloom_hasher_update_val(void *builder, Datum val);
static uint64 composite_bloom_hasher_update_null(void *builder);
static void composite_bloom_hasher_reset(void *builder);
static uint64 composite_bloom_common_update_val(void *builder, Datum val);
static uint64 composite_bloom_common_update_null(void *builder);

static uint64
composite_bloom_common_update_val(void *builder_, Datum val)
{
	Bloom1CompositeCommon *common = (Bloom1CompositeCommon *) builder_;
	Assert(common->update_call_count < common->num_columns);

	/* calculate the hash for the current column */
	PGFunction hash_fn = common->hash_functions[common->update_call_count];
	FmgrInfo *hash_finfo = common->hash_finfos[common->update_call_count];

	const uint64 datum_hash =
		batch_metadata_builder_bloom1_calculate_hash(hash_fn, hash_finfo, val);
	common->accumulated_hash = city_hash_combine(common->accumulated_hash, datum_hash);
	common->update_call_count++;

	return common->accumulated_hash;
}

static uint64
composite_bloom_common_update_null(void *builder_)
{
	Bloom1CompositeCommon *common = (Bloom1CompositeCommon *) builder_;
	Assert(common->update_call_count < common->num_columns);

	/* The NULL values may be matched with an IS_NULL clause. For this reason we add the NULL marker
	 * to the accumulated hash. */
	common->accumulated_hash = city_hash_combine(common->accumulated_hash, NULL_MARKER);
	common->update_call_count++;

	return common->accumulated_hash;
}

static void
composite_bloom_update_val(void *builder_, Datum val)
{
	BatchMetadataBuilder *base = (BatchMetadataBuilder *) builder_;
	Assert(base->builder_type == METADATA_BUILDER_BLOOM1_COMPOSITE);
	Bloom1CompositeMetadataBuilder *builder =
		(Bloom1CompositeMetadataBuilder *) (((char *) base) -
											offsetof(Bloom1CompositeMetadataBuilder, functions));
	Assert(builder->reset_call_count == 0);
	Assert(builder->insert_call_count == 0);

	composite_bloom_common_update_val(&builder->common, val);

	if (builder->common.update_call_count == builder->common.num_columns)
	{
		batch_metadata_builder_bloom1_update_bloom_filter_with_hash(builder->bloom_varlena,
																	builder->common
																		.accumulated_hash);
		builder->common.update_call_count = 0;
		builder->common.accumulated_hash = 0;
	}
}

static void
composite_bloom_update_null(void *builder_)
{
	BatchMetadataBuilder *base = (BatchMetadataBuilder *) builder_;
	Assert(base->builder_type == METADATA_BUILDER_BLOOM1_COMPOSITE);
	Bloom1CompositeMetadataBuilder *builder =
		(Bloom1CompositeMetadataBuilder *) (((char *) base) -
											offsetof(Bloom1CompositeMetadataBuilder, functions));
	/* when the call to update_null arrives, the reset and insert call counts should be 0
	 * because at this stage we are accumulating hashes for the current column */
	Assert(builder->reset_call_count == 0);
	Assert(builder->insert_call_count == 0);

	composite_bloom_common_update_null(&builder->common);

	if (builder->common.update_call_count == builder->common.num_columns)
	{
		batch_metadata_builder_bloom1_update_bloom_filter_with_hash(builder->bloom_varlena,
																	builder->common
																		.accumulated_hash);
		builder->common.update_call_count = 0;
		builder->common.accumulated_hash = 0;
	}
}

static uint64
composite_bloom_hasher_update_val(void *hasher_, Datum val)
{
	Bloom1Hasher *base = (Bloom1Hasher *) hasher_;
	Assert(base->builder_type == METADATA_BUILDER_BLOOM1_COMPOSITE);
	Bloom1CompositeHasher *hasher =
		(Bloom1CompositeHasher *) (((char *) base) - offsetof(Bloom1CompositeHasher, functions));

	return composite_bloom_common_update_val(&hasher->common, val);
}

static uint64
composite_bloom_hasher_update_null(void *hasher_)
{
	Bloom1Hasher *base = (Bloom1Hasher *) hasher_;
	Assert(base->builder_type == METADATA_BUILDER_BLOOM1_COMPOSITE);
	Bloom1CompositeHasher *hasher =
		(Bloom1CompositeHasher *) (((char *) base) - offsetof(Bloom1CompositeHasher, functions));

	return composite_bloom_common_update_null(&hasher->common);
}

static void
composite_bloom_insert_to_compressed_row(void *builder_, RowCompressor *compressor)
{
	BatchMetadataBuilder *base = (BatchMetadataBuilder *) builder_;
	Assert(base->builder_type == METADATA_BUILDER_BLOOM1_COMPOSITE);
	Bloom1CompositeMetadataBuilder *builder =
		(Bloom1CompositeMetadataBuilder *) (((char *) base) -
											offsetof(Bloom1CompositeMetadataBuilder, functions));
	/* when the call to insert_to_compressed_row arrives, the updates and reset call counts should
	 * be 0 because at this stage we are collecting enough insert calls, but no longer accumulating
	 * hashes and neither resetting the bloom filter */
	Assert(builder->reset_call_count == 0);
	Assert(builder->common.update_call_count == 0);
	Assert(builder->common.accumulated_hash == 0);

	builder->insert_call_count++;

	if (builder->insert_call_count == builder->common.num_columns)
	{
		batch_metadata_builder_bloom1_insert_bloom_filter_to_compressed_row(builder->bloom_varlena,
																			builder
																				->bloom_attr_offset,
																			compressor);
		builder->insert_call_count = 0;
		builder->common.accumulated_hash = 0;
	}
}

static void
composite_bloom_reset(void *builder_, RowCompressor *compressor)
{
	BatchMetadataBuilder *base = (BatchMetadataBuilder *) builder_;
	Assert(base->builder_type == METADATA_BUILDER_BLOOM1_COMPOSITE);
	Bloom1CompositeMetadataBuilder *builder =
		(Bloom1CompositeMetadataBuilder *) (((char *) base) -
											offsetof(Bloom1CompositeMetadataBuilder, functions));
	/* when the call to reset arrives, we should have updated and flushed the bloom filter for each
	 * column so we are not collectuing insert calls and neither accumulating hashes */
	Assert(builder->insert_call_count == 0);
	Assert(builder->common.update_call_count == 0);
	Assert(builder->common.accumulated_hash == 0);

	builder->reset_call_count++;

	if (builder->reset_call_count == builder->common.num_columns)
	{
		struct varlena *bloom = builder->bloom_varlena;
		memset(bloom, 0, builder->allocated_varlena_bytes);
		SET_VARSIZE(bloom, builder->allocated_varlena_bytes);

		compressor->compressed_is_null[builder->bloom_attr_offset] = true;
		compressor->compressed_values[builder->bloom_attr_offset] = 0;

		builder->reset_call_count = 0;
		builder->common.accumulated_hash = 0;
	}
}

static void
composite_bloom_hasher_reset(void *hasher_)
{
	Bloom1Hasher *base = (Bloom1Hasher *) hasher_;
	Assert(base->builder_type == METADATA_BUILDER_BLOOM1_COMPOSITE);
	Bloom1CompositeHasher *hasher =
		(Bloom1CompositeHasher *) (((char *) base) - offsetof(Bloom1CompositeHasher, functions));
	Assert(hasher->functions.builder_type == METADATA_BUILDER_BLOOM1_COMPOSITE);
	hasher->common.accumulated_hash = 0;
	hasher->common.update_call_count = 0;
}

BatchMetadataBuilder *
batch_metadata_builder_bloom1_composite_create(const Oid *type_oids, int num_columns,
											   int bloom_attr_offset)
{
	Assert(num_columns >= 2 && num_columns <= MAX_BLOOM_FILTER_COLUMNS);

	const int varlena_bytes = batch_metadata_builder_bloom1_varlena_size();
	Bloom1CompositeMetadataBuilder *builder = palloc0(sizeof(*builder));
	*builder = (Bloom1CompositeMetadataBuilder){
		.common =
			(Bloom1CompositeCommon){
				.num_columns = num_columns,
				.update_call_count = 0,
				.accumulated_hash = 0,
			},
		.functions =
			(BatchMetadataBuilder){
				.update_val = composite_bloom_update_val,
				.update_null = composite_bloom_update_null,
				.insert_to_compressed_row = composite_bloom_insert_to_compressed_row,
				.reset = composite_bloom_reset,
				.builder_type = METADATA_BUILDER_BLOOM1_COMPOSITE,
			},
		.bloom_attr_offset = bloom_attr_offset,
		.allocated_varlena_bytes = varlena_bytes,
		.bloom_varlena = palloc0(varlena_bytes),
	};

	for (int i = 0; i < num_columns; i++)
	{
		builder->common.hash_functions[i] =
			bloom1_get_hash_function(type_oids[i], &builder->common.hash_finfos[i]);
		if (builder->common.hash_functions[i] == NULL)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("no hash function for type %u", type_oids[i])));
		}
	}

	SET_VARSIZE(builder->bloom_varlena, varlena_bytes);
	return &builder->functions;
}

Bloom1Hasher *
bloom1_composite_hasher_create(const Oid *type_oids, int num_columns)
{
	Assert(num_columns >= 2 && num_columns <= MAX_BLOOM_FILTER_COLUMNS);
	Bloom1CompositeHasher *hasher = palloc0(sizeof(*hasher));
	*hasher = (Bloom1CompositeHasher){
		.common =
			(Bloom1CompositeCommon){
				.num_columns = num_columns,
				.update_call_count = 0,
				.accumulated_hash = 0,
			},
		.functions =
			(Bloom1Hasher){
				.update_val = composite_bloom_hasher_update_val,
				.update_null = composite_bloom_hasher_update_null,
				.reset = composite_bloom_hasher_reset,
				.builder_type = METADATA_BUILDER_BLOOM1_COMPOSITE,
			},
	};

	for (int i = 0; i < num_columns; i++)
	{
		hasher->common.hash_functions[i] =
			bloom1_get_hash_function(type_oids[i], &hasher->common.hash_finfos[i]);
		if (hasher->common.hash_functions[i] == NULL)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("no hash function for type %u", type_oids[i])));
		}
	}

	return &hasher->functions;
}

#ifndef NDEBUG

TS_FUNCTION_INFO_V1(ts_bloom1_composite_debug_hash);

/*
 * Debug function: Calculate composite bloom hash from arrays.
 * Used for testing to verify hash calculation correctness and stability.
 *
 * SQL signature:
 *   ts_bloom1_composite_debug_hash(type_oids oid[], field_values anyarray, field_nulls bool[])
 *   RETURNS int8
 */
Datum
ts_bloom1_composite_debug_hash(PG_FUNCTION_ARGS)
{
	ArrayType *type_oids_array = PG_GETARG_ARRAYTYPE_P(0);
	ArrayType *field_values_array = PG_GETARG_ARRAYTYPE_P(1);
	ArrayType *field_nulls_array = PG_GETARG_ARRAYTYPE_P(2);

	/* Extract type OIDs */
	int num_fields;
	Datum *type_oid_datums;
	bool *type_oid_nulls;
	deconstruct_array(type_oids_array,
					  OIDOID,
					  sizeof(Oid),
					  true,
					  TYPALIGN_INT,
					  &type_oid_datums,
					  &type_oid_nulls,
					  &num_fields);

	if (num_fields < 2 || num_fields > MAX_BLOOM_FILTER_COLUMNS)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("composite bloom requires 2-%d fields, got %d",
						MAX_BLOOM_FILTER_COLUMNS,
						num_fields)));
	}

	Oid type_oids[MAX_BLOOM_FILTER_COLUMNS];
	for (int i = 0; i < num_fields; i++)
	{
		type_oids[i] = DatumGetObjectId(type_oid_datums[i]);
	}

	/* Extract field values */
	Datum *field_value_datums;
	bool *field_value_nulls;
	int num_values;
	int16 elmlen;
	bool elmbyval;
	char elmalign;

	get_typlenbyvalalign(ARR_ELEMTYPE(field_values_array), &elmlen, &elmbyval, &elmalign);
	deconstruct_array(field_values_array,
					  ARR_ELEMTYPE(field_values_array),
					  elmlen,
					  elmbyval,
					  elmalign,
					  &field_value_datums,
					  &field_value_nulls,
					  &num_values);

	if (num_values != num_fields)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("type_oids and field_values must have same length")));
	}

	/* Extract NULL flags */
	Datum *null_flag_datums;
	bool *null_flag_nulls;
	int num_nulls;
	deconstruct_array(field_nulls_array,
					  BOOLOID,
					  sizeof(bool),
					  true,
					  TYPALIGN_CHAR,
					  &null_flag_datums,
					  &null_flag_nulls,
					  &num_nulls);

	if (num_nulls != num_fields)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("type_oids and field_nulls must have same length")));
	}

	bool field_nulls[MAX_BLOOM_FILTER_COLUMNS];
	for (int i = 0; i < num_fields; i++)
	{
		field_nulls[i] = DatumGetBool(null_flag_datums[i]);
	}

	/* Create temporary builder */
	Bloom1Hasher *hasher = bloom1_composite_hasher_create(type_oids, num_fields);

	/* Call update functions with type conversion */
	Oid array_elemtype = ARR_ELEMTYPE(field_values_array);
	uint64 hash = 0;

	for (int i = 0; i < num_fields; i++)
	{
		if (field_nulls[i])
		{
			hash = composite_bloom_hasher_update_null(hasher);
		}
		else
		{
			Datum value = field_value_datums[i];

			/* Convert if array type != target type */
			if (array_elemtype != type_oids[i])
			{
				Oid typinput, typioparam, typoutput;
				bool typIsVarlena;
				char *str;

				getTypeOutputInfo(array_elemtype, &typoutput, &typIsVarlena);
				str = OidOutputFunctionCall(typoutput, value);

				getTypeInputInfo(type_oids[i], &typinput, &typioparam);
				value = OidInputFunctionCall(typinput, str, typioparam, -1);

				pfree(str);
			}

			hash = composite_bloom_hasher_update_val(hasher, value);
		}
	}

	PG_RETURN_INT64((int64) hash);
}
#endif // #ifndef NDEBUG
