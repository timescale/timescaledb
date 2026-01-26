/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include "batch_metadata_builder.h"
#include "compression.h"
#include "postgres.h"
#include "sparse_index_bloom1.h"			 /* for bloom1_get_hash_function */
#include "ts_catalog/compression_settings.h" /* for MAX_BLOOM_FILTER_COLUMNS */

/* The NULL marker is chosen to be a value that doesn't cancel out with a left rotation and XOR
 * operation, so NULL positions are preserved in the composite hash. The value is coming from Golden
 * ratio constant that has no mathematical relationship with the UMASH GF(2^64) space, so it is
 * unlikely to degrade the collision resistance of the bloom filter. */
#define NULL_MARKER 0x9E3779B97F4A7C15

typedef struct Bloom1CompositeMetadataBuilder
{
	BatchMetadataBuilder functions;

	/* the number of columns in the composite bloom filter */
	int num_columns;

	/* the accumulated hash for the current row */
	uint64 accumulated_hash;

	/* the last hash value just before the current row's hash is reset */
	uint64 last_hash_value;

	/* the number of times the reset function has been called.
	 * this is used to ensure reset is only performed once, even if
	 * the same builder is present in multiple columns.
	 */
	int reset_call_count;

	/* the number of times the update_val or update_null function has been called
	 * for this row. this is used to determine if we should finalize the bloom filter. */
	int update_call_count;

	/* the number of times the insert_to_compressed_row function has been called
	 * for this row. this is used to ensure insert_to_compressed_row is only called
	 * once, even if the same builder is present in multiple columns. */
	int insert_call_count;

	/* the hash functions for each column */
	PGFunction hash_functions[MAX_BLOOM_FILTER_COLUMNS];

	/* the function infos for each column */
	FmgrInfo *hash_finfos[MAX_BLOOM_FILTER_COLUMNS];

	/* where to store the bloom filter in the compressed tuple */
	int16 bloom_attr_offset;

	/* the size of the bloom filter in bytes */
	int allocated_varlena_bytes;

	/* the bloom filter itself */
	struct varlena *bloom_varlena;
} Bloom1CompositeMetadataBuilder;

static void composite_bloom_update_val(void *builder, Datum val);
static void composite_bloom_update_null(void *builder);
static void composite_bloom_insert_to_compressed_row(void *builder, RowCompressor *compressor);
static void composite_bloom_reset(void *builder, RowCompressor *compressor);

/*
 * Rotate left by 1 bit.
 */
static inline uint64
rotate_left_1(uint64 x)
{
	return (x << 1) | (x >> 63);
}

static void
composite_bloom_update_val(void *builder_, Datum val)
{
	Bloom1CompositeMetadataBuilder *builder = (Bloom1CompositeMetadataBuilder *) builder_;
	Assert(builder->functions.builder_type == METADATA_BUILDER_BLOOM1_COMPOSITE);
	/* when the call to update_null arrives, the reset and insert call counts should be 0
	 * because at this stage we are accumulating hashes for the current column */
	Assert(builder->reset_call_count == 0);
	Assert(builder->insert_call_count == 0);

	/* calculate the hash for the current column */
	PGFunction hash_fn = builder->hash_functions[builder->update_call_count];
	FmgrInfo *hash_finfo = builder->hash_finfos[builder->update_call_count];

	const uint64 datum_hash =
		batch_metadata_builder_bloom1_calculate_hash(hash_fn, hash_finfo, val);
	builder->accumulated_hash = rotate_left_1(builder->accumulated_hash) ^ datum_hash;

	builder->update_call_count++;

	if (builder->update_call_count == builder->num_columns)
	{
		builder->last_hash_value = builder->accumulated_hash;
		batch_metadata_builder_bloom1_update_bloom_filter_with_hash(builder->bloom_varlena,
																	builder->accumulated_hash);
		builder->update_call_count = 0;
		builder->accumulated_hash = 0;
	}
}

static void
composite_bloom_update_null(void *builder_)
{
	Bloom1CompositeMetadataBuilder *builder = (Bloom1CompositeMetadataBuilder *) builder_;
	Assert(builder->functions.builder_type == METADATA_BUILDER_BLOOM1_COMPOSITE);
	/* when the call to update_null arrives, the reset and insert call counts should be 0
	 * because at this stage we are accumulating hashes for the current column */
	Assert(builder->reset_call_count == 0);
	Assert(builder->insert_call_count == 0);

	/* The NULL values may be matched with an IS_NULL clause. For this reason we add the NULL marker
	 * to the accumulated hash. */
	builder->accumulated_hash = rotate_left_1(builder->accumulated_hash) ^ NULL_MARKER;

	builder->update_call_count++;

	if (builder->update_call_count == builder->num_columns)
	{
		builder->last_hash_value = builder->accumulated_hash;
		batch_metadata_builder_bloom1_update_bloom_filter_with_hash(builder->bloom_varlena,
																	builder->accumulated_hash);
		builder->update_call_count = 0;
		builder->accumulated_hash = 0;
	}
}

static void
composite_bloom_insert_to_compressed_row(void *builder_, RowCompressor *compressor)
{
	Bloom1CompositeMetadataBuilder *builder = (Bloom1CompositeMetadataBuilder *) builder_;
	Assert(builder->functions.builder_type == METADATA_BUILDER_BLOOM1_COMPOSITE);
	/* when the call to insert_to_compressed_row arrives, the updates and reset call counts should
	 * be 0 because at this stage we are collecting enough insert calls, but no longer accumulating
	 * hashes and neither resetting the bloom filter */
	Assert(builder->reset_call_count == 0);
	Assert(builder->update_call_count == 0);
	Assert(builder->accumulated_hash == 0);

	builder->insert_call_count++;

	if (builder->insert_call_count == builder->num_columns)
	{
		batch_metadata_builder_bloom1_insert_bloom_filter_to_compressed_row(builder->bloom_varlena,
																			builder
																				->bloom_attr_offset,
																			compressor);
		builder->insert_call_count = 0;
		builder->accumulated_hash = 0;
	}
}

static void
composite_bloom_reset(void *builder_, RowCompressor *compressor)
{
	Bloom1CompositeMetadataBuilder *builder = (Bloom1CompositeMetadataBuilder *) builder_;
	Assert(builder->functions.builder_type == METADATA_BUILDER_BLOOM1_COMPOSITE);
	/* when the call to reset arrives, we should have updated and flushed the bloom filter for each
	 * column so we are not collectuing insert calls and neither accumulating hashes */
	Assert(builder->insert_call_count == 0);
	Assert(builder->update_call_count == 0);
	Assert(builder->accumulated_hash == 0);

	builder->reset_call_count++;

	if (builder->reset_call_count == builder->num_columns)
	{
		struct varlena *bloom = builder->bloom_varlena;
		memset(bloom, 0, builder->allocated_varlena_bytes);
		SET_VARSIZE(bloom, builder->allocated_varlena_bytes);

		compressor->compressed_is_null[builder->bloom_attr_offset] = true;
		compressor->compressed_values[builder->bloom_attr_offset] = 0;

		builder->reset_call_count = 0;
		builder->accumulated_hash = 0;
	}
}

BatchMetadataBuilder *
batch_metadata_builder_bloom1_composite_create(const Oid *type_oids, int num_columns,
											   int bloom_attr_offset)
{
	Assert(num_columns >= 2 && num_columns <= MAX_BLOOM_FILTER_COLUMNS);

	const int varlena_bytes = batch_metadata_builder_bloom1_varlena_size();
	Bloom1CompositeMetadataBuilder *builder = palloc0(sizeof(*builder));
	*builder = (Bloom1CompositeMetadataBuilder){
		.functions =
			(BatchMetadataBuilder){
				.update_val = composite_bloom_update_val,
				.update_null = composite_bloom_update_null,
				.insert_to_compressed_row = composite_bloom_insert_to_compressed_row,
				.reset = composite_bloom_reset,
				.builder_type = METADATA_BUILDER_BLOOM1_COMPOSITE,
			},
		.num_columns = num_columns,
		.bloom_attr_offset = bloom_attr_offset,
		.allocated_varlena_bytes = varlena_bytes,
		.bloom_varlena = palloc0(varlena_bytes),
	};

	for (int i = 0; i < num_columns; i++)
	{
		builder->hash_functions[i] =
			bloom1_get_hash_function(type_oids[i], &builder->hash_finfos[i]);
		if (builder->hash_functions[i] == NULL)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("no hash function for type %u", type_oids[i])));
		}
	}

	SET_VARSIZE(builder->bloom_varlena, varlena_bytes);
	return &builder->functions;
}

uint64
batch_metadata_builder_bloom1_composite_get_last_hash(void *builder_)
{
	Bloom1CompositeMetadataBuilder *builder = (Bloom1CompositeMetadataBuilder *) builder_;
	Assert(builder->functions.builder_type == METADATA_BUILDER_BLOOM1_COMPOSITE);

	return builder->last_hash_value;
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
	BatchMetadataBuilder *temp_builder =
		batch_metadata_builder_bloom1_composite_create(type_oids, num_fields, 0);

	/* Call update functions with type conversion */
	Oid array_elemtype = ARR_ELEMTYPE(field_values_array);

	for (int i = 0; i < num_fields; i++)
	{
		if (field_nulls[i])
		{
			temp_builder->update_null(temp_builder);
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

			temp_builder->update_val(temp_builder, value);
		}
	}

	/* Get hash */
	uint64 hash = batch_metadata_builder_bloom1_composite_get_last_hash(temp_builder);

	pfree(temp_builder);

	PG_RETURN_INT64((int64) hash);
}
#endif // #ifndef NDEBUG
