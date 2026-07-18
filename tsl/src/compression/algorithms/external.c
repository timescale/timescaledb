/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

/*
 * External compression delegates whole column batches to codec functions
 * registered by the data type (see external.h).
 *
 * On-disk layout of an ExternalCompressed varlena:
 *
 *   16-byte header (vl_len_, algorithm, header_version, has_nulls, padding,
 *                   element_type)
 *   uint8 schema_len, char schema[schema_len]  \ operator class that wrote
 *   uint8 name_len,   char name[name_len]      / the batch
 *   if has_nulls:
 *       uint32 n_rows
 *       uint8 null_bitmap[(n_rows + 7) / 8]    bit i set => row i is NULL
 *   uint8 payload[]                            bytea body the codec returned
 *
 * The payload length is implied by the varlena size. Only non-null values are
 * handed to the compress function. The header bitmap handles NULLs during
 * decompression.
 *
 * Each batch records the schema-qualified name of the operator class that
 * compressed it, and decompression resolves the codec by that name. Writes
 * are configured per-column via timescaledb.compress_column_codec.
 * On decompress, the batch uses the decompress opclass it was compressed with.
 */

#include <postgres.h>
#include "guc.h"
#include <access/htup_details.h>
#include <catalog/namespace.h>
#include <catalog/pg_opclass.h>
#include <catalog/pg_type.h>
#include <commands/defrem.h>
#include <fmgr.h>
#include <lib/stringinfo.h>
#include <libpq/pqformat.h>
#include <utils/array.h>
#include <utils/builtins.h>
#include <utils/datum.h>
#include <utils/lsyscache.h>
#include <utils/syscache.h>

#include "compression/compression.h"
#include "compression_codec_am.h"
#include "external.h"

#define EXTERNAL_HEADER_VERSION 1

typedef struct ExternalCompressed
{
	CompressedDataHeaderFields;
	uint8 header_version;
	uint8 has_nulls;
	uint8 padding[5];
	Oid element_type;
	/* 8-byte alignment sentinel for the following data */
	uint64 alignment_sentinel[FLEXIBLE_ARRAY_MEMBER];
} ExternalCompressed;

static void
pg_attribute_unused() assertions(void)
{
	ExternalCompressed test_val = { .vl_len_ = { 0 } };
	/* make sure no padding bytes make it to disk */
	StaticAssertStmt(sizeof(ExternalCompressed) ==
						 sizeof(test_val.vl_len_) + sizeof(test_val.compression_algorithm) +
							 sizeof(test_val.header_version) + sizeof(test_val.has_nulls) +
							 sizeof(test_val.padding) + sizeof(test_val.element_type),
					 "ExternalCompressed wrong size");
	StaticAssertStmt(sizeof(ExternalCompressed) == 16, "ExternalCompressed wrong size");
}

#define EXTERNAL_HEADER_SIZE offsetof(ExternalCompressed, alignment_sentinel)

/*
 * Resolve the operator class a column is configured to use.
 *
 * Errors if the registration does not satisfy the codec contract
 */
static void
external_codec_for_compression(const char *opclass_qualname, Oid element_type, Oid *compress_fn,
							   NameData *opclass_schema, NameData *opclass_name)
{
	Oid opclass = ts_compression_codec_opclass_resolve(opclass_qualname,
													   element_type,
													   /* require_compress */ true,
													   compress_fn,
													   NULL);

	HeapTuple opctup = SearchSysCache1(CLAOID, ObjectIdGetDatum(opclass));
	if (!HeapTupleIsValid(opctup))
	{
		elog(ERROR, "cache lookup failed for operator class %u", opclass);
	}
	Form_pg_opclass opcform = (Form_pg_opclass) GETSTRUCT(opctup);
	namestrcpy(opclass_name, NameStr(opcform->opcname));
	namestrcpy(opclass_schema, get_namespace_name(opcform->opcnamespace));
	ReleaseSysCache(opctup);
}

/*
 * Resolve the decompress function of the operator class a batch was stamped with.
 */
static Oid
external_codec_decompress_fn(const char *schema_name, const char *opclass_name, Oid element_type)
{
	Oid am_oid = get_am_oid(COMPRESSION_CODEC_AM_NAME, /* missing_ok */ true);
	Oid namespace_oid = InvalidOid;
	Oid opclass = InvalidOid;
	if (OidIsValid(am_oid))
	{
		namespace_oid = get_namespace_oid(schema_name, /* missing_ok */ true);
	}
	if (OidIsValid(namespace_oid))
	{
		opclass = GetSysCacheOid3(CLAAMNAMENSP,
								  Anum_pg_opclass_oid,
								  ObjectIdGetDatum(am_oid),
								  PointerGetDatum(opclass_name),
								  ObjectIdGetDatum(namespace_oid));
	}
	if (!OidIsValid(opclass))
	{
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("compression codec operator class %s for type \"%s\" does not exist",
						quote_qualified_identifier(schema_name, opclass_name),
						format_type_be(element_type)),
				 errhint("Compressed batches record the operator class that wrote them; it must "
						 "keep its name while such batches exist.")));
	}

	/*
	 * A same-named opclass could have been recreated for an unrelated type
	 * since the batch was written. The contract check rejects one whose
	 * input type does not share the column type. Reading does not need the
	 * compress function.
	 */
	Oid decompress_fn;
	ts_compression_codec_opclass_functions(opclass,
										   element_type,
										   /* require_compress */ false,
										   NULL,
										   &decompress_fn);
	return decompress_fn;
}

typedef struct ExternalCompressor
{
	Compressor base;
	Oid element_type;
	Oid compress_fn;
	/* stamped into every batch for decompress-time codec resolution */
	NameData codec_schema;
	NameData codec_name;
	int16 typlen;
	bool typbyval;
	char typalign;
	/* long-lived context the buffered values are copied into */
	MemoryContext mctx;
	/* rows. values[i] is valid if !isnull[i] */
	Datum *values;
	bool *isnull;
	int nrows;
	int capacity;
	bool has_nulls;
} ExternalCompressor;

static void
external_ensure_capacity(ExternalCompressor *compressor)
{
	if (compressor->nrows < compressor->capacity)
	{
		return;
	}
	compressor->capacity *= 2;
	compressor->values = repalloc(compressor->values, sizeof(Datum) * compressor->capacity);
	compressor->isnull = repalloc(compressor->isnull, sizeof(bool) * compressor->capacity);
}

static void
external_compressor_append_datum(Compressor *pcompressor, Datum val)
{
	ExternalCompressor *compressor = (ExternalCompressor *) pcompressor;
	external_ensure_capacity(compressor);

	/*
	 * The input datum may be toasted or may live in a short-lived context.
	 * `construct_array` and the user codec need plain values, and the buffered
	 * values need to survive until finish. So we have to fully detoast/copy
	 * into our context.
	 */
	MemoryContext previous_context = MemoryContextSwitchTo(compressor->mctx);
	if (!compressor->typbyval && compressor->typlen == -1)
	{
		compressor->values[compressor->nrows] = PointerGetDatum(PG_DETOAST_DATUM_COPY(val));
	}
	else
	{
		compressor->values[compressor->nrows] =
			datumCopy(val, compressor->typbyval, compressor->typlen);
	}
	MemoryContextSwitchTo(previous_context);

	compressor->isnull[compressor->nrows] = false;
	compressor->nrows++;
}

static void
external_compressor_append_null_value(Compressor *pcompressor)
{
	ExternalCompressor *compressor = (ExternalCompressor *) pcompressor;
	external_ensure_capacity(compressor);
	compressor->values[compressor->nrows] = (Datum) 0;
	compressor->isnull[compressor->nrows] = true;
	compressor->has_nulls = true;
	compressor->nrows++;
}

static bool
external_compressor_is_full(Compressor *pcompressor, Datum val)
{
	ExternalCompressor *compressor = (ExternalCompressor *) pcompressor;
	return compressor->nrows >= TARGET_COMPRESSED_BATCH_SIZE;
}

static void *
external_compressor_finish_and_reset(Compressor *pcompressor)
{
	ExternalCompressor *compressor = (ExternalCompressor *) pcompressor;

	/* marshal the non-null values into an array for the codec */
	Datum *non_null = palloc(sizeof(Datum) * Max(compressor->nrows, 1));
	int n_non_null = 0;
	for (int i = 0; i < compressor->nrows; i++)
	{
		if (!compressor->isnull[i])
		{
			non_null[n_non_null++] = compressor->values[i];
		}
	}

	ArrayType *input = construct_array(non_null,
									   n_non_null,
									   compressor->element_type,
									   compressor->typlen,
									   compressor->typbyval,
									   compressor->typalign);

	bytea *payload =
		DatumGetByteaP(OidFunctionCall1(compressor->compress_fn, PointerGetDatum(input)));
	Size payload_len = VARSIZE_ANY_EXHDR(payload);

	const char *codec_schema = NameStr(compressor->codec_schema);
	const char *codec_name = NameStr(compressor->codec_name);
	Size schema_len = strlen(codec_schema);
	Size name_len = strlen(codec_name);
	Size name_section_len = 2 + schema_len + name_len;
	Size bitmap_len = compressor->has_nulls ? sizeof(uint32) + (compressor->nrows + 7) / 8 : 0;
	Size total = EXTERNAL_HEADER_SIZE + name_section_len + bitmap_len + payload_len;

	ExternalCompressed *compressed = palloc0(total);
	SET_VARSIZE(compressed, total);
	compressed->compression_algorithm = COMPRESSION_ALGORITHM_EXTERNAL;
	compressed->header_version = EXTERNAL_HEADER_VERSION;
	compressed->has_nulls = compressor->has_nulls ? 1 : 0;
	compressed->element_type = compressor->element_type;

	char *data = (char *) compressed + EXTERNAL_HEADER_SIZE;
	*(uint8 *) data = (uint8) schema_len;
	data++;
	memcpy(data, codec_schema, schema_len);
	data += schema_len;
	*(uint8 *) data = (uint8) name_len;
	data++;
	memcpy(data, codec_name, name_len);
	data += name_len;
	if (compressor->has_nulls)
	{
		uint32 n_rows = compressor->nrows;
		memcpy(data, &n_rows, sizeof(uint32));
		data += sizeof(uint32);
		/* the bitmap area is already zeroed by palloc0 */
		for (int i = 0; i < compressor->nrows; i++)
		{
			if (compressor->isnull[i])
			{
				data[i / 8] |= (uint8) (1 << (i % 8));
			}
		}
		data += (compressor->nrows + 7) / 8;
	}
	memcpy(data, VARDATA_ANY(payload), payload_len);

	/*
	 * The row compressor reuses this compressor for the next batch, so reset
	 * to empty. Only free by-reference values.
	 */
	if (!compressor->typbyval)
	{
		for (int i = 0; i < compressor->nrows; i++)
		{
			if (!compressor->isnull[i])
			{
				pfree(DatumGetPointer(compressor->values[i]));
			}
		}
	}
	compressor->nrows = 0;
	compressor->has_nulls = false;

	return compressed;
}

const Compressor external_compressor = {
	.append_val = external_compressor_append_datum,
	.append_null = external_compressor_append_null_value,
	.is_full = external_compressor_is_full,
	.finish = external_compressor_finish_and_reset,
};

Compressor *
external_compressor_for_opclass(Oid element_type, const char *opclass_qualname)
{
	Oid compress_fn;
	NameData codec_schema;
	NameData codec_name;
	external_codec_for_compression(opclass_qualname,
								   element_type,
								   &compress_fn,
								   &codec_schema,
								   &codec_name);

	ExternalCompressor *compressor = palloc0(sizeof(*compressor));
	compressor->base = external_compressor;
	compressor->element_type = element_type;
	compressor->compress_fn = compress_fn;
	compressor->codec_schema = codec_schema;
	compressor->codec_name = codec_name;
	compressor->mctx = CurrentMemoryContext;
	get_typlenbyvalalign(element_type,
						 &compressor->typlen,
						 &compressor->typbyval,
						 &compressor->typalign);
	compressor->capacity = TARGET_COMPRESSED_BATCH_SIZE;
	compressor->values = palloc(sizeof(Datum) * compressor->capacity);
	compressor->isnull = palloc(sizeof(bool) * compressor->capacity);
	return &compressor->base;
}

typedef struct ExternalDecompressionIterator
{
	DecompressionIterator base;
	DecompressResult *results;
	int num_results;
	int position;
	/* +1 forward, -1 reverse */
	int step;
} ExternalDecompressionIterator;

bool
external_compressed_has_nulls(const CompressedDataHeader *header)
{
	return ((const ExternalCompressed *) header)->has_nulls != 0;
}

static DecompressResult
external_decompression_iterator_try_next(DecompressionIterator *base_iter)
{
	ExternalDecompressionIterator *iter = (ExternalDecompressionIterator *) base_iter;
	if (iter->position < 0 || iter->position >= iter->num_results)
	{
		return (DecompressResult){ .is_done = true };
	}
	DecompressResult result = iter->results[iter->position];
	iter->position += iter->step;
	return result;
}

/*
 * Read one length-prefixed name from the batch's codec name section into
 * name_out (at least NAMEDATALEN bytes), returning the advanced cursor.
 */
static char *
external_read_name(char *data, const char *end, char *name_out)
{
	CheckCompressedData(data < end);
	uint8 len = *(const uint8 *) data;
	data++;
	CheckCompressedData(len > 0 && len < NAMEDATALEN && data + len <= end);
	memcpy(name_out, data, len);
	name_out[len] = '\0';
	return data + len;
}

static DecompressionIterator *
external_decompression_iterator_alloc(Datum compressed, Oid element_type, bool forward)
{
	ExternalCompressed *header = (ExternalCompressed *) PG_DETOAST_DATUM(compressed);
	CheckCompressedData(header->compression_algorithm == COMPRESSION_ALGORITHM_EXTERNAL);
	CheckCompressedData(header->header_version == EXTERNAL_HEADER_VERSION);
	CheckCompressedData(header->element_type == element_type);

	char *data = (char *) header + EXTERNAL_HEADER_SIZE;
	const char *end = (const char *) header + VARSIZE(header);

	char codec_schema[NAMEDATALEN];
	char codec_name[NAMEDATALEN];
	data = external_read_name(data, end, codec_schema);
	data = external_read_name(data, end, codec_name);

	uint32 n_rows = 0;
	const uint8 *bitmap = NULL;
	if (header->has_nulls)
	{
		CheckCompressedData(data + sizeof(uint32) <= end);
		memcpy(&n_rows, data, sizeof(uint32));
		/* Bound n_rows before it feeds bitmap pointer arithmetic or data pointer */
		CheckCompressedData(n_rows <= GLOBAL_MAX_ROWS_PER_COMPRESSION);
		data += sizeof(uint32);
		bitmap = (const uint8 *) data;
		data += (n_rows + 7) / 8;
	}

	CheckCompressedData(data <= end);
	Size payload_len = end - data;
	bytea *payload = palloc(VARHDRSZ + payload_len);
	SET_VARSIZE(payload, VARHDRSZ + payload_len);
	memcpy(VARDATA(payload), data, payload_len);

	Oid decompress_fn = external_codec_decompress_fn(codec_schema, codec_name, element_type);

	ArrayType *values_array =
		DatumGetArrayTypeP(OidFunctionCall1(decompress_fn, PointerGetDatum(payload)));

	int16 typlen;
	bool typbyval;
	char typalign;
	get_typlenbyvalalign(element_type, &typlen, &typbyval, &typalign);

	Datum *values;
	bool *value_nulls;
	int num_values;
	deconstruct_array(values_array,
					  element_type,
					  typlen,
					  typbyval,
					  typalign,
					  &values,
					  &value_nulls,
					  &num_values);

	int total_rows = header->has_nulls ? (int) n_rows : num_values;
	DecompressResult *results = palloc(sizeof(DecompressResult) * Max(total_rows, 1));
	int next_value = 0;
	for (int row = 0; row < total_rows; row++)
	{
		bool is_null = bitmap != NULL && (bitmap[row / 8] & (1 << (row % 8))) != 0;
		if (is_null)
		{
			results[row] = (DecompressResult){ .is_null = true };
		}
		else
		{
			CheckCompressedData(next_value < num_values);
			results[row] = (DecompressResult){ .val = values[next_value++] };
		}
	}
	/* every decompressed value must map to exactly one row */
	CheckCompressedData(next_value == num_values);

	ExternalDecompressionIterator *iterator = palloc0(sizeof(*iterator));
	iterator->base.compression_algorithm = COMPRESSION_ALGORITHM_EXTERNAL;
	iterator->base.forward = forward;
	iterator->base.element_type = element_type;
	iterator->base.try_next = external_decompression_iterator_try_next;
	iterator->results = results;
	iterator->num_results = total_rows;
	iterator->position = forward ? 0 : total_rows - 1;
	iterator->step = forward ? 1 : -1;
	return &iterator->base;
}

DecompressionIterator *
tsl_external_decompression_iterator_from_datum_forward(Datum compressed, Oid element_type)
{
	return external_decompression_iterator_alloc(compressed, element_type, /* forward */ true);
}

DecompressionIterator *
tsl_external_decompression_iterator_from_datum_reverse(Datum compressed, Oid element_type)
{
	return external_decompression_iterator_alloc(compressed, element_type, /* forward */ false);
}

/*
 * The body after the header is opaque and self-describing, so send/recv only
 * have to ship it verbatim along with the header fields.
 */
void
external_compressed_send(CompressedDataHeader *header, StringInfo buffer)
{
	ExternalCompressed *compressed = (ExternalCompressed *) header;
	uint32 body_len = VARSIZE(compressed) - EXTERNAL_HEADER_SIZE;
	pq_sendbyte(buffer, compressed->header_version);
	pq_sendint32(buffer, compressed->element_type);
	pq_sendbyte(buffer, compressed->has_nulls);
	pq_sendint32(buffer, body_len);
	pq_sendbytes(buffer, (char *) compressed + EXTERNAL_HEADER_SIZE, body_len);
}

Datum
external_compressed_recv(StringInfo buffer)
{
	uint8 header_version = pq_getmsgbyte(buffer);
	CheckCompressedData(header_version == EXTERNAL_HEADER_VERSION);
	Oid element_type = pq_getmsgint(buffer, 4);
	uint8 has_nulls = pq_getmsgbyte(buffer);
	uint32 body_len = pq_getmsgint(buffer, 4);
	CheckCompressedData(body_len < MaxAllocSize - EXTERNAL_HEADER_SIZE);

	Size total = EXTERNAL_HEADER_SIZE + body_len;
	ExternalCompressed *compressed = palloc0(total);
	SET_VARSIZE(compressed, total);
	compressed->compression_algorithm = COMPRESSION_ALGORITHM_EXTERNAL;
	compressed->header_version = header_version;
	compressed->has_nulls = has_nulls;
	compressed->element_type = element_type;
	memcpy((char *) compressed + EXTERNAL_HEADER_SIZE, pq_getmsgbytes(buffer, body_len), body_len);

	PG_RETURN_POINTER(compressed);
}
