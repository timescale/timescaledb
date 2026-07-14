/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

/*
 * External compression delegates whole column batches to the data type's own
 * compress/decompress SQL functions (see external.h for the contract).
 *
 * On-disk layout of an ExternalCompressed varlena:
 *
 *   16-byte header (vl_len_, algorithm, has_nulls, padding, element_type)
 *   if has_nulls:
 *       uint32 n_rows
 *       uint8 null_bitmap[(n_rows + 7) / 8]    bit i set => row i is NULL
 *   uint8 payload[]                            bytea body the codec returned
 *
 * The payload length is implied by the varlena size. Only non-null values are
 * handed to the compress function. The header bitmap handles NULLs during
 * decompression.
 */

#include <postgres.h>
#include "guc.h"
#include <catalog/pg_type.h>
#include <fmgr.h>
#include <lib/stringinfo.h>
#include <libpq/pqformat.h>
#include <nodes/makefuncs.h>
#include <parser/parse_func.h>
#include <utils/array.h>
#include <utils/builtins.h>
#include <utils/catcache.h>
#include <utils/datum.h>
#include <utils/hsearch.h>
#include <utils/inval.h>
#include <utils/lsyscache.h>
#include <utils/memutils.h>
#include <utils/syscache.h>

#include "compression/compression.h"
#include "external.h"

typedef struct ExternalCompressed
{
	CompressedDataHeaderFields;
	uint8 has_nulls;
	uint8 padding[6];
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
							 sizeof(test_val.has_nulls) + sizeof(test_val.padding) +
							 sizeof(test_val.element_type),
					 "ExternalCompressed wrong size");
	StaticAssertStmt(sizeof(ExternalCompressed) == 16, "ExternalCompressed wrong size");
}

#define EXTERNAL_HEADER_SIZE offsetof(ExternalCompressed, alignment_sentinel)

static bool
external_codec_lookup_uncached(Oid type_oid, Oid *compress_fn, Oid *decompress_fn)
{
	Oid array_type = get_array_type(type_oid);
	if (!OidIsValid(array_type))
	{
		return false;
	}

	HeapTuple type_tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(type_oid));
	if (!HeapTupleIsValid(type_tuple))
	{
		return false;
	}
	Form_pg_type type_form = (Form_pg_type) GETSTRUCT(type_tuple);
	char *type_name = pstrdup(NameStr(type_form->typname));
	Oid type_namespace = type_form->typnamespace;
	ReleaseSysCache(type_tuple);

	char *schema_name = get_namespace_name(type_namespace);
	if (schema_name == NULL)
	{
		return false;
	}

	List *compress_name =
		list_make2(makeString(schema_name), makeString(psprintf("%s_compress", type_name)));
	Oid compress_argtypes[1] = { array_type };
	Oid compress_oid = LookupFuncName(compress_name, 1, compress_argtypes, /* missing_ok */ true);

	List *decompress_name =
		list_make2(makeString(schema_name), makeString(psprintf("%s_decompress", type_name)));
	Oid decompress_argtypes[1] = { BYTEAOID };
	Oid decompress_oid =
		LookupFuncName(decompress_name, 1, decompress_argtypes, /* missing_ok */ true);

	if (!OidIsValid(compress_oid) || !OidIsValid(decompress_oid))
	{
		return false;
	}

	*compress_fn = compress_oid;
	*decompress_fn = decompress_oid;
	return true;
}

/*
 * Backend-local cache over external_codec_lookup_uncached, which walks
 * pg_type, pg_namespace and pg_proc for every batch during
 * decompression. Algorithm selection probes every non-builtin column
 * type. All entries are flushed when any of those catalogs change, so
 * dropping or (re)creating codec functions is seen by the next lookup.
 */
typedef struct ExternalCodecCacheEntry
{
	Oid type_oid;
	bool has_codec;
	Oid compress_fn;
	Oid decompress_fn;
} ExternalCodecCacheEntry;

static HTAB *external_codec_cache = NULL;

static void
external_codec_cache_invalidate(Datum arg, int cacheid, uint32 hashvalue)
{
	HASH_SEQ_STATUS status;
	ExternalCodecCacheEntry *entry;

	hash_seq_init(&status, external_codec_cache);
	while ((entry = hash_seq_search(&status)) != NULL)
	{
		hash_search(external_codec_cache, &entry->type_oid, HASH_REMOVE, NULL);
	}
}

static HTAB *
external_codec_cache_get(void)
{
	if (external_codec_cache == NULL)
	{
		if (CacheMemoryContext == NULL)
		{
			CreateCacheMemoryContext();
		}
		HASHCTL ctl = {
			.keysize = sizeof(Oid),
			.entrysize = sizeof(ExternalCodecCacheEntry),
			.hcxt = CacheMemoryContext,
		};
		external_codec_cache =
			hash_create("external codec cache", 8, &ctl, HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
		CacheRegisterSyscacheCallback(PROCOID, external_codec_cache_invalidate, 0);
		CacheRegisterSyscacheCallback(TYPEOID, external_codec_cache_invalidate, 0);
		CacheRegisterSyscacheCallback(NAMESPACEOID, external_codec_cache_invalidate, 0);
	}
	return external_codec_cache;
}

bool
external_codec_lookup(Oid type_oid, Oid *compress_fn, Oid *decompress_fn)
{
	HTAB *cache = external_codec_cache_get();
	bool found;
	ExternalCodecCacheEntry *entry = hash_search(cache, &type_oid, HASH_FIND, &found);
	if (!found)
	{
		/*
		 * Look up before entering into the cache: the catalog access can
		 * accept invalidations and flush the cache mid-lookup, which would
		 * invalidate the entry pointer.
		 */
		Oid compress_oid = InvalidOid;
		Oid decompress_oid = InvalidOid;
		bool has_codec =
			external_codec_lookup_uncached(type_oid, &compress_oid, &decompress_oid);

		entry = hash_search(cache, &type_oid, HASH_ENTER, NULL);
		entry->has_codec = has_codec;
		entry->compress_fn = compress_oid;
		entry->decompress_fn = decompress_oid;
	}

	if (!entry->has_codec)
	{
		return false;
	}
	if (compress_fn != NULL)
	{
		*compress_fn = entry->compress_fn;
	}
	if (decompress_fn != NULL)
	{
		*decompress_fn = entry->decompress_fn;
	}
	return true;
}

typedef struct ExternalCompressor
{
	Compressor base;
	Oid element_type;
	Oid compress_fn;
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
		compressor->values[compressor->nrows] = datumCopy(val, compressor->typbyval, compressor->typlen);
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

	Size bitmap_len = compressor->has_nulls ? sizeof(uint32) + (compressor->nrows + 7) / 8 : 0;
	Size total = EXTERNAL_HEADER_SIZE + bitmap_len + payload_len;

	ExternalCompressed *compressed = palloc0(total);
	SET_VARSIZE(compressed, total);
	compressed->compression_algorithm = COMPRESSION_ALGORITHM_EXTERNAL;
	compressed->has_nulls = compressor->has_nulls ? 1 : 0;
	compressed->element_type = compressor->element_type;

	char *data = (char *) compressed + EXTERNAL_HEADER_SIZE;
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
external_compressor_for_type(Oid element_type)
{
	Oid compress_fn;
	if (!external_codec_lookup(element_type, &compress_fn, NULL))
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("no external compression functions for type \"%s\"",
						format_type_be(element_type))));
	}

	ExternalCompressor *compressor = palloc0(sizeof(*compressor));
	compressor->base = external_compressor;
	compressor->element_type = element_type;
	compressor->compress_fn = compress_fn;
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
		return (DecompressResult) { .is_done = true };
	}
	DecompressResult result = iter->results[iter->position];
	iter->position += iter->step;
	return result;
}

static DecompressionIterator *
external_decompression_iterator_alloc(Datum compressed, Oid element_type, bool forward)
{
	ExternalCompressed *header = (ExternalCompressed *) PG_DETOAST_DATUM(compressed);
	CheckCompressedData(header->compression_algorithm == COMPRESSION_ALGORITHM_EXTERNAL);
	CheckCompressedData(header->element_type == element_type);

	char *data = (char *) header + EXTERNAL_HEADER_SIZE;
	uint32 n_rows = 0;
	const uint8 *bitmap = NULL;
	if (header->has_nulls)
	{
		CheckCompressedData(VARSIZE(header) >= EXTERNAL_HEADER_SIZE + sizeof(uint32));
		memcpy(&n_rows, data, sizeof(uint32));
		/* Bound n_rows before it feeds bitmap pointer arithmetic or data pointer */
		CheckCompressedData(n_rows <= GLOBAL_MAX_ROWS_PER_COMPRESSION);
		data += sizeof(uint32);
		bitmap = (const uint8 *) data;
		data += (n_rows + 7) / 8;
	}

	CheckCompressedData(data <= (char *) header + VARSIZE(header));
	Size payload_len = VARSIZE(header) - (data - (char *) header);
	bytea *payload = palloc(VARHDRSZ + payload_len);
	SET_VARSIZE(payload, VARHDRSZ + payload_len);
	memcpy(VARDATA(payload), data, payload_len);

	Oid decompress_fn;
	if (!external_codec_lookup(element_type, NULL, &decompress_fn))
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("no external decompression functions for type \"%s\"",
						format_type_be(element_type))));
	}

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
			results[row] = (DecompressResult) { .is_null = true };
		}
		else
		{
			CheckCompressedData(next_value < num_values);
			results[row] = (DecompressResult) { .val = values[next_value++] };
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
	pq_sendint32(buffer, compressed->element_type);
	pq_sendbyte(buffer, compressed->has_nulls);
	pq_sendint32(buffer, body_len);
	pq_sendbytes(buffer, (char *) compressed + EXTERNAL_HEADER_SIZE, body_len);
}

Datum
external_compressed_recv(StringInfo buffer)
{
	Oid element_type = pq_getmsgint(buffer, 4);
	uint8 has_nulls = pq_getmsgbyte(buffer);
	uint32 body_len = pq_getmsgint(buffer, 4);
	CheckCompressedData(body_len < MaxAllocSize - EXTERNAL_HEADER_SIZE);

	Size total = EXTERNAL_HEADER_SIZE + body_len;
	ExternalCompressed *compressed = palloc0(total);
	SET_VARSIZE(compressed, total);
	compressed->compression_algorithm = COMPRESSION_ALGORITHM_EXTERNAL;
	compressed->has_nulls = has_nulls;
	compressed->element_type = element_type;
	memcpy((char *) compressed + EXTERNAL_HEADER_SIZE, pq_getmsgbytes(buffer, body_len), body_len);

	PG_RETURN_POINTER(compressed);
}
