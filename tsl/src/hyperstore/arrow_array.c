/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <postgres.h>
#include <access/tupmacs.h>
#include <fmgr.h>
#include <utils/datum.h>
#include <utils/palloc.h>

#include "arrow_array.h"
#include "compression/arrow_c_data_interface.h"
#include "compression/compression.h"

#define TYPLEN_VARLEN (-1)

/*
 * Extend a buffer if necessary.
 *
 * We double the memory because we want to amortize the allocation cost to so
 * that it becomes O(n). The new memory will be allocated in the same memory
 * context as the memory was originally allocated in.
 */
#define EXTEND_BUFFER_IF_NEEDED(BUFFER, NEEDED, CAPACITY)                                          \
	do                                                                                             \
	{                                                                                              \
		if ((unsigned long) (NEEDED) >= (unsigned long) (CAPACITY))                                \
		{                                                                                          \
			(CAPACITY) *= 2;                                                                       \
			(BUFFER) = repalloc((BUFFER), (CAPACITY));                                             \
		}                                                                                          \
	} while (0)

/*
 * Release buffer memory.
 */
static void
arrow_release_buffers(ArrowArray *array)
{
	/*
	 * The recommended release function frees child nodes and the dictionary
	 * in the Arrow array, but, currently, the child array is not used so we
	 * do not care about it.
	 */
	Assert(array->children == NULL);

	for (int64 i = 0; i < array->n_buffers; ++i)
	{
		/* Validity bitmap might be NULL even if it is counted
		 * in n_buffers, so need to check for NULL values. */
		if (array->buffers[i] != NULL)
		{
			pfree((void *) array->buffers[i]);
			array->buffers[i] = NULL; /* Just a precaution to avoid a dangling reference */
		}
	}

	array->n_buffers = 0;

	if (array->dictionary)
	{
		arrow_release_buffers(array->dictionary);
		array->dictionary = NULL;
	}
}

/*
 * Variable-size primitive layout ArrowArray from decompression iterator.
 */
static ArrowArray *
arrow_from_iterator_varlen(MemoryContext mcxt, DecompressionIterator *iterator, Oid typid)
{
	int64 offsets_capacity =
		sizeof(int32) * 128; /* Starting capacity of the offset buffer in bytes */
	int64 data_capacity = 4 * offsets_capacity; /* Starting capacity of the data buffer in bytes */
	int64 validity_capacity = sizeof(uint64) * (pad_to_multiple(64, offsets_capacity) / 64);
	int32 endpos = 0; /* Can be 32 or 64 bits signed integers */
	int64 array_length;
	int64 null_count = 0;
	int32 *offsets_buffer = MemoryContextAlloc(mcxt, offsets_capacity);
	uint8 *data_buffer = MemoryContextAlloc(mcxt, data_capacity);
	uint64 *validity_buffer = MemoryContextAlloc(mcxt, validity_capacity);

	/* Just a precaution: type should be varlen */
	Assert(get_typlen(typid) == TYPLEN_VARLEN);

	/* First offset is always zero and there are length + 1 offsets */
	offsets_buffer[0] = 0;

	for (array_length = 0;; ++array_length)
	{
		DecompressResult result = iterator->try_next(iterator);

		if (result.is_done)
			break;

		TS_DEBUG_LOG("storing %s varlen value row " INT64_FORMAT
					 " at offset %d (varlen size %lu, offset "
					 "capacity " INT64_FORMAT ", data capacity " INT64_FORMAT ")",
					 datum_as_string(typid, result.val, result.is_null),
					 array_length,
					 endpos,
					 (unsigned long) VARSIZE_ANY(result.val), /* cast for 32-bit builds */
					 offsets_capacity,
					 data_capacity);

		/* Offsets buffer contains array_length + 1 offsets */
		EXTEND_BUFFER_IF_NEEDED(offsets_buffer,
								sizeof(*offsets_buffer) * (array_length + 1),
								offsets_capacity);
		EXTEND_BUFFER_IF_NEEDED(validity_buffer,
								sizeof(uint64) * (pad_to_multiple(64, array_length) / 64),
								validity_capacity);

		arrow_set_row_validity(validity_buffer, array_length, !result.is_null);

		if (result.is_null)
			++null_count;
		else
		{
			/* We store all the varlen data here, including header, so we are
			 * not strictly following the arrow format. */
			const int varlen = VARSIZE_ANY(result.val);
			EXTEND_BUFFER_IF_NEEDED(data_buffer, endpos + varlen, data_capacity);
			memcpy(&data_buffer[endpos], DatumGetPointer(result.val), varlen);
			endpos += varlen;
		}

		offsets_buffer[array_length + 1] = endpos;
	}

	ArrowArray *array = arrow_create_with_buffers(mcxt, 3);
	array->length = array_length;
	array->buffers[0] = validity_buffer;
	array->buffers[1] = offsets_buffer;
	array->buffers[2] = data_buffer;
	array->null_count = null_count;
	array->release = arrow_release_buffers;
	return array;
}

/*
 * Fixed-Size Primitive layout ArrowArray from decompression iterator.
 */
static ArrowArray *
arrow_from_iterator_fixlen(MemoryContext mcxt, DecompressionIterator *iterator, Oid typid)
{
	const int typlen = get_typlen(typid);
	const bool typbyval = get_typbyval(typid);
	int64 data_capacity = 64 * typlen; /* Capacity of the data buffer */
	int64 validity_capacity = sizeof(uint64) * (pad_to_multiple(64, data_capacity) / 64);
	uint8 *data_buffer = MemoryContextAlloc(mcxt, data_capacity * sizeof(uint8));
	uint64 *validity_buffer = MemoryContextAlloc(mcxt, validity_capacity);
	int64 array_length;
	int64 null_count = 0;

	/* Just a precaution: this should not be a varlen type */
	Assert(typlen > 0);

	for (array_length = 0;; ++array_length)
	{
		DecompressResult result = iterator->try_next(iterator);

		if (result.is_done)
			break;

		EXTEND_BUFFER_IF_NEEDED(validity_buffer, array_length / 8, validity_capacity);
		EXTEND_BUFFER_IF_NEEDED(data_buffer, typlen * array_length, data_capacity);

		arrow_set_row_validity(validity_buffer, array_length, !result.is_null);

		if (result.is_null)
			++null_count;
		else if (typbyval)
		{
			/*
			 * We use unsigned integers to avoid conversions between signed
			 * and unsigned values (which in theory could change the value)
			 * when converting to datum (which is an unsigned value).
			 *
			 * Conversions between unsigned values is well-defined in the C
			 * standard and will work here.
			 */
			switch (typlen)
			{
				case sizeof(uint8):
					data_buffer[array_length] = DatumGetUInt8(result.val);
					break;
				case sizeof(uint16):
					((uint16 *) data_buffer)[array_length] = DatumGetUInt16(result.val);
					break;
				case sizeof(uint32):
					((uint32 *) data_buffer)[array_length] = DatumGetUInt32(result.val);
					break;
				case sizeof(uint64):
					/* This branch is not called for by-reference 64-bit values */
					((uint64 *) data_buffer)[array_length] = DatumGetUInt64(result.val);
					break;
				default:
					ereport(ERROR,
							errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("not supporting writing by value length %d", typlen));
			}
		}
		else
		{
			memcpy(&data_buffer[typlen * array_length], DatumGetPointer(result.val), typlen);
		}
	}

	ArrowArray *array = arrow_create_with_buffers(mcxt, 2);
	array->length = array_length;
	array->buffers[0] = validity_buffer;
	array->buffers[1] = data_buffer;
	array->null_count = null_count;
	array->release = arrow_release_buffers;
	return array;
}

/*
 * Read the entire contents of a decompression iterator into the arrow array.
 */
static ArrowArray *
arrow_from_iterator(MemoryContext mcxt, DecompressionIterator *iterator, Oid typid)
{
	const int typlen = get_typlen(typid);
	if (typlen == TYPLEN_VARLEN)
		return arrow_from_iterator_varlen(mcxt, iterator, typid);
	else
		return arrow_from_iterator_fixlen(mcxt, iterator, typid);
}

static ArrowArray *
arrow_generic_decompress_all(Datum compressed, Oid element_type, MemoryContext dest_mctx)
{
	/* Slightly weird interface for passing the header, but this is what the
	 * other decompress_all functions are using. We might want to refactor
	 * this later. */
	const CompressedDataHeader *header =
		(const CompressedDataHeader *) PG_DETOAST_DATUM(compressed);
	DecompressionInitializer initializer =
		tsl_get_decompression_iterator_init(header->compression_algorithm, false);
	DecompressionIterator *iterator = initializer(compressed, element_type);
	return arrow_from_iterator(dest_mctx, iterator, element_type);
}

static DecompressAllFunction
arrow_get_decompress_all(uint8 compression_alg, Oid typid)
{
	DecompressAllFunction decompress_all = NULL;

	decompress_all = tsl_get_decompress_all_function(compression_alg, typid);

	if (decompress_all == NULL)
		decompress_all = arrow_generic_decompress_all;

	Assert(decompress_all != NULL);
	return decompress_all;
}

#ifdef USE_ASSERT_CHECKING
static bool
verify_offsets(const ArrowArray *array)
{
	if (array->n_buffers == 3)
	{
		const int32 *offsets = array->buffers[1];

		for (int64 i = 0; i < array->length; ++i)
			if (offsets[i + 1] < offsets[i])
				return false;
	}
	return true;
}
#endif

ArrowArray *
arrow_from_compressed(Datum compressed, Oid typid, MemoryContext dest_mcxt, MemoryContext tmp_mcxt)
{
	const CompressedDataHeader *header = (CompressedDataHeader *) PG_DETOAST_DATUM(compressed);
	DecompressAllFunction decompress_all =
		arrow_get_decompress_all(header->compression_algorithm, typid);

	TS_DEBUG_LOG("decompressing column with type %s using decompression algorithm %s",
				 format_type_be(typid),
				 NameStr(*compression_get_algorithm_name(header->compression_algorithm)));

	MemoryContext oldcxt = MemoryContextSwitchTo(tmp_mcxt);
	ArrowArray *array = decompress_all(PointerGetDatum(header), typid, dest_mcxt);

	Assert(verify_offsets(array));

	/*
	 * If the release function is not set, it is the old-style decompress_all
	 * and then buffers should be deleted by default.
	 */
	if (array->release == NULL)
		array->release = arrow_release_buffers;

	/*
	 * Not sure how necessary this reset is, but keeping it for now.
	 *
	 * The amount of data is bounded by the number of columns in the tuple
	 * table slot, so it might be possible to skip this reset.
	 */
	MemoryContextReset(tmp_mcxt);
	MemoryContextSwitchTo(oldcxt);

	return array;
}

/*
 * Get varlen datum from arrow array.
 *
 * This will always be a reference.
 */
static NullableDatum
arrow_get_datum_varlen(const ArrowArray *array, Oid typid, int64 index)
{
	const uint64 *restrict validity = array->buffers[0];
	const int32 *offsets;
	const uint8 *data;
	Datum value;

	if (!arrow_row_is_valid(validity, index))
		return (NullableDatum){ .isnull = true };

	if (array->dictionary)
	{
		const ArrowArray *dict = array->dictionary;
		const int16 *indexes = (int16 *) array->buffers[1];
		index = indexes[index];
		offsets = dict->buffers[1];
		data = dict->buffers[2];
	}
	else
	{
		offsets = array->buffers[1];
		data = array->buffers[2];
	}

	const int32 offset = offsets[index];
	const int32 datalen = offsets[index + 1] - offset;

	/* Need to handle text as a special case because the cstrings are stored
	 * back-to-back without varlena header */
	if (typid == TEXTOID)
	{
		const int32 varlen = VARHDRSZ + datalen;
		value = (Datum) palloc(varlen);
		SET_VARSIZE(value, varlen);
		memcpy(VARDATA_ANY(value), &data[offset], datalen);
	}
	else
		value = PointerGetDatum(&data[offset]);

	/* We have stored the bytes of the varlen value directly in the buffer, so
	 * this should work as expected. */
	TS_DEBUG_LOG("retrieved varlen value '%s' row " INT64_FORMAT
				 " from offset %d dictionary=%p in memory context %s",
				 datum_as_string(typid, value, false),
				 index,
				 offset,
				 array->dictionary,
				 GetMemoryChunkContext((void *) data)->name);

	return (NullableDatum){ .isnull = false, .value = value };
}

/*
 * Get a fixed-length datum from the arrow array.
 *
 * This handles lengths that are not more than 8 bytes currently. We probably
 * need to copy some of the code from `datumSerialize` (which is used to
 * serialize datums for transfer to parallel workers) to serialize arbitrary
 * data into an arrow array.
 */
static NullableDatum
arrow_get_datum_fixlen(ArrowArray *array, Oid typid, int64 index)
{
	const int typlen = get_typlen(typid);
	const bool typbyval = get_typbyval(typid);
	const uint64 *restrict validity = array->buffers[0];
	const char *restrict values = array->buffers[1];

	Assert(typlen > 0);

	if (!arrow_row_is_valid(validity, index))
		return (NullableDatum){ .isnull = true };

	/* In order to handle fixed-length values of arbitrary size that are byref
	 * and byval, we use fetch_all() rather than rolling our own. This is
	 * taken from utils/adt/rangetypes.c */
	Datum datum = fetch_att(&values[index * typlen], typbyval, typlen);

	TS_DEBUG_LOG("retrieved fixlen value %s row " INT64_FORMAT " from offset " INT64_FORMAT
				 " in memory context %s",
				 datum_as_string(typid, datum, false),
				 index,
				 typlen * index,
				 GetMemoryChunkContext((void *) values)->name);

	return (NullableDatum){ .isnull = false, .value = datum };
}

NullableDatum
arrow_get_datum(ArrowArray *array, Oid typid, int64 index)
{
	if (get_typlen(typid) == TYPLEN_VARLEN)
		return arrow_get_datum_varlen(array, typid, index);
	else
		return arrow_get_datum_fixlen(array, typid, index);
}

/*
 * Create an arrow array with memory for buffers.
 *
 * The space for buffers are allocated after the main structure.
 */
ArrowArray *
arrow_create_with_buffers(MemoryContext mcxt, int n_buffers)
{
	struct
	{
		ArrowArray array;
		const void *buffers[FLEXIBLE_ARRAY_MEMBER];
	} *array_with_buffers =
		MemoryContextAllocZero(mcxt, sizeof(ArrowArray) + sizeof(const void *) * n_buffers);

	ArrowArray *array = &array_with_buffers->array;

	array->n_buffers = n_buffers;
	array->buffers = array_with_buffers->buffers;

	return array;
}
