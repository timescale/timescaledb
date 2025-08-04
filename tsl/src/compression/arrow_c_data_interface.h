/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

/*
 * This header describes the Arrow C data interface which is a well-known
 * standard for in-memory interchange of columnar data.
 *
 * https://arrow.apache.org/docs/format/CDataInterface.html
 *
 * Citing from the above link:
 *
 * Arrow C data interface defines a very small, stable set of C definitions that
 * can be easily copied in any project’s source code and used for columnar data
 * interchange in the Arrow format. For non-C/C++ languages and runtimes, it
 * should be almost as easy to translate the C definitions into the
 * corresponding C FFI declarations.
 *
 * Applications and libraries can therefore work with Arrow memory without
 * necessarily using Arrow libraries or reinventing the wheel.
 */

#ifndef ARROW_C_DATA_INTERFACE
#define ARROW_C_DATA_INTERFACE

#define ARROW_FLAG_DICTIONARY_ORDERED 1
#define ARROW_FLAG_NULLABLE 2
#define ARROW_FLAG_MAP_KEYS_SORTED 4

typedef struct ArrowArray
{
	/*
	 * Mandatory. The logical length of the array (i.e. its number of items).
	 */
	int64 length;

	/*
	 * Mandatory. The number of null items in the array. MAY be -1 if not yet
	 * computed.
	 */
	int64 null_count;

	/*
	 * Mandatory. The logical offset inside the array (i.e. the number of
	 * items from the physical start of the buffers). MUST be 0 or positive.
	 *
	 * Producers MAY specify that they will only produce 0-offset arrays to
	 * ease implementation of consumer code. Consumers MAY decide not to
	 * support non-0-offset arrays, but they should document this limitation.
	 */
	int64 offset;

	/*
	 * Mandatory. The number of physical buffers backing this array. The
	 * number of buffers is a function of the data type, as described in the
	 * Columnar format specification.
	 *
	 * Buffers of children arrays are not included.
	 */
	int64 n_buffers;

	/*
	 * Mandatory. The number of children this type has.
	 */
	int64 n_children;

	/*
	 * Mandatory. A C array of pointers to the start of each physical buffer
	 * backing this array. Each void* pointer is the physical start of a
	 * contiguous buffer. There must be ArrowArray.n_buffers pointers.
	 *
	 * The producer MUST ensure that each contiguous buffer is large enough to
	 * represent length + offset values encoded according to the Columnar
	 * format specification.
	 *
	 * It is recommended, but not required, that the memory addresses of the
	 * buffers be aligned at least according to the type of primitive data
	 * that they contain. Consumers MAY decide not to support unaligned
	 * memory.
	 *
	 * The buffer pointers MAY be null only in two situations:
	 *
	 * - for the null bitmap buffer, if ArrowArray.null_count is 0;
	 *
	 * - for any buffer, if the size in bytes of the corresponding buffer would
	 * be 0.
	 *
	 * Buffers of children arrays are not included.
	 */
	const void **buffers;

	struct ArrowArray **children;
	struct ArrowArray *dictionary;

	/*
	 * Mandatory. A pointer to a producer-provided release callback.
	 *
	 * See below for memory management and release callback semantics.
	 */
	void (*release)(struct ArrowArray *);

	/* Opaque producer-specific data */
	void *private_data;
} ArrowArray;

/*
 * We don't use the schema but have to define it for completeness because we're
 * defining the ARROW_C_DATA_INTERFACE macro.
 */
struct ArrowSchema
{
	const char *format;
	const char *name;
	const char *metadata;
	int64 flags;
	int64 n_children;
	struct ArrowSchema **children;
	struct ArrowSchema *dictionary;

	void (*release)(struct ArrowSchema *);
	void *private_data;
};

/*
 * The include guard ARROW_C_DATA_INTERFACE is required by the Arrow docs to
 * avoid redefinition of the Arrow structs in the third-party headers, but the
 * following functions are not part of Arrow C Data Interface, so they are not
 * under the guard. We still need some kind of guard for them, so we also have
 * pragma once above.
 */
#endif

static pg_attribute_always_inline bool
arrow_row_is_valid(const uint64 *bitmap, size_t row_number)
{
	if (likely(bitmap == NULL))
	{
		return true;
	}

	const size_t qword_index = row_number / 64;
	const size_t bit_index = row_number % 64;
	const uint64 mask = 1ull << bit_index;
	return bitmap[qword_index] & mask;
}

/*
 * Same as above but for two bitmaps, this is a typical situation when we have
 * validity bitmap + filter result.
 */
static pg_attribute_always_inline bool
arrow_row_both_valid(const uint64 *bitmap1, const uint64 *bitmap2, size_t row_number)
{
	if (likely(bitmap1 == NULL))
	{
		return arrow_row_is_valid(bitmap2, row_number);
	}

	if (likely(bitmap2 == NULL))
	{
		return arrow_row_is_valid(bitmap1, row_number);
	}

	const size_t qword_index = row_number / 64;
	const size_t bit_index = row_number % 64;
	const uint64 mask = 1ull << bit_index;
	return (bitmap1[qword_index] & bitmap2[qword_index]) & mask;
}

static pg_attribute_always_inline void
arrow_set_row_validity(uint64 *bitmap, size_t row_number, bool value)
{
	const size_t qword_index = row_number / 64;
	const size_t bit_index = row_number % 64;
	const uint64 mask = 1ull << bit_index;
	const uint64 new_bit = (value ? 1ull : 0ull) << bit_index;

	bitmap[qword_index] = (bitmap[qword_index] & ~mask) | new_bit;

	Assert(arrow_row_is_valid(bitmap, row_number) == value);
}

/*
 * Combine the validity bitmaps into the given storage.
 */
static inline const uint64 *
arrow_combine_validity(size_t num_words, uint64 *restrict storage, const uint64 *filter1,
					   const uint64 *filter2, const uint64 *filter3)
{
	/*
	 * Any and all of the filters can be null. For simplicity, move the non-null
	 * filters to the leading positions.
	 */
	const uint64 *tmp;
#define SWAP(X, Y)                                                                                 \
	tmp = (X);                                                                                     \
	(X) = (Y);                                                                                     \
	(Y) = tmp;

	if (filter1 == NULL)
	{
		/*
		 * We have at least one NULL that goes to the last position.
		 */
		SWAP(filter1, filter3);

		if (filter1 == NULL)
		{
			/*
			 * We have another NULL that goes to the second position.
			 */
			SWAP(filter1, filter2);
		}
	}
	else
	{
		if (filter2 == NULL)
		{
			/*
			 * We have at least one NULL that goes to the last position.
			 */
			SWAP(filter2, filter3);
		}
	}
#undef SWAP

	Assert(filter2 == NULL || filter1 != NULL);
	Assert(filter3 == NULL || filter2 != NULL);

	if (filter2 == NULL)
	{
		/* Either have one non-null filter, or all of them are null. */
		return filter1;
	}

	if (filter3 == NULL)
	{
		/* Have two non-null filters. */
		for (size_t i = 0; i < num_words; i++)
		{
			storage[i] = filter1[i] & filter2[i];
		}
	}
	else
	{
		/* Have three non-null filters. */
		for (size_t i = 0; i < num_words; i++)
		{
			storage[i] = filter1[i] & filter2[i] & filter3[i];
		}
	}

	return storage;
}

/*
 * Increase the `source_value` to be an even multiple of `pad_to`.
 */
static inline uint64
pad_to_multiple(uint64 pad_to, uint64 source_value)
{
	return ((source_value + pad_to - 1) / pad_to) * pad_to;
}

static inline int
arrow_num_valid(const uint64 *bitmap, size_t total_rows)
{
	if (bitmap == NULL)
	{
		return total_rows;
	}

	uint64 num_valid = 0;
#ifdef HAVE__BUILTIN_POPCOUNT
	const uint64 words = pad_to_multiple(64, total_rows) / 64;
	for (uint64 i = 0; i < words; i++)
	{
		num_valid += __builtin_popcountll(bitmap[i]);
	}
#else
	for (size_t i = 0; i < total_rows; i++)
	{
		num_valid += arrow_row_is_valid(bitmap, i);
	}
#endif
	return num_valid;
}
