/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

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

#pragma once

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
	const size_t qword_index = row_number / 64;
	const size_t bit_index = row_number % 64;
	const uint64 mask = 1ull << bit_index;
	return bitmap[qword_index] & mask;
}

static pg_attribute_always_inline void
arrow_set_row_validity(uint64 *bitmap, size_t row_number, bool value)
{
	const size_t qword_index = row_number / 64;
	const size_t bit_index = row_number % 64;
	const uint64 mask = 1ull << bit_index;

	bitmap[qword_index] = (bitmap[qword_index] & ~mask) | ((-(uint64) value) & mask);

	Assert(arrow_row_is_valid(bitmap, row_number) == value);
}
