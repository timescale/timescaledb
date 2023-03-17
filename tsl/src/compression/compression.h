/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_TSL_COMPRESSION_COMPRESSION_H
#define TIMESCALEDB_TSL_COMPRESSION_COMPRESSION_H

#include <postgres.h>

#include <c.h>
#include <executor/tuptable.h>
#include <fmgr.h>
#include <lib/stringinfo.h>
#include <utils/date.h>
#include <utils/lsyscache.h>
#include <utils/relcache.h>
#include <utils/timestamp.h>

/* Normal compression uses 1k rows, but the regression tests use up to 1015. */
#ifndef GLOBAL_MAX_ROWS_PER_COMPRESSION
#define GLOBAL_MAX_ROWS_PER_COMPRESSION 1015
#endif

/*
 * Compressed data starts with a specialized varlen type starting with the usual
 * varlen header, and followed by a version specifying which compression
 * algorithm was used. This allows us to share the same code across different
 * SQL datatypes. Currently we only allow 127 versions, as we may want to use
 * variable-width integer type in the event we have more than a non-trivial
 * number of compression algorithms.
 */
#define CompressedDataHeaderFields                                                                 \
	char vl_len_[4];                                                                               \
	uint8 compression_algorithm

typedef struct CompressedDataHeader
{
	CompressedDataHeaderFields;
} CompressedDataHeader;

/* On 32-bit architectures, 64-bit values are boxed when returned as datums. To avoid
this overhead we have this type and corresponding iterators for efficiency. The iterators
are private to the compression algorithms for now. */
typedef uint64 DecompressDataInternal;

typedef struct DecompressResultInternal
{
	DecompressDataInternal val;
	bool is_null;
	bool is_done;
} DecompressResultInternal;

/* This type returns datums and is used as our main interface */
typedef struct DecompressResult
{
	Datum val;
	bool is_null;
	bool is_done;
} DecompressResult;

/*
 * Let's use Arrow C data interface. There are many computation engines that are
 * compatible with it, even if it's a bit excessive for our current needs.
 *
 * https://arrow.apache.org/docs/format/CDataInterface.html
 */
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

static pg_attribute_always_inline bool
arrow_validity_bitmap_get(const uint64 *bitmap, int row_number)
{
	const int qword_index = row_number / 64;
	const int bit_index = row_number % 64;
	const uint64 mask = 1ull << bit_index;
	return (bitmap[qword_index] & mask) ? 1 : 0;
}

static pg_attribute_always_inline void
arrow_validity_bitmap_set(uint64 *bitmap, int row_number, bool value)
{
	const int qword_index = row_number / 64;
	const int bit_index = row_number % 64;
	const uint64 mask = 1ull << bit_index;

	if (value)
	{
		bitmap[qword_index] |= mask;
	}
	else
	{
		bitmap[qword_index] &= ~mask;
	}

	Assert(arrow_validity_bitmap_get(bitmap, row_number) == value);
}

/* Forward declaration of ColumnCompressionInfo so we don't need to include catalog.h */
typedef struct FormData_hypertable_compression ColumnCompressionInfo;

typedef struct Compressor Compressor;
struct Compressor
{
	void (*append_null)(Compressor *compressord);
	void (*append_val)(Compressor *compressor, Datum val);
	void *(*finish)(Compressor *data);
};

typedef struct DecompressionIterator
{
	uint8 compression_algorithm;
	bool forward;

	Oid element_type;
	DecompressResult (*try_next)(struct DecompressionIterator *);
	ArrowArray (*decompress_all_forward_direction)(struct DecompressionIterator *);
} DecompressionIterator;

/*
 * TOAST_STORAGE_EXTENDED for out of line storage.
 * TOAST_STORAGE_EXTERNAL for out of line storage + native PG toast compression
 * used when you want to enable postgres native toast
 * compression on the output of the compression algorithm.
 */
typedef enum
{
	TOAST_STORAGE_EXTERNAL,
	TOAST_STORAGE_EXTENDED
} CompressionStorage;

typedef struct CompressionAlgorithmDefinition
{
	DecompressionIterator *(*iterator_init_forward)(Datum, Oid element_type);
	DecompressionIterator *(*iterator_init_reverse)(Datum, Oid element_type);
	ArrowArray *(*decompress_all_forward_direction)(Datum, Oid element_type);
	void (*compressed_data_send)(CompressedDataHeader *, StringInfo);
	Datum (*compressed_data_recv)(StringInfo);

	Compressor *(*compressor_for_type)(Oid element_type);
	CompressionStorage compressed_data_storage;
} CompressionAlgorithmDefinition;

typedef enum CompressionAlgorithms
{
	/* Not a real algorithm, if this does get used, it's a bug in the code */
	_INVALID_COMPRESSION_ALGORITHM = 0,

	COMPRESSION_ALGORITHM_ARRAY,
	COMPRESSION_ALGORITHM_DICTIONARY,
	COMPRESSION_ALGORITHM_GORILLA,
	COMPRESSION_ALGORITHM_DELTADELTA,

	/* When adding an algorithm also add a static assert statement below */
	/* end of real values */
	_END_COMPRESSION_ALGORITHMS,
	_MAX_NUM_COMPRESSION_ALGORITHMS = 128,
} CompressionAlgorithms;

typedef struct CompressionStats
{
	int64 rowcnt_pre_compression;
	int64 rowcnt_post_compression;
} CompressionStats;

extern Datum tsl_compressed_data_decompress_forward(PG_FUNCTION_ARGS);
extern Datum tsl_compressed_data_decompress_reverse(PG_FUNCTION_ARGS);
extern Datum tsl_compressed_data_send(PG_FUNCTION_ARGS);
extern Datum tsl_compressed_data_recv(PG_FUNCTION_ARGS);
extern Datum tsl_compressed_data_in(PG_FUNCTION_ARGS);
extern Datum tsl_compressed_data_out(PG_FUNCTION_ARGS);

static void
pg_attribute_unused() assert_num_compression_algorithms_sane(void)
{
	/* make sure not too many compression algorithms   */
	StaticAssertStmt(_END_COMPRESSION_ALGORITHMS <= _MAX_NUM_COMPRESSION_ALGORITHMS,
					 "Too many compression algorthims, make sure a decision on variable-length "
					 "version field has been made.");

	/* existing indexes that MUST NEVER CHANGE */
	StaticAssertStmt(COMPRESSION_ALGORITHM_ARRAY == 1, "algorithm index has changed");
	StaticAssertStmt(COMPRESSION_ALGORITHM_DICTIONARY == 2, "algorithm index has changed");
	StaticAssertStmt(COMPRESSION_ALGORITHM_GORILLA == 3, "algorithm index has changed");
	StaticAssertStmt(COMPRESSION_ALGORITHM_DELTADELTA == 4, "algorithm index has changed");

	/*
	 * This should change when adding a new algorithm after adding the new
	 * algorithm to the assert list above. This statement prevents adding a
	 * new algorithm without updating the asserts above
	 */
	StaticAssertStmt(_END_COMPRESSION_ALGORITHMS == 5,
					 "number of algorithms have changed, the asserts should be updated");
}

extern CompressionStorage compression_get_toast_storage(CompressionAlgorithms algo);
extern CompressionStats compress_chunk(Oid in_table, Oid out_table,
									   const ColumnCompressionInfo **column_compression_info,
									   int num_compression_infos);
extern void decompress_chunk(Oid in_table, Oid out_table);

extern DecompressionIterator *(*tsl_get_decompression_iterator_init(
	CompressionAlgorithms algorithm, bool reverse))(Datum, Oid element_type);
extern ArrowArray *tsl_try_decompress_all(CompressionAlgorithms algorithm, Datum compressed_data,
										  Oid element_type);
extern void update_compressed_chunk_relstats(Oid uncompressed_relid, Oid compressed_relid);
extern void merge_chunk_relstats(Oid merged_relid, Oid compressed_relid);

typedef struct Chunk Chunk;
typedef struct ChunkInsertState ChunkInsertState;
extern void decompress_batches_for_insert(ChunkInsertState *cis, Chunk *chunk,
										  TupleTableSlot *slot);

#endif
