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
#include <utils/relcache.h>

typedef struct BulkInsertStateData *BulkInsertState;

#include <nodes/execnodes.h>
#include "segment_meta.h"

#include "compat/compat.h"
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

#define MAX_ROWS_PER_COMPRESSION 1000
/* gap in sequence id between rows, potential for adding rows in gap later */
#define SEQUENCE_NUM_GAP 10
#define COMPRESSIONCOL_IS_SEGMENT_BY(col) ((col)->segmentby_column_index > 0)
#define COMPRESSIONCOL_IS_ORDER_BY(col) ((col)->orderby_column_index > 0)

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

/* Forward declaration of ColumnCompressionInfo so we don't need to include catalog.h */
typedef struct FormData_hypertable_compression ColumnCompressionInfo;

typedef struct Compressor Compressor;
struct Compressor
{
	void (*append_null)(Compressor *compressord);
	void (*append_val)(Compressor *compressor, Datum val);
	void *(*finish)(Compressor *data);
};

typedef struct ArrowArray ArrowArray;

typedef struct DecompressionIterator
{
	uint8 compression_algorithm;
	bool forward;

	Oid element_type;
	DecompressResult (*try_next)(struct DecompressionIterator *);
} DecompressionIterator;

typedef struct SegmentInfo
{
	Datum val;
	FmgrInfo eq_fn;
	FunctionCallInfo eq_fcinfo;
	int16 typlen;
	bool is_null;
	bool typ_by_val;
	Oid collation;
} SegmentInfo;

/* this struct holds information about a segmentby column,
 * and additionally stores the mapping for this column in
 * the uncompressed chunk. */
typedef struct CompressedSegmentInfo
{
	SegmentInfo *segment_info;
	int16 decompressed_chunk_offset;
} CompressedSegmentInfo;

typedef struct PerCompressedColumn
{
	Oid decompressed_type;

	/* the compressor to use for compressed columns, always NULL for segmenters
	 * only use if is_compressed
	 */
	DecompressionIterator *iterator;

	/* segment info; only used if !is_compressed */
	Datum val;

	/* is this a compressed column or a segment-by column */
	bool is_compressed;

	/* the value stored in the compressed table was NULL */
	bool is_null;

	/* the index in the decompressed table of the data -1,
	 * if the data is metadata not found in the decompressed table
	 */
	int16 decompressed_column_offset;
} PerCompressedColumn;

typedef struct RowDecompressor
{
	PerCompressedColumn *per_compressed_cols;
	int16 num_compressed_columns;

	TupleDesc in_desc;
	Relation in_rel;

	TupleDesc out_desc;
	Relation out_rel;
	ResultRelInfo *indexstate;

	CommandId mycid;
	BulkInsertState bistate;

	Datum *compressed_datums;
	bool *compressed_is_nulls;

	Datum *decompressed_datums;
	bool *decompressed_is_nulls;

	MemoryContext per_compressed_row_ctx;
} RowDecompressor;

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

typedef ArrowArray *(*DecompressAllFunction)(Datum compressed, Oid element_type,
											 MemoryContext dest_mctx);

typedef struct CompressionAlgorithmDefinition
{
	DecompressionIterator *(*iterator_init_forward)(Datum, Oid element_type);
	DecompressionIterator *(*iterator_init_reverse)(Datum, Oid element_type);
	DecompressAllFunction decompress_all;
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

typedef struct PerColumn
{
	/* the compressor to use for regular columns, NULL for segmenters */
	Compressor *compressor;
	/*
	 * Information on the metadata we'll store for this column (currently only min/max).
	 * Only used for order-by columns right now, will be {-1, NULL} for others.
	 */
	int16 min_metadata_attr_offset;
	int16 max_metadata_attr_offset;
	SegmentMetaMinMaxBuilder *min_max_metadata_builder;

	/* segment info; only used if compressor is NULL */
	SegmentInfo *segment_info;
	int16 segmentby_column_index;
} PerColumn;

typedef struct RowCompressor
{
	/* memory context reset per-row is stored */
	MemoryContext per_row_ctx;

	/* the table we're writing the compressed data to */
	Relation compressed_table;
	BulkInsertState bistate;
	/* segment by index Oid if any */
	Oid index_oid;
	/* relation info necessary to update indexes on compressed table */
	ResultRelInfo *resultRelInfo;
	/* segment by index index in the RelInfo if any */
	int8 segmentby_index_index;

	/* in theory we could have more input columns than outputted ones, so we
	   store the number of inputs/compressors separately */
	int n_input_columns;

	/* info about each column */
	struct PerColumn *per_column;

	/* the order of columns in the compressed data need not match the order in the
	 * uncompressed. This array maps each attribute offset in the uncompressed
	 * data to the corresponding one in the compressed
	 */
	int16 *uncompressed_col_to_compressed_col;
	int16 count_metadata_column_offset;
	int16 sequence_num_metadata_column_offset;

	/* the number of uncompressed rows compressed into the current compressed row */
	uint32 rows_compressed_into_current_value;
	/* a unique monotonically increasing (according to order by) id for each compressed row */
	int32 sequence_num;

	/* cached arrays used to build the HeapTuple */
	Datum *compressed_values;
	bool *compressed_is_null;
	int64 rowcnt_pre_compression;
	int64 num_compressed_rows;
	/* if recompressing segmentwise, we use this info to reset the sequence number */
	bool reset_sequence;
	/* flag for checking if we are working on the first tuple */
	bool first_iteration;
} RowCompressor;

/* SegmentFilter is used for filtering segments based on qualifiers */
typedef struct SegmentFilter
{
	/* Column which we use for filtering */
	NameData column_name;
	/* Filter operation used */
	StrategyNumber strategy;
	/* Value to compare with */
	Const *value;
	/* IS NULL or IS NOT NULL */
	bool is_null_check;
} SegmentFilter;

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

extern DecompressAllFunction tsl_get_decompress_all_function(CompressionAlgorithms algorithm);

typedef struct Chunk Chunk;
typedef struct ChunkInsertState ChunkInsertState;
extern void decompress_batches_for_insert(ChunkInsertState *cis, Chunk *chunk,
										  TupleTableSlot *slot);
#if PG14_GE
extern bool decompress_target_segments(ModifyTableState *ps);
#endif
/* CompressSingleRowState methods */
struct CompressSingleRowState;
typedef struct CompressSingleRowState CompressSingleRowState;

extern CompressSingleRowState *compress_row_init(int srcht_id, Relation in_rel, Relation out_rel);
extern SegmentInfo *segment_info_new(Form_pg_attribute column_attr);
extern bool segment_info_datum_is_in_group(SegmentInfo *segment_info, Datum datum, bool is_null);
extern TupleTableSlot *compress_row_exec(CompressSingleRowState *cr, TupleTableSlot *slot);
extern void compress_row_end(CompressSingleRowState *cr);
extern void compress_row_destroy(CompressSingleRowState *cr);
extern void row_decompressor_decompress_row(RowDecompressor *row_decompressor,
											Tuplesortstate *tuplesortstate);
extern int16 *compress_chunk_populate_keys(Oid in_table, const ColumnCompressionInfo **columns,
										   int n_columns, int *n_keys_out,
										   const ColumnCompressionInfo ***keys_out);
extern void compress_chunk_populate_sort_info_for_column(Oid table,
														 const ColumnCompressionInfo *column,
														 AttrNumber *att_nums, Oid *sort_operator,
														 Oid *collation, bool *nulls_first);
extern PerCompressedColumn *create_per_compressed_column(TupleDesc in_desc, TupleDesc out_desc,
														 Oid out_relid,
														 Oid compressed_data_type_oid);
extern void row_compressor_init(RowCompressor *row_compressor, TupleDesc uncompressed_tuple_desc,
								Relation compressed_table, int num_compression_infos,
								const ColumnCompressionInfo **column_compression_info,
								int16 *column_offsets, int16 num_columns_in_compressed_table,
								bool need_bistate, bool reset_sequence);
extern void row_compressor_finish(RowCompressor *row_compressor);
extern void populate_per_compressed_columns_from_data(PerCompressedColumn *per_compressed_cols,
													  int16 num_cols, Datum *compressed_datums,
													  bool *compressed_is_nulls);
extern void row_compressor_append_sorted_rows(RowCompressor *row_compressor,
											  Tuplesortstate *sorted_rel, TupleDesc sorted_desc);
extern void segment_info_update(SegmentInfo *segment_info, Datum val, bool is_null);

extern RowDecompressor build_decompressor(Relation in_rel, Relation out_rel);

/*
 * A convenience macro to throw an error about the corrupted compressed data, if
 * the argument is false. When fuzzing is enabled, we don't show the message not
 * to pollute the logs.
 */
#ifndef TS_COMPRESSION_FUZZING
#define CORRUPT_DATA_MESSAGE                                                                       \
	(errmsg("the compressed data is corrupt"), errcode(ERRCODE_DATA_CORRUPTED))
#else
#define CORRUPT_DATA_MESSAGE (errcode(ERRCODE_DATA_CORRUPTED))
#endif

#define CheckCompressedData(X)                                                                     \
	if (unlikely(!(X)))                                                                            \
	ereport(ERROR, CORRUPT_DATA_MESSAGE)

inline static void *
consumeCompressedData(StringInfo si, int bytes)
{
	CheckCompressedData(bytes >= 0);
	CheckCompressedData(bytes < PG_INT32_MAX / 2);
	CheckCompressedData(si->cursor + bytes >= 0);
	CheckCompressedData(si->cursor + bytes <= si->len);

	void *result = si->data + si->cursor;
	si->cursor += bytes;
	return result;
}

/*
 * Normal compression uses 1k rows, but the regression tests use up to 1015.
 * We use this limit for sanity checks in case the compressed data is corrupt.
 */
#define GLOBAL_MAX_ROWS_PER_COMPRESSION 1015

#endif
