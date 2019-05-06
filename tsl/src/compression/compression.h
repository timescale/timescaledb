/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_TSL_COMPRESSION_COMPRESSION_H
#define TIMESCALEDB_TSL_COMPRESSION_COMPRESSION_H

#include <postgres.h>
#include <c.h>
#include <fmgr.h>
#include <lib/stringinfo.h>

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

typedef struct DecompressionIterator
{
	uint8 compression_algorithm;
	bool forward;

	Oid element_type;
	DecompressResult (*try_next)(struct DecompressionIterator *);
} DecompressionIterator;

typedef struct CompressionAlgorithmDefinition
{
	DecompressionIterator *(*iterator_init_forward)(Datum, Oid element_type);
	DecompressionIterator *(*iterator_init_reverse)(Datum, Oid element_type);
	void (*compressed_data_send)(CompressedDataHeader *, StringInfo);
	Datum (*compressed_data_recv)(StringInfo);
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

	/* This should change when adding a new algorithm after adding the new algorithm to the assert
	 * list above. This statement prevents adding a new algorithm without updating the asserts above
	 */
	StaticAssertStmt(_END_COMPRESSION_ALGORITHMS == 5,
					 "number of algorithms have changed, the asserts should be updated");
}

#endif
