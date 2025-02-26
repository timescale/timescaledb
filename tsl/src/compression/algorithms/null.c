/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include "null.h"
#include "fmgr.h"

typedef struct NullCompressed
{
	CompressedDataHeaderFields;
} NullCompressed;

extern DecompressionIterator *
null_decompression_iterator_from_datum_forward(Datum bool_compressed, Oid element_type)
{
	elog(ERROR, "null decompression iterator not implemented");
	return NULL;
}

extern DecompressionIterator *
null_decompression_iterator_from_datum_reverse(Datum bool_compressed, Oid element_type)
{
	elog(ERROR, "null decompression iterator not implemented");
	return NULL;
}

extern void
null_compressed_send(CompressedDataHeader *header, StringInfo buffer)
{
	elog(ERROR, "null compression doesn't implement send");
}

extern Datum
null_compressed_recv(StringInfo buffer)
{
	elog(ERROR, "null compression doesn't implement recv");
	PG_RETURN_VOID();
}

extern Compressor *
null_compressor_for_type(Oid element_type)
{
	elog(ERROR, "null compressor not implemented");
	return NULL;
}

extern void *
null_compressor_get_dummy_block(void)
{
	NullCompressed *compressed = palloc(sizeof(NullCompressed));
	Size compressed_size = sizeof(NullCompressed);
	compressed->compression_algorithm = COMPRESSION_ALGORITHM_NULL;
	SET_VARSIZE(&compressed->vl_len_, compressed_size);
	return compressed;
}
