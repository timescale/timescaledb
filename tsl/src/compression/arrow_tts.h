/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef PG_ARROW_TUPTABLE_H
#define PG_ARROW_TUPTABLE_H

#include <postgres.h>
#include <access/attnum.h>
#include <executor/tuptable.h>
#include <nodes/bitmapset.h>
#include <utils/builtins.h>

#include "arrow_c_data_interface.h"
#include "compression/create.h"
#include "nodes/decompress_chunk/detoaster.h"

typedef struct ArrowTupleTableSlot
{
	BufferHeapTupleTableSlot base;
	TupleTableSlot *compressed_slot;
	ArrowArray **arrow_columns;
	uint16 tuple_index; /* Index of this particular tuple in the compressed (columnar data) tuple */
	MemoryContext decompression_mcxt;
	Bitmapset *segmentby_columns;
	char *data;
	int16 *attrs_offset_map;
} ArrowTupleTableSlot;

extern const TupleTableSlotOps TTSOpsArrowTuple;

static inline int16 *
build_attribute_offset_map(const TupleDesc tupdesc, const TupleDesc ctupdesc,
						   AttrNumber *count_attno)
{
	int16 *attrs_offset_map = palloc0(sizeof(int16) * tupdesc->natts);

	for (int i = 0; i < tupdesc->natts; i++)
	{
		const Form_pg_attribute attr = TupleDescAttr(tupdesc, i);

		if (attr->attisdropped)
		{
			attrs_offset_map[i] = -1;
		}
		else
		{
			bool found = false;

			for (int j = 0; j < ctupdesc->natts; j++)
			{
				const Form_pg_attribute cattr = TupleDescAttr(ctupdesc, j);

				if (!cattr->attisdropped &&
					namestrcmp(&attr->attname, NameStr(cattr->attname)) == 0)
				{
					attrs_offset_map[i] = AttrNumberGetAttrOffset(cattr->attnum);
					found = true;
					break;
				}
			}

			Ensure(found, "missing attribute in compressed relation");
		}
	}

	if (count_attno)
	{
		*count_attno = InvalidAttrNumber;

		/* Find the count column attno */
		for (int i = 0; i < ctupdesc->natts; i++)
		{
			const Form_pg_attribute cattr = TupleDescAttr(ctupdesc, i);

			if (namestrcmp(&cattr->attname, COMPRESSION_COLUMN_METADATA_COUNT_NAME) == 0)
			{
				*count_attno = cattr->attnum;
				break;
			}
		}

		Assert(*count_attno != InvalidAttrNumber);
	}

	return attrs_offset_map;
}

extern TupleTableSlot *ExecStoreArrowTuple(TupleTableSlot *slot, TupleTableSlot *compressed_slot,
										   uint16 tuple_index);
extern TupleTableSlot *ExecStoreArrowTupleExisting(TupleTableSlot *slot, uint16 tuple_index);

#define TTS_IS_ARROWTUPLE(slot) ((slot)->tts_ops == &TTSOpsArrowTuple)

#endif /* PG_ARROW_TUPTABLE_H */
