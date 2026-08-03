/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

/*
 * This file contains source code that was copied and/or modified from
 * the PostgreSQL database, which is licensed under the open-source
 * PostgreSQL License. Please see the NOTICE at the top level
 * directory for a copy of the PostgreSQL License.
 *
 * This mirrors backend/access/heap/heaptoast.c; see
 * import/compression_toast.h for what the fork as a whole is for, and the
 * comment above each function for which core function it copies and what
 * was changed.
 */

#include <postgres.h>

#include <access/heaptoast.h>
#include <access/htup_details.h>
#include <access/toast_helper.h>
#include <utils/rel.h>

#include "import/compression_toast.h"

/*
 * This is a copy of heap_toast_insert_or_update() in backend/access/heap/heaptoast.c
 * from PG 18.4, git commit sha f5cc81719e6da4cbdb1f797c48b693e91018153a. It
 * has one modification: it calls compression_toast_tuple_externalize()
 * instead of toast_tuple_externalize(), so externalized values go through
 * the heap_multi_insert()-based writer.
 */
HeapTuple
compression_toast_insert_or_update(BulkWriter *writer, HeapTuple newtup)
{
	Relation rel = writer->out_rel;
	HeapTuple result_tuple;
	TupleDesc tupleDesc;
	int numAttrs;

	Size maxDataLen;
	Size hoff;

	bool toast_isnull[MaxHeapAttributeNumber];
	Datum toast_values[MaxHeapAttributeNumber];
	ToastAttrInfo toast_attr[MaxHeapAttributeNumber];
	ToastTupleContext ttc;

	Assert(rel->rd_rel->relkind == RELKIND_RELATION);

	tupleDesc = rel->rd_att;
	numAttrs = tupleDesc->natts;

	Assert(numAttrs <= MaxHeapAttributeNumber);
	heap_deform_tuple(newtup, tupleDesc, toast_values, toast_isnull);

	ttc.ttc_rel = rel;
	ttc.ttc_values = toast_values;
	ttc.ttc_isnull = toast_isnull;
	ttc.ttc_oldvalues = NULL;
	ttc.ttc_oldisnull = NULL;
	ttc.ttc_attr = toast_attr;
	toast_tuple_init(&ttc);

	hoff = SizeofHeapTupleHeader;
	if ((ttc.ttc_flags & TOAST_HAS_NULLS) != 0)
	{
		hoff += BITMAPLEN(numAttrs);
	}
	hoff = MAXALIGN(hoff);
	maxDataLen = RelationGetToastTupleTarget(rel, TOAST_TUPLE_TARGET) - hoff;

	while (heap_compute_data_size(tupleDesc, toast_values, toast_isnull) > maxDataLen)
	{
		int biggest_attno;

		biggest_attno = toast_tuple_find_biggest_attribute(&ttc, true, false);
		if (biggest_attno < 0)
		{
			break;
		}

		if (TupleDescAttr(tupleDesc, biggest_attno)->attstorage == TYPSTORAGE_EXTENDED)
		{
			toast_tuple_try_compression(&ttc, biggest_attno);
		}
		else
		{
			toast_attr[biggest_attno].tai_colflags |= TOASTCOL_INCOMPRESSIBLE;
		}

		if ((Size) toast_attr[biggest_attno].tai_size > maxDataLen &&
			OidIsValid(rel->rd_rel->reltoastrelid))
		{
			compression_toast_tuple_externalize(writer, &ttc, biggest_attno);
		}
	}

	while (heap_compute_data_size(tupleDesc, toast_values, toast_isnull) > maxDataLen &&
		   OidIsValid(rel->rd_rel->reltoastrelid))
	{
		int biggest_attno;

		biggest_attno = toast_tuple_find_biggest_attribute(&ttc, false, false);
		if (biggest_attno < 0)
		{
			break;
		}
		compression_toast_tuple_externalize(writer, &ttc, biggest_attno);
	}

	while (heap_compute_data_size(tupleDesc, toast_values, toast_isnull) > maxDataLen)
	{
		int biggest_attno;

		biggest_attno = toast_tuple_find_biggest_attribute(&ttc, true, true);
		if (biggest_attno < 0)
		{
			break;
		}

		toast_tuple_try_compression(&ttc, biggest_attno);
	}

	maxDataLen = TOAST_TUPLE_TARGET_MAIN - hoff;

	while (heap_compute_data_size(tupleDesc, toast_values, toast_isnull) > maxDataLen &&
		   OidIsValid(rel->rd_rel->reltoastrelid))
	{
		int biggest_attno;

		biggest_attno = toast_tuple_find_biggest_attribute(&ttc, false, true);
		if (biggest_attno < 0)
		{
			break;
		}

		compression_toast_tuple_externalize(writer, &ttc, biggest_attno);
	}

	if ((ttc.ttc_flags & TOAST_NEEDS_CHANGE) != 0)
	{
		HeapTupleHeader olddata = newtup->t_data;
		HeapTupleHeader new_data;
		int32 new_header_len;
		int32 new_data_len;
		int32 new_tuple_len;

		new_header_len = SizeofHeapTupleHeader;
		if ((ttc.ttc_flags & TOAST_HAS_NULLS) != 0)
		{
			new_header_len += BITMAPLEN(numAttrs);
		}
		new_header_len = MAXALIGN(new_header_len);
		new_data_len = heap_compute_data_size(tupleDesc, toast_values, toast_isnull);
		new_tuple_len = new_header_len + new_data_len;

		result_tuple = (HeapTuple) palloc0(HEAPTUPLESIZE + new_tuple_len);
		result_tuple->t_len = new_tuple_len;
		result_tuple->t_self = newtup->t_self;
		result_tuple->t_tableOid = newtup->t_tableOid;
		new_data = (HeapTupleHeader) ((char *) result_tuple + HEAPTUPLESIZE);
		result_tuple->t_data = new_data;

		memcpy(new_data, olddata, SizeofHeapTupleHeader);
		HeapTupleHeaderSetNatts(new_data, numAttrs);
		new_data->t_hoff = new_header_len;

		heap_fill_tuple(tupleDesc,
						toast_values,
						toast_isnull,
						(char *) new_data + new_header_len,
						new_data_len,
						&(new_data->t_infomask),
						((ttc.ttc_flags & TOAST_HAS_NULLS) != 0) ? new_data->t_bits : NULL);
	}
	else
	{
		result_tuple = newtup;
	}

	toast_tuple_cleanup(&ttc);

	return result_tuple;
}
