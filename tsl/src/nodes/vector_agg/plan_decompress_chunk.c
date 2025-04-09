/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <nodes/pathnodes.h>
#include <nodes/plannodes.h>

#include "nodes/decompress_chunk/planner.h"
#include "plan.h"

/*
 * Whether the given compressed column index corresponds to a vector variable.
 */
static bool
is_vector_compressed_column(const CustomScan *custom, int compressed_column_index,
							bool *out_is_segmentby)
{
	List *bulk_decompression_column = list_nth(custom->custom_private, DCP_BulkDecompressionColumn);
	const bool bulk_decompression_enabled_for_column =
		list_nth_int(bulk_decompression_column, compressed_column_index);

	/*
	 * Bulk decompression can be disabled for all columns in the DecompressChunk
	 * node settings, we can't do vectorized aggregation for compressed columns
	 * in that case. For segmentby columns it's still possible.
	 */
	List *settings = linitial(custom->custom_private);
	const bool bulk_decompression_enabled_globally =
		list_nth_int(settings, DCS_EnableBulkDecompression);

	/*
	 * Check if this column is a segmentby.
	 */
	List *is_segmentby_column = list_nth(custom->custom_private, DCP_IsSegmentbyColumn);
	const bool is_segmentby = list_nth_int(is_segmentby_column, compressed_column_index);
	if (out_is_segmentby)
	{
		*out_is_segmentby = is_segmentby;
	}

	/*
	 * We support vectorized aggregation either for segmentby columns or for
	 * columns with bulk decompression enabled.
	 */
	if (!is_segmentby &&
		!(bulk_decompression_enabled_for_column && bulk_decompression_enabled_globally))
	{
		/* Vectorized aggregation not possible for this particular column. */
		return false;
	}

	return true;
}

/*
 * Map the custom scan attribute number to the uncompressed chunk attribute
 * number.
 */
static int
custom_scan_to_uncompressed_chunk_attno(List *custom_scan_tlist, int custom_scan_attno)
{
	if (custom_scan_tlist == NIL)
	{
		return custom_scan_attno;
	}

	Var *var =
		castNode(Var,
				 castNode(TargetEntry,
						  list_nth(custom_scan_tlist, AttrNumberGetAttrOffset(custom_scan_attno)))
					 ->expr);
	return var->varattno;
}

void
vectoragg_plan_decompress_chunk(Plan *childplan, VectorQualInfo *vqi)
{
	const CustomScan *custom = castNode(CustomScan, childplan);

	vqi->rti = custom->scan.scanrelid;

	/*
	 * Now, we have to translate the decompressed varno into the compressed
	 * column index, to check if the column supports bulk decompression.
	 */
	List *decompression_map = list_nth(custom->custom_private, DCP_DecompressionMap);

	/*
	 * There's no easy way to determine maximum attribute number for uncompressed
	 * chunk at this stage, so we'll have to go through all the compressed columns
	 * for this.
	 */
	int maxattno = 0;
	for (int compressed_column_index = 0; compressed_column_index < list_length(decompression_map);
		 compressed_column_index++)
	{
		const int custom_scan_attno = list_nth_int(decompression_map, compressed_column_index);
		if (custom_scan_attno <= 0)
		{
			continue;
		}

		const int uncompressed_chunk_attno =
			custom_scan_to_uncompressed_chunk_attno(custom->custom_scan_tlist, custom_scan_attno);

		if (uncompressed_chunk_attno > maxattno)
		{
			maxattno = uncompressed_chunk_attno;
		}
	}

	vqi->maxattno = maxattno;
	vqi->vector_attrs = (bool *) palloc0(sizeof(bool) * (maxattno + 1));
	vqi->segmentby_attrs = (bool *) palloc0(sizeof(bool) * (maxattno + 1));

	for (int compressed_column_index = 0; compressed_column_index < list_length(decompression_map);
		 compressed_column_index++)
	{
		const int custom_scan_attno = list_nth_int(decompression_map, compressed_column_index);
		if (custom_scan_attno <= 0)
		{
			continue;
		}

		const int uncompressed_chunk_attno =
			custom_scan_to_uncompressed_chunk_attno(custom->custom_scan_tlist, custom_scan_attno);

		vqi->vector_attrs[uncompressed_chunk_attno] =
			is_vector_compressed_column(custom,
										compressed_column_index,
										&vqi->segmentby_attrs[uncompressed_chunk_attno]);
	}

	List *settings = linitial(custom->custom_private);
	vqi->reverse = list_nth_int(settings, DCS_Reverse);
}
