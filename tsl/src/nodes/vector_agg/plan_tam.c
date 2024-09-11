/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <access/attnum.h>
#include <nodes/parsenodes.h>
#include <parser/parsetree.h>

#include "hypercore/hypercore_handler.h"
#include "nodes/decompress_chunk/vector_quals.h"
#include "plan.h"

bool
vectoragg_plan_tam(Plan *childplan, const List *rtable, VectorQualInfo *vqi)
{
	const CustomScan *customscan = castNode(CustomScan, childplan);
	RangeTblEntry *rte = rt_fetch(customscan->scan.scanrelid, rtable);
	Relation rel = table_open(rte->relid, AccessShareLock);
	const HypercoreInfo *hinfo = RelationGetHypercoreInfo(rel);

	*vqi = (VectorQualInfo){
		.rti = customscan->scan.scanrelid,
		.vector_attrs = (bool *) palloc0(sizeof(bool) * (hinfo->num_columns + 1)),
		.segmentby_attrs = (bool *) palloc0(sizeof(bool) * (hinfo->num_columns + 1)),
		/*
		 * Hypercore TAM and ColumnarScan do not yet support specific ordering
		 * (via pathkeys) so vector data will always be as read.
		 */
		.reverse = false,
	};

	for (int i = 0; i < hinfo->num_columns; i++)
	{
		AttrNumber attno = AttrOffsetGetAttrNumber(i);

		if (!hinfo->columns[i].is_dropped)
		{
			/*
			 * Hypercore TAM only supports bulk decompression, so all columns
			 * are vectorizable, including segmentby columns.
			 */
			vqi->vector_attrs[attno] = true;
			vqi->segmentby_attrs[attno] = hinfo->columns[i].is_segmentby;
		}
	}

	table_close(rel, NoLock);

	return true;
}
