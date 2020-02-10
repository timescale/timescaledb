/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <postgres.h>

void pushdown_quals(PlannerInfo *root, RelOptInfo *chunk_rel, RelOptInfo *compressed_rel,
					List *compression_info);
