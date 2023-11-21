/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

#include <postgres.h>

void pushdown_quals(PlannerInfo *root, CompressionSettings *settings, RelOptInfo *chunk_rel,
					RelOptInfo *compressed_rel, bool chunk_partial);
