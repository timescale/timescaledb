/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

extern Path *apply_vectorized_agg_optimization(PlannerInfo *root, AggPath *aggregation_path,
											  Path *subpath);
