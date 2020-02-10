/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_DIMENSION_SLICE_H
#define TIMESCALEDB_DIMENSION_SLICE_H

#include <postgres.h>
#include <nodes/pg_list.h>

#include "chunk_constraint.h"

#define DIMENSION_SLICE_MAXVALUE ((int64) PG_INT64_MAX)
#define DIMENSION_SLICE_MINVALUE ((int64) PG_INT64_MIN)

/* partition functions return int32 */
#define DIMENSION_SLICE_CLOSED_MAX ((int64) PG_INT32_MAX)

#define VALUE_GT(v1, v2) ((v1) > (v2))
#define VALUE_LT(v1, v2) ((v1) < (v2))
/*
 * Compare two values, returning -1, 1, 0 if the left one is, less, greater,
 * or equal to the right one, respectively.
 */
#define VALUE_CMP(v1, v2) VALUE_GT(v1, v2) - VALUE_LT(v1, v2)

/* Compare the range start values of two slices */
#define DIMENSION_SLICE_RANGE_START_CMP(s1, s2)                                                    \
	VALUE_CMP((s1)->fd.range_start, (s2)->fd.range_start)

/* Compare the range end values of two slices */
#define DIMENSION_SLICE_RANGE_END_CMP(s1, s2) VALUE_CMP((s1)->fd.range_end, (s2)->fd.range_end)

typedef struct DimensionSlice
{
	FormData_dimension_slice fd;
	void (*storage_free)(void *);
	void *storage;
} DimensionSlice;

typedef struct DimensionVec DimensionVec;
typedef struct Hypercube Hypercube;

extern DimensionVec *ts_dimension_slice_scan_limit(int32 dimension_id, int64 coordinate, int limit);
extern DimensionVec *ts_dimension_slice_scan_range_limit(int32 dimension_id,
														 StrategyNumber start_strategy,
														 int64 start_value,
														 StrategyNumber end_strategy,
														 int64 end_value, int limit);
extern DimensionVec *ts_dimension_slice_collision_scan_limit(int32 dimension_id, int64 range_start,
															 int64 range_end, int limit);
extern DimensionSlice *ts_dimension_slice_scan_for_existing(DimensionSlice *slice);
extern DimensionSlice *ts_dimension_slice_scan_by_id(int32 dimension_slice_id, MemoryContext mctx);
extern DimensionVec *ts_dimension_slice_scan_by_dimension(int32 dimension_id, int limit);
extern DimensionVec *ts_dimension_slice_scan_by_dimension_before_point(int32 dimension_id,
																	   int64 point, int limit,
																	   ScanDirection scandir,
																	   MemoryContext mctx);
extern int ts_dimension_slice_delete_by_dimension_id(int32 dimension_id, bool delete_constraints);
extern int ts_dimension_slice_delete_by_id(int32 dimension_slice_id, bool delete_constraints);
extern DimensionSlice *ts_dimension_slice_create(int dimension_id, int64 range_start,
												 int64 range_end);
extern DimensionSlice *ts_dimension_slice_copy(const DimensionSlice *original);
extern bool ts_dimension_slices_collide(DimensionSlice *slice1, DimensionSlice *slice2);
extern bool ts_dimension_slices_equal(DimensionSlice *slice1, DimensionSlice *slice2);
extern bool ts_dimension_slice_cut(DimensionSlice *to_cut, DimensionSlice *other, int64 coord);
extern void ts_dimension_slice_free(DimensionSlice *slice);
extern void ts_dimension_slice_insert_multi(DimensionSlice **slice, Size num_slices);
extern int ts_dimension_slice_cmp(const DimensionSlice *left, const DimensionSlice *right);
extern int ts_dimension_slice_cmp_coordinate(const DimensionSlice *slice, int64 coord);

extern TSDLLEXPORT DimensionSlice *ts_dimension_slice_nth_latest_slice(int32 dimension_id, int n);
extern TSDLLEXPORT int ts_dimension_slice_oldest_chunk_without_executed_job(
	int32 job_id, int32 dimension_id, StrategyNumber start_strategy, int64 start_value,
	StrategyNumber end_strategy, int64 end_value);
extern TSDLLEXPORT int32 ts_dimension_slice_get_chunkid_to_compress(int32 dimension_id,
																	StrategyNumber start_strategy,
																	int64 start_value,
																	StrategyNumber end_strategy,
																	int64 end_value);
#define dimension_slice_insert(slice) ts_dimension_slice_insert_multi(&(slice), 1)

#define dimension_slice_scan(dimension_id, coordinate)                                             \
	ts_dimension_slice_scan_limit(dimension_id, coordinate, 0)

#define dimension_slice_collision_scan(dimension_id, range_start, range_end)                       \
	ts_dimension_slice_collision_scan_limit(dimension_id, range_start, range_end, 0)

#endif /* TIMESCALEDB_DIMENSION_SLICE_H */
