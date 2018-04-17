#ifndef TIMESCALEDB_DIMENSION_SLICE_H
#define TIMESCALEDB_DIMENSION_SLICE_H

#include <postgres.h>
#include <nodes/pg_list.h>

#include "catalog.h"
#include "dimension.h"
#include "chunk_constraint.h"

#define DIMENSION_SLICE_MAXVALUE ((int64)PG_INT64_MAX)
#define DIMENSION_SLICE_MINVALUE ((int64)PG_INT64_MIN)

/* partition functions return int32 */
#define DIMENSION_SLICE_CLOSED_MAX ((int64)PG_INT32_MAX)

typedef struct DimensionSlice
{
	FormData_dimension_slice fd;
	void		(*storage_free) (void *);
	void	   *storage;
} DimensionSlice;

typedef struct DimensionVec DimensionVec;
typedef struct Hypercube Hypercube;

extern DimensionVec *dimension_slice_scan_limit(int32 dimension_id, int64 coordinate, int limit);
extern DimensionVec *dimension_slice_scan_range_limit(int32 dimension_id, StrategyNumber start_strategy, int64 start_value, StrategyNumber end_strategy, int64 end_value, int limit);
extern DimensionVec *dimension_slice_collision_scan_limit(int32 dimension_id, int64 range_start, int64 range_end, int limit);
extern Hypercube *dimension_slice_point_scan(Hyperspace *space, int64 point[]);
extern DimensionSlice *dimension_slice_scan_for_existing(DimensionSlice *slice);
extern DimensionSlice *dimension_slice_scan_by_id(int32 dimension_slice_id);
extern DimensionVec *dimension_slice_scan_by_dimension(int32 dimension_id, int limit);
extern int	dimension_slice_delete_by_dimension_id(int32 dimension_id, bool delete_constraints);
extern int	dimension_slice_delete_by_id(int32 dimension_slice_id, bool delete_constraints);
extern DimensionSlice *dimension_slice_create(int dimension_id, int64 range_start, int64 range_end);
extern DimensionSlice *dimension_slice_copy(const DimensionSlice *original);
extern bool dimension_slices_collide(DimensionSlice *slice1, DimensionSlice *slice2);
extern bool dimension_slices_equal(DimensionSlice *slice1, DimensionSlice *slice2);
extern bool dimension_slice_cut(DimensionSlice *to_cut, DimensionSlice *other, int64 coord);
extern void dimension_slice_free(DimensionSlice *slice);
extern void dimension_slice_insert_multi(DimensionSlice **slice, Size num_slices);
extern int	dimension_slice_cmp(const DimensionSlice *left, const DimensionSlice *right);
extern int	dimension_slice_cmp_coordinate(const DimensionSlice *slice, int64 coord);

#define dimension_slice_insert(slice) \
	dimension_slice_insert_multi(&(slice), 1)

#define dimension_slice_scan(dimension_id, coordinate)	\
	dimension_slice_scan_limit(dimension_id, coordinate, 0)

#define dimension_slice_collision_scan(dimension_id, range_start, range_end)		\
	dimension_slice_collision_scan_limit(dimension_id, range_start, range_end, 0)


#endif							/* TIMESCALEDB_DIMENSION_SLICE_H */
