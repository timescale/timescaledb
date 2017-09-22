#ifndef TIMESCALEDB_DIMENSION_H
#define TIMESCALEDB_DIMENSION_H

#include <postgres.h>
#include <access/attnum.h>
#include <access/htup_details.h>

#include "catalog.h"

typedef struct PartitioningInfo PartitioningInfo;
typedef struct DimensionSlice DimensionSlice;

typedef enum DimensionType
{
	DIMENSION_TYPE_OPEN,
	DIMENSION_TYPE_CLOSED,
} DimensionType;

typedef struct Dimension
{
	FormData_dimension fd;
	DimensionType type;
	AttrNumber	column_attno;
	Oid			main_table_relid;
	PartitioningInfo *partitioning;
} Dimension;


#define IS_OPEN_DIMENSION(d)					\
	((d)->type == DIMENSION_TYPE_OPEN)

#define IS_CLOSED_DIMENSION(d)					\
	((d)->type == DIMENSION_TYPE_CLOSED)

/*
 * A hyperspace defines how to partition in a N-dimensional space.
 */
typedef struct Hyperspace
{
	int32		hypertable_id;
	Oid			main_table_relid;
	uint16		capacity;
	uint16		num_dimensions;
	/* Open dimensions should be stored before closed dimensions */
	Dimension	dimensions[FLEXIBLE_ARRAY_MEMBER];
} Hyperspace;

#define HYPERSPACE_SIZE(num_dimensions)							\
	(sizeof(Hyperspace) + (sizeof(Dimension) * (num_dimensions)))

/*
 * A point in an N-dimensional hyperspace.
 */
typedef struct Point
{
	int16		cardinality;
	uint8		num_coords;
	/* Open dimension coordinates are stored before the closed coordinates */
	int64		coordinates[FLEXIBLE_ARRAY_MEMBER];
} Point;

#define POINT_SIZE(cardinality)							\
	(sizeof(Point) + (sizeof(int64) * (cardinality)))

extern Hyperspace *dimension_scan(int32 hypertable_id, Oid main_table_relid, int16 num_dimension);
extern DimensionSlice *dimension_calculate_default_slice(Dimension *dim, int64 value);
extern Point *hyperspace_calculate_point(Hyperspace *h, HeapTuple tuple, TupleDesc tupdesc);
extern Dimension *hyperspace_get_dimension_by_id(Hyperspace *hs, int32 id);
extern Dimension *hyperspace_get_dimension(Hyperspace *hs, DimensionType type, Index n);

#define hyperspace_get_open_dimension(space, i)				\
	hyperspace_get_dimension(space, DIMENSION_TYPE_OPEN, i)
#define hyperspace_get_closed_dimension(space, i )				\
	hyperspace_get_dimension(space, DIMENSION_TYPE_CLOSED, i)

#endif   /* TIMESCALEDB_DIMENSION_H */
