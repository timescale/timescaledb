#ifndef TIMESCALEDB_DIMENSION_H
#define TIMESCALEDB_DIMENSION_H

#include <postgres.h>
#include <access/attnum.h>
#include <access/htup_details.h>

#include "catalog.h"

typedef struct PartitioningInfo PartitioningInfo;

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
	int16		num_open_dimensions;
	int16		num_closed_dimensions;
	/* Open dimensions should be stored before closed dimensions */
	Dimension	dimensions[0];
} Hyperspace;

#define HYPERSPACE_NUM_DIMENSIONS(hs) \
	((hs)->num_open_dimensions + \
	 (hs)->num_closed_dimensions)

#define HYPERSPACE_SIZE(num_dimensions)							\
	(sizeof(Hyperspace) + (sizeof(Dimension) * (num_dimensions)))

#define hyperspace_get_closed_dimension(hs, i)		\
	(&(hs)->dimensions[(hs)->num_open_dimensions + i])

/*
 * A point in an N-dimensional hyperspace.
 */
typedef struct Point
{
	int16		cardinality;
	uint8		num_open;
	uint8		num_closed;
	/* Open dimension coordinates are stored before the closed coordinates */
	int64		coordinates[0];
} Point;

#define POINT_SIZE(cardinality)							\
	(sizeof(Point) + (sizeof(int64) * (cardinality)))

extern Hyperspace *dimension_scan(int32 hypertable_id, Oid main_table_relid, int16 num_dimension);
extern Point *hyperspace_calculate_point(Hyperspace *h, HeapTuple tuple, TupleDesc tupdesc);

#endif   /* TIMESCALEDB_DIMENSION_H */
