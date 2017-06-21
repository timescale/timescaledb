#ifndef TIMESCALEDB_DIMENSION_H
#define TIMESCALEDB_DIMENSION_H

#include <postgres.h>

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
	Oid main_table_relid;
	PartitioningInfo *partitioning;
} Dimension;


#define IS_OPEN_DIMENSION(d) \
	((d)->type == DIMENSION_TYPE_OPEN)

#define IS_CLOSED_DIMENSION(d) \
	((d)->type == DIMENSION_TYPE_CLOSED)

/* We currently support only one open dimension and one closed dimension */
#define MAX_OPEN_DIMENSIONS 1 /* Cannot exceed 255 */
#define MAX_CLOSED_DIMENSIONS 1 /* Cannot exceed 255 */
#define MAX_DIMENSIONS (MAX_OPEN_DIMENSIONS + MAX_CLOSED_DIMENSIONS)

/*
 * Hyperspace defines the current partitioning in a N-dimensional space.
 */
typedef struct Hyperspace
{
	int32 hypertable_id;
	Oid main_table_relid;
	uint8 num_open_dimensions;
	uint8 num_closed_dimensions;
	Dimension *open_dimensions[MAX_OPEN_DIMENSIONS];
	Dimension *closed_dimensions[MAX_CLOSED_DIMENSIONS];
} Hyperspace;

#define HYPERSPACE_NUM_DIMENSIONS(hs) \
	((hs)->num_open_dimensions + \
	 (hs)->num_closed_dimensions)

/*
 * A point in an N-dimensional hyperspace.
 */
typedef struct Point
{
	int16 cardinality;
	uint8 num_open;
	uint8 num_closed;
	/* Open dimension coordinates are stored before the closed coordinates */
	int64 coordinates[0];
} Point;

#define point_coordinate_is_in_slice(slice, coord)					\
	(coord >= (slice)->range_start && coord < (slice)->range_end)

#define point_get_open_dimension_coordinate(p, i)	\
	(p)->coordinates[i]

#define point_get_closed_dimension_coordinate(p, i) \
	(p)->coordinates[(p)->num_open + i]

extern Hyperspace *dimension_scan(int32 hypertable_id, Oid main_table_relid);
extern Point *hyperspace_calculate_point(Hyperspace *h, HeapTuple tuple, TupleDesc tupdesc);
extern const char *point_to_string(Point *p);


#endif /* TIMESCALEDB_DIMENSION_H */
