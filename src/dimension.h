#ifndef TIMESCALEDB_DIMENSION_H
#define TIMESCALEDB_DIMENSION_H

#include <postgres.h>

#include "catalog.h"

typedef struct PartitioningInfo PartitioningInfo;

typedef enum DimensionType
{
	DIMENSION_TYPE_TIME,
	DIMENSION_TYPE_SPACE,
} DimensionType;
	
typedef struct Dimension
{
	FormData_dimension fd;
	DimensionType type;
	/* num_slices is the number of slices in the cached Dimension for a
	 * particular time interval, which might differ from the num_slices in the
	 * FormData in case partitioning has changed. */
	int16 num_slices;
	PartitioningInfo *partitioning;
} Dimension;


#define IS_TIME_DIMENSION(d) \
	((d)->type == DIMENSION_TYPE_TIME)

#define IS_SPACE_DIMENSION(d) \
	((d)->type == DIMENSION_TYPE_SPACE)

/* We currently support only one time dimension and one space dimension */
#define MAX_TIME_DIMENSIONS 1
#define MAX_SPACE_DIMENSIONS 1
#define MAX_DIMENSIONS (MAX_TIME_DIMENSIONS + MAX_SPACE_DIMENSIONS)

/*
 * Hyperspace defines the current partitioning in a N-dimensional space.
 */
typedef struct Hyperspace
{
	int16 num_time_dimensions;
	int16 num_space_dimensions;
	union {
		struct {
			Dimension *time_dimensions[MAX_TIME_DIMENSIONS];
			Dimension *space_dimensions[MAX_SPACE_DIMENSIONS];
		};
		Dimension *dimensions[MAX_DIMENSIONS];
	};
} Hyperspace;

#define HYPERSPACE_NUM_DIMENSIONS(hs) \
	((hs)->num_time_dimensions + (hs)->num_space_dimensions)

extern Hyperspace *dimension_scan(int32 hypertable_id);

#endif /* TIMESCALEDB_DIMENSION_H */
