#ifndef TIMESCALEDB_DIMENSION_H
#define TIMESCALEDB_DIMENSION_H

#include <postgres.h>
#include <access/attnum.h>
#include <access/htup_details.h>

#include "catalog.h"
#include "utils.h"

typedef struct PartitioningInfo PartitioningInfo;
typedef struct DimensionSlice DimensionSlice;
typedef struct DimensionVec DimensionVec;

typedef enum DimensionType
{
	DIMENSION_TYPE_OPEN,
	DIMENSION_TYPE_CLOSED,
	DIMENSION_TYPE_ANY,
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

#define IS_INTEGER_TYPE(type)							\
	(type == INT2OID || type == INT4OID || type == INT8OID)

#define IS_TIMESTAMP_TYPE(type)									\
	(type == TIMESTAMPOID || type == TIMESTAMPTZOID || type == DATEOID)

#define IS_VALID_OPEN_DIM_TYPE(type)					\
	(IS_INTEGER_TYPE(type) || IS_TIMESTAMP_TYPE(type) || type_is_int8_binary_compatible(type))

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

#define DEFAULT_CHUNK_TIME_INTERVAL (USECS_PER_DAY * 7)	/* 7 days w/o adaptive */
#define DEFAULT_CHUNK_TIME_INTERVAL_ADAPTIVE (USECS_PER_DAY)	/* 1 day with adaptive
																 * chunking enabled */

typedef struct Hypertable Hypertable;

/*
 * Dimension information used to validate, create and update dimensions.
 */
typedef struct DimensionInfo
{
	Oid			table_relid;
	Name		colname;
	Oid			coltype;
	DimensionType type;
	Datum		interval_datum;
	Oid			interval_type;	/* Type of the interval datum */
	int64		interval;
	int32		num_slices;
	regproc		partitioning_func;
	bool		if_not_exists;
	bool		skip;
	bool		set_not_null;
	bool		num_slices_is_set;
	bool		adaptive_chunking;	/* True if adaptive chunking is enabled */
	Hypertable *ht;
} DimensionInfo;

#define DIMENSION_INFO_IS_SET(di)										\
	(OidIsValid((di)->table_relid) &&									\
	 (di)->colname != NULL &&											\
	 ((di)->num_slices_is_set || OidIsValid((di)->interval_datum)))

extern Hyperspace *dimension_scan(int32 hypertable_id, Oid main_table_relid, int16 num_dimension, MemoryContext mctx);
extern DimensionSlice *dimension_calculate_default_slice(Dimension *dim, int64 value);
extern Point *hyperspace_calculate_point(Hyperspace *h, HeapTuple tuple, TupleDesc tupdesc);
extern Dimension *hyperspace_get_dimension_by_id(Hyperspace *hs, int32 id);
extern Dimension *hyperspace_get_dimension(Hyperspace *hs, DimensionType type, Index n);
extern Dimension *hyperspace_get_dimension_by_name(Hyperspace *hs, DimensionType type, const char *name);
extern DimensionVec *dimension_get_slices(Dimension *dim);
extern int32 dimension_get_hypertable_id(int32 dimension_id);
extern int	dimension_set_type(Dimension *dim, Oid newtype);
extern int	dimension_set_name(Dimension *dim, const char *newname);
extern int	dimension_set_chunk_interval(Dimension *dim, int64 chunk_interval);
extern int	dimension_delete_by_hypertable_id(int32 hypertable_id, bool delete_slices);
extern void dimension_validate_info(DimensionInfo *info);
extern void dimension_add_from_info(DimensionInfo *info);

#define hyperspace_get_open_dimension(space, i)				\
	hyperspace_get_dimension(space, DIMENSION_TYPE_OPEN, i)
#define hyperspace_get_closed_dimension(space, i)				\
	hyperspace_get_dimension(space, DIMENSION_TYPE_CLOSED, i)

#endif							/* TIMESCALEDB_DIMENSION_H */
