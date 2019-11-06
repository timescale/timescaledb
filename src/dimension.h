/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_DIMENSION_H
#define TIMESCALEDB_DIMENSION_H

#include <postgres.h>
#include <access/attnum.h>
#include <access/htup_details.h>
#include <catalog/pg_type.h>

#include "catalog.h"
#include "export.h"
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
	AttrNumber column_attno;
	Oid main_table_relid;
	PartitioningInfo *partitioning;
} Dimension;

#define IS_OPEN_DIMENSION(d) ((d)->type == DIMENSION_TYPE_OPEN)

#define IS_CLOSED_DIMENSION(d) ((d)->type == DIMENSION_TYPE_CLOSED)

#define IS_INTEGER_TYPE(type) (type == INT2OID || type == INT4OID || type == INT8OID)

#define IS_TIMESTAMP_TYPE(type) (type == TIMESTAMPOID || type == TIMESTAMPTZOID || type == DATEOID)

#define IS_VALID_OPEN_DIM_TYPE(type)                                                               \
	(IS_INTEGER_TYPE(type) || IS_TIMESTAMP_TYPE(type) || ts_type_is_int8_binary_compatible(type))

/*
 * A hyperspace defines how to partition in a N-dimensional space.
 */
typedef struct Hyperspace
{
	int32 hypertable_id;
	Oid main_table_relid;
	uint16 capacity;
	uint16 num_dimensions;
	/* Open dimensions should be stored before closed dimensions */
	Dimension dimensions[FLEXIBLE_ARRAY_MEMBER];
} Hyperspace;

#define HYPERSPACE_SIZE(num_dimensions)                                                            \
	(sizeof(Hyperspace) + (sizeof(Dimension) * (num_dimensions)))

/*
 * A point in an N-dimensional hyperspace.
 */
typedef struct Point
{
	int16 cardinality;
	uint8 num_coords;
	/* Open dimension coordinates are stored before the closed coordinates */
	int64 coordinates[FLEXIBLE_ARRAY_MEMBER];
} Point;

#define POINT_SIZE(cardinality) (sizeof(Point) + (sizeof(int64) * (cardinality)))

#define DEFAULT_CHUNK_TIME_INTERVAL (USECS_PER_DAY * 7) /* 7 days w/o adaptive */
#define DEFAULT_CHUNK_TIME_INTERVAL_ADAPTIVE                                                       \
	(USECS_PER_DAY) /* 1 day with adaptive                                                         \
					 * chunking enabled */

typedef struct Hypertable Hypertable;

/*
 * Dimension information used to validate, create and update dimensions.
 */
typedef struct DimensionInfo
{
	Oid table_relid;
	int32 dimension_id;
	Name colname;
	Oid coltype;
	DimensionType type;
	Datum interval_datum;
	Oid interval_type; /* Type of the interval datum */
	int64 interval;
	int32 num_slices;
	regproc partitioning_func;
	bool if_not_exists;
	bool skip;
	bool set_not_null;
	bool num_slices_is_set;
	bool adaptive_chunking; /* True if adaptive chunking is enabled */
	Hypertable *ht;
} DimensionInfo;

#define DIMENSION_INFO_IS_SET(di)                                                                  \
	(di != NULL && OidIsValid((di)->table_relid) && (di)->colname != NULL)

/* add_dimension record attribute numbers */
enum Anum_add_dimension
{
	Anum_add_dimension_id = 1,
	Anum_add_dimension_schema_name,
	Anum_add_dimension_table_name,
	Anum_add_dimension_column_name,
	Anum_add_dimension_created,
	_Anum_add_dimension_max,
};

#define Natts_add_dimension (_Anum_add_dimension_max - 1)

extern Hyperspace *ts_dimension_scan(int32 hypertable_id, Oid main_table_relid, int16 num_dimension,
									 MemoryContext mctx);
extern DimensionSlice *ts_dimension_calculate_default_slice(Dimension *dim, int64 value);
extern TSDLLEXPORT Point *ts_hyperspace_calculate_point(Hyperspace *h, HeapTuple tuple,
														TupleDesc tupdesc);
extern Dimension *ts_hyperspace_get_dimension_by_id(Hyperspace *hs, int32 id);
extern TSDLLEXPORT Dimension *ts_hyperspace_get_dimension(Hyperspace *hs, DimensionType type,
														  Index n);
extern Dimension *ts_hyperspace_get_dimension_by_name(Hyperspace *hs, DimensionType type,
													  const char *name);
extern DimensionVec *ts_dimension_get_slices(Dimension *dim);
extern int32 ts_dimension_get_hypertable_id(int32 dimension_id);
extern int ts_dimension_set_type(Dimension *dim, Oid newtype);
extern TSDLLEXPORT Oid ts_dimension_get_partition_type(Dimension *dim);
extern int ts_dimension_set_name(Dimension *dim, const char *newname);
extern int ts_dimension_set_chunk_interval(Dimension *dim, int64 chunk_interval);
extern Datum ts_dimension_transform_value(Dimension *dim, Oid collation, Datum value,
										  Oid const_datum_type, Oid *restype);
extern int ts_dimension_delete_by_hypertable_id(int32 hypertable_id, bool delete_slices);
extern TSDLLEXPORT void ts_dimension_open_typecheck(Oid arg_type, Oid time_column_type,
													const char *caller_name);

extern TSDLLEXPORT DimensionInfo *ts_dimension_info_create_open(Oid table_relid, Name column_name,
																Datum interval, Oid interval_type,
																regproc partitioning_func);

static inline DimensionInfo *
ts_dimension_info_create_open_interval_usec(Oid table_relid, Name column_name, int64 interval_usec,
											regproc partitioning_func)
{
	return ts_dimension_info_create_open(table_relid,
										 column_name,
										 Int64GetDatum(interval_usec),
										 INT8OID,
										 partitioning_func);
}

extern TSDLLEXPORT DimensionInfo *ts_dimension_info_create_closed(Oid table_relid, Name column_name,
																  int32 num_slices,
																  regproc partitioning_func);

extern void ts_dimension_info_validate(DimensionInfo *info);
extern void ts_dimension_add_from_info(DimensionInfo *info);
extern void ts_dimensions_rename_schema_name(char *oldname, char *newname);
extern void ts_dimension_update(Oid table_relid, Name dimname, DimensionType dimtype,
								Datum *interval, Oid *intervaltype, int16 *num_slices,
								Oid *integer_now_func);

#define hyperspace_get_open_dimension(space, i)                                                    \
	ts_hyperspace_get_dimension(space, DIMENSION_TYPE_OPEN, i)
#define hyperspace_get_closed_dimension(space, i)                                                  \
	ts_hyperspace_get_dimension(space, DIMENSION_TYPE_CLOSED, i)

#endif /* TIMESCALEDB_DIMENSION_H */
