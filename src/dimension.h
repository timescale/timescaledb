/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#pragma once

#include <postgres.h>
#include <access/attnum.h>
#include <access/htup_details.h>
#include <catalog/pg_type.h>
#include <executor/tuptable.h>

#include "export.h"
#include "time_utils.h"
#include "ts_catalog/catalog.h"

typedef struct PartitioningInfo PartitioningInfo;
typedef struct DimensionSlice DimensionSlice;
typedef struct DimensionVec DimensionVec;

typedef struct ChunkInterval
{
	Oid type;		 /* Interval type (INTERVALOID, INT8OID, etc.) */
	Datum value;	 /* Interval value */
	Oid origin_type; /* Origin type (same as partitioning column type) */
	Datum origin;	 /* Origin value (same type as partitioning column) */
	bool has_origin; /* True if origin was explicitly specified */
} ChunkInterval;

typedef enum DimensionType
{
	DIMENSION_TYPE_OPEN,
	DIMENSION_TYPE_CLOSED,
	DIMENSION_TYPE_STATS,
	DIMENSION_TYPE_ANY,
} DimensionType;

typedef struct Dimension
{
	FormData_dimension fd;
	DimensionType type;
	AttrNumber column_attno;
	Oid main_table_relid;
	ChunkInterval chunk_interval;
	PartitioningInfo *partitioning;
} Dimension;

#define IS_OPEN_DIMENSION(d) ((d)->type == DIMENSION_TYPE_OPEN)
#define IS_CLOSED_DIMENSION(d) ((d)->type == DIMENSION_TYPE_CLOSED)
#define IS_VALID_OPEN_DIM_TYPE(type)                                                               \
	(IS_INTEGER_TYPE(type) || IS_TIMESTAMP_TYPE(type) || IS_UUID_TYPE(type) ||                     \
	 ts_type_is_int8_binary_compatible(type))

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

/* Default intervals for integer types */
#define DEFAULT_SMALLINT_INTERVAL 10000
#define DEFAULT_INT_INTERVAL 100000
#define DEFAULT_BIGINT_INTERVAL 1000000

typedef struct Hypertable Hypertable;

/*
 * Dimension information used to validate, create and update dimensions.
 *
 * This structure is used both partially filled in from the dimension info
 * constructors as well as when building dimension info for the storage into
 * the dimension table.
 *
 * @see ts_hash_dimension
 * @see ts_range_dimension
 */
typedef struct DimensionInfo
{
	/* We declare the SQL type dimension_info with INTERNALLENGTH = VARIABLE.
	 * So, PostgreSQL expects a proper length info field (varlena header).
	 */
	int32 vl_len_;

	Oid table_relid;
	int32 dimension_id;
	NameData colname;
	Oid coltype;
	DimensionType type;
	Datum interval_datum;
	Oid interval_type; /* Type of the interval datum */
	int64 interval;
	Datum origin_datum;	   /* Origin value as Datum */
	Oid origin_type;	   /* Type of origin_datum */
	int64 interval_origin; /* Origin in internal format */
	bool has_origin;	   /* True if origin was explicitly specified */
	int32 num_slices;
	regproc partitioning_func;
	bool if_not_exists;
	bool skip;
	bool set_not_null;
	bool num_slices_is_set;
	bool adaptive_chunking; /* True if adaptive chunking is enabled */
	Hypertable *ht;
} DimensionInfo;

#define DIMENSION_INFO_IS_SET(di) (di != NULL && OidIsValid((di)->table_relid))
#define DIMENSION_INFO_IS_VALID(di) (info->num_slices_is_set || OidIsValid(info->interval_type))

extern Hyperspace *ts_dimension_scan(int32 hypertable_id, Oid main_table_relid, int16 num_dimension,
									 MemoryContext mctx);
extern DimensionSlice *ts_dimension_calculate_default_slice(const Dimension *dim, int64 value);
extern TSDLLEXPORT Point *ts_hyperspace_calculate_point(const Hyperspace *h, TupleTableSlot *slot);
extern int ts_dimension_get_slice_ordinal(const Dimension *dim, const DimensionSlice *slice);
extern TSDLLEXPORT const Dimension *ts_hyperspace_get_dimension_by_id(const Hyperspace *hs,
																	  int32 id);
extern TSDLLEXPORT const Dimension *ts_hyperspace_get_dimension(const Hyperspace *hs,
																DimensionType type, Index n);
extern TSDLLEXPORT Dimension *ts_hyperspace_get_mutable_dimension(Hyperspace *hs,
																  DimensionType type, Index n);
extern TSDLLEXPORT const Dimension *
ts_hyperspace_get_dimension_by_name(const Hyperspace *hs, DimensionType type, const char *name);
extern TSDLLEXPORT Dimension *
ts_hyperspace_get_mutable_dimension_by_name(Hyperspace *hs, DimensionType type, const char *name);
extern DimensionVec *ts_dimension_get_slices(const Dimension *dim);
extern int32 ts_dimension_get_hypertable_id(int32 dimension_id);
extern int ts_dimension_set_type(Dimension *dim, Oid newtype);
extern TSDLLEXPORT Oid ts_dimension_get_partition_type(const Dimension *dim);
extern int ts_dimension_set_name(Dimension *dim, const char *newname);
extern TSDLLEXPORT int ts_dimension_set_chunk_interval(Dimension *dim, int64 chunk_interval);
extern int ts_dimension_set_compress_interval(Dimension *dim, int64 compress_interval);
extern Datum ts_dimension_transform_value(const Dimension *dim, Oid collation, Datum value,
										  Oid const_datum_type, Oid *restype);
extern int ts_dimension_delete_by_hypertable_id(int32 hypertable_id, bool delete_slices);

extern TSDLLEXPORT DimensionInfo *ts_dimension_info_create_open(Oid table_relid, Name column_name,
																Datum interval, Oid interval_type,
																regproc partitioning_func,
																Datum origin, Oid origin_type,
																bool has_origin);

extern TSDLLEXPORT DimensionInfo *ts_dimension_info_create_closed(Oid table_relid, Name column_name,
																  int32 num_slices,
																  regproc partitioning_func);

extern void ts_dimension_info_validate(DimensionInfo *info);
extern int32 ts_dimension_add_from_info(DimensionInfo *info);
extern void ts_dimensions_rename_schema_name(const char *old_name, const char *new_name);
extern TSDLLEXPORT void ts_dimension_update(const Hypertable *ht, const NameData *dimname,
											DimensionType dimtype, Datum *interval,
											Oid *intervaltype, int16 *num_slices,
											Oid *integer_now_func);
extern TSDLLEXPORT void ts_dimension_update_with_origin(
	const Hypertable *ht, const NameData *dimname, DimensionType dimtype, Datum *interval,
	Oid *intervaltype, int16 *num_slices, Oid *integer_now_func, Datum *origin, Oid *origin_type);
extern TSDLLEXPORT Point *ts_point_create(int16 num_dimensions);
extern TSDLLEXPORT bool ts_is_equality_operator(Oid opno, Oid left, Oid right);
extern TSDLLEXPORT Datum ts_dimension_info_in(PG_FUNCTION_ARGS);
extern TSDLLEXPORT Datum ts_dimension_info_out(PG_FUNCTION_ARGS);
extern TSDLLEXPORT int64 ts_dimension_origin_to_internal(Datum origin, Oid origin_type);

#define hyperspace_get_open_dimension(space, i)                                                    \
	ts_hyperspace_get_dimension(space, DIMENSION_TYPE_OPEN, i)
#define hyperspace_get_closed_dimension(space, i)                                                  \
	ts_hyperspace_get_dimension(space, DIMENSION_TYPE_CLOSED, i)
