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
#include <catalog/pg_type_d.h>
#include <executor/tuptable.h>
#include <utils/date.h>

#include "export.h"
#include "time_utils.h"
#include "ts_catalog/catalog.h"
#include "utils.h"

typedef struct PartitioningInfo PartitioningInfo;
typedef struct DimensionSlice DimensionSlice;
typedef struct DimensionVec DimensionVec;

/*
 * ChunkInterval stores both the interval value and origin for a dimension.
 * Values are stored directly (not as Datum pointers) making the struct safe
 * to copy with simple struct assignment.
 *
 * Use chunk_interval_get_datum() and chunk_interval_get_origin() to get Datum
 * values that work correctly on both 32-bit and 64-bit platforms.
 */
typedef struct ChunkInterval
{
	Oid type;		 /* Interval type (INTERVALOID, INT8OID, INT4OID, INT2OID) */
	Oid origin_type; /* Origin type (column type for timestamps/integers, TIMESTAMPTZOID for UUID)
					  */
	bool has_origin; /* True if origin was explicitly specified */

	/* Interval value storage */
	union
	{
		int64 integer_interval; /* For INT8OID, INT4OID, INT2OID */
		Interval interval;		/* For INTERVALOID */
	};

	/* Origin storage - type depends on origin_type.
	 * For timestamp types (including UUID columns): stored in PostgreSQL epoch (not Unix epoch!)
	 * For integer types: stored as-is */
	union
	{
		TimestampTz ts_origin; /* For TIMESTAMPTZOID, TIMESTAMPOID, DATEOID */
		int64 integer_origin;  /* For integer types (INT2OID, INT4OID, INT8OID) */
	};
} ChunkInterval;

/*
 * Get the interval value as a Datum from a ChunkInterval.
 * On 32-bit platforms, int64 is pass-by-reference so we need Int64GetDatumFast.
 */
static inline Datum
chunk_interval_get_datum(const ChunkInterval *ci)
{
	switch (ci->type)
	{
		case INT2OID:
			return Int16GetDatum((int16) ci->integer_interval);
		case INT4OID:
			return Int32GetDatum((int32) ci->integer_interval);
		case INT8OID:
			/* int64 is pass-by-ref on 32-bit, pass-by-val on 64-bit */
			return Int64GetDatumFast(ci->integer_interval);
		case INTERVALOID:
			/* Interval is always pass-by-ref */
			return IntervalPGetDatum(&((ChunkInterval *) ci)->interval);
		default:
			Ensure(false, "unsupported chunk interval type %d", ci->type);
			return UnassignedDatum;
	}
}

/*
 * Get the origin value as a Datum from a ChunkInterval.
 * For timestamps, ts_origin is already in PostgreSQL epoch format.
 * For integers, integer_origin stores the value directly.
 */
static inline Datum
chunk_interval_get_origin(const ChunkInterval *ci)
{
	switch (ci->origin_type)
	{
		case TIMESTAMPTZOID:
			return TimestampTzGetDatum(ci->ts_origin);
		case TIMESTAMPOID:
			return TimestampGetDatum(ci->ts_origin);
		case DATEOID:
			/* Convert from TimestampTz to Date using PostgreSQL's built-in conversion */
			return DirectFunctionCall1(timestamp_date, TimestampTzGetDatum(ci->ts_origin));
		case INT2OID:
			return Int16GetDatum((int16) ci->integer_origin);
		case INT4OID:
			return Int32GetDatum((int32) ci->integer_origin);
		case INT8OID:
		default:
			return Int64GetDatumFast(ci->integer_origin);
	}
}

/*
 * Get the origin value as internal format (Unix epoch microseconds for timestamps).
 * Use this when writing to the catalog or fd.interval_origin.
 */
static inline int64
chunk_interval_get_origin_internal(const ChunkInterval *ci)
{
	Ensure(OidIsValid(ci->origin_type),
		   "chunk_interval_get_origin_internal called with invalid origin_type");
	return ts_time_value_to_internal(chunk_interval_get_origin(ci), ci->origin_type);
}

static inline void
chunk_interval_set(ChunkInterval *chunk_interval, Datum interval, Oid type)
{
	chunk_interval->type = type;

	/* Store interval value in appropriate union field */
	if (type == INT8OID)
		chunk_interval->integer_interval = DatumGetInt64(interval);
	else if (type == INTERVALOID)
		chunk_interval->interval = *DatumGetIntervalP(interval);
	else if (type == INT4OID)
		chunk_interval->integer_interval = DatumGetInt32(interval);
	else if (type == INT2OID)
		chunk_interval->integer_interval = DatumGetInt16(interval);
}

/*
 * Set the origin value from internal format (Unix epoch microseconds for timestamps).
 * Use this when reading from the catalog or fd.interval_origin.
 */
static inline void
chunk_interval_set_origin_internal(ChunkInterval *ci, int64 internal_origin, Oid origin_type)
{
	Ensure(OidIsValid(origin_type),
		   "chunk_interval_set_origin_internal called with invalid origin_type");
	ci->origin_type = origin_type;

	if (IS_TIMESTAMP_TYPE(origin_type))
	{
		/*
		 * For all timestamp types (including DATEOID), convert internal directly to
		 * Timestamp and store. We use TIMESTAMPOID for the conversion to avoid
		 * precision loss that would occur when converting through DateADT.
		 * The origin_type is preserved so chunk_interval_get_origin can convert
		 * back to the correct type when needed.
		 */
		Datum ts_datum = ts_internal_to_time_value(internal_origin, TIMESTAMPOID);
		ci->ts_origin = DatumGetTimestamp(ts_datum);
	}
	else
	{
		/*
		 * For integer types, the internal representation IS the stored value.
		 * No conversion needed - just store directly.
		 */
		ci->integer_origin = internal_origin;
	}
}

/* Forward declaration for use in inline function below */
extern TSDLLEXPORT int64 ts_dimension_origin_to_internal(Datum origin, Oid origin_type);

static inline void
chunk_interval_set_with_origin(ChunkInterval *ci, Datum interval, Oid interval_type, Datum origin,
							   Oid origin_type, bool has_origin)
{
	chunk_interval_set(ci, interval, interval_type);

	ci->origin_type = origin_type;
	ci->has_origin = has_origin;

	if (has_origin && OidIsValid(origin_type))
	{
		/*
		 * Convert origin to internal format (Unix epoch microseconds) and then
		 * back to the stored format. This handles type conversions properly,
		 * e.g., DATE values (days) to timestamps (microseconds).
		 */
		int64 internal_origin = ts_dimension_origin_to_internal(origin, origin_type);
		chunk_interval_set_origin_internal(ci, internal_origin, origin_type);
	}
}

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
/*
 * Check if a dimension uses calendar chunking vs non-calendar (fixed-size) chunking.
 * Calendar mode: chunk_interval.type == INTERVALOID
 *   - Catalog: interval IS NOT NULL, interval_length IS NULL
 *   - In-memory: fd.interval_length == 0
 * Non-calendar mode: chunk_interval.type == INT8OID
 *   - Catalog: interval IS NULL, interval_length IS NOT NULL (> 0)
 *   - In-memory: fd.interval_length > 0
 * Closed (hash) dimensions: chunk_interval.type == InvalidOid (not applicable)
 */
#define IS_CALENDAR_CHUNKING(d) ((d)->chunk_interval.type == INTERVALOID)
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
	ChunkInterval chunk_interval;
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
#define DIMENSION_INFO_IS_VALID(di)                                                                \
	((di)->num_slices_is_set || OidIsValid((di)->chunk_interval.type))

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

/*
 * Convert a user-provided origin value to internal time format (Unix epoch microseconds).
 * Supports TIMESTAMPOID, TIMESTAMPTZOID, DATEOID, and integer types.
 * Returns 0 if origin_type is invalid.
 */
extern TSDLLEXPORT int64 ts_dimension_origin_to_internal(Datum origin, Oid origin_type);
extern TSDLLEXPORT DimensionInfo *ts_dimension_info_create_closed(Oid table_relid, Name column_name,
																  int32 num_slices,
																  regproc partitioning_func);

extern void ts_dimension_info_validate(DimensionInfo *info);
extern void ts_dimension_info_set_defaults(DimensionInfo *info);
extern int32 ts_dimension_add_from_info(DimensionInfo *info);
extern void ts_dimensions_rename_schema_name(const char *old_name, const char *new_name);
extern TSDLLEXPORT void ts_dimension_update(const Hypertable *ht, const NameData *dimname,
											DimensionType dimtype,
											const ChunkInterval *chunk_interval, int16 *num_slices,
											Oid *integer_now_func);
extern TSDLLEXPORT Point *ts_point_create(int16 num_dimensions);
extern TSDLLEXPORT bool ts_is_equality_operator(Oid opno, Oid left, Oid right);
extern TSDLLEXPORT Datum ts_dimension_info_in(PG_FUNCTION_ARGS);
extern TSDLLEXPORT Datum ts_dimension_info_out(PG_FUNCTION_ARGS);

#define hyperspace_get_open_dimension(space, i)                                                    \
	ts_hyperspace_get_dimension(space, DIMENSION_TYPE_OPEN, i)
#define hyperspace_get_closed_dimension(space, i)                                                  \
	ts_hyperspace_get_dimension(space, DIMENSION_TYPE_CLOSED, i)
