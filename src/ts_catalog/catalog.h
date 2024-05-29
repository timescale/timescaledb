/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#pragma once

#include <postgres.h>
#include <access/heapam.h>
#include <nodes/nodes.h>
#include <utils/jsonb.h>
#include <utils/rel.h>

#include "export.h"
#include "extension_constants.h"
#include "scanner.h"

/*
 * TimescaleDB catalog.
 *
 * The TimescaleDB catalog contains schema metadata for hypertables, among other
 * things. The metadata is stored in regular tables. This header file contains
 * definitions for those tables and should match any table declarations in
 * sql/pre_install/tables.sql.
 *
 * A source file that includes this header has access to a catalog object,
 * which contains cached information about catalog tables, such as relation
 * OIDs.
 *
 * Generally, definitions and naming should roughly follow how things are done
 * in Postgres internally.
 */
typedef enum CatalogTable
{
	HYPERTABLE = 0,
	DIMENSION,
	DIMENSION_SLICE,
	CHUNK,
	CHUNK_CONSTRAINT,
	CHUNK_INDEX,
	TABLESPACE,
	BGW_JOB,
	BGW_JOB_STAT,
	BGW_JOB_STAT_HISTORY,
	METADATA,
	BGW_POLICY_CHUNK_STATS,
	CONTINUOUS_AGG,
	CONTINUOUS_AGGS_HYPERTABLE_INVALIDATION_LOG,
	CONTINUOUS_AGGS_INVALIDATION_THRESHOLD,
	CONTINUOUS_AGGS_MATERIALIZATION_INVALIDATION_LOG,
	COMPRESSION_SETTINGS,
	COMPRESSION_CHUNK_SIZE,
	CONTINUOUS_AGGS_BUCKET_FUNCTION,
	CONTINUOUS_AGGS_WATERMARK,
	TELEMETRY_EVENT,
	/* Don't forget updating catalog.c when adding new tables! */
	_MAX_CATALOG_TABLES,
} CatalogTable;

typedef struct TableInfoDef
{
	const char *schema_name;
	const char *table_name;
} TableInfoDef;

typedef struct TableIndexDef
{
	int length;
	char **names;
} TableIndexDef;

#define TS_CAGG_CATALOG_IDX "continuous_agg_pkey"

#define INVALID_CATALOG_TABLE _MAX_CATALOG_TABLES
#define INVALID_INDEXID -1

#define CATALOG_INTERNAL_FUNC(catalog, func) (catalog->functions[func].function_id)

#define CatalogInternalCall1(func, datum1)                                                         \
	OidFunctionCall1(CATALOG_INTERNAL_FUNC(ts_catalog_get(), func), datum1)
#define CatalogInternalCall2(func, datum1, datum2)                                                 \
	OidFunctionCall2(CATALOG_INTERNAL_FUNC(ts_catalog_get(), func), datum1, datum2)
#define CatalogInternalCall3(func, datum1, datum2, datum3)                                         \
	OidFunctionCall3(CATALOG_INTERNAL_FUNC(ts_catalog_get(), func), datum1, datum2, datum3)
#define CatalogInternalCall4(func, datum1, datum2, datum3, datum4)                                 \
	OidFunctionCall4(CATALOG_INTERNAL_FUNC(ts_catalog_get(), func), datum1, datum2, datum3, datum4)

typedef enum InternalFunction
{
	DDL_ADD_CHUNK_CONSTRAINT,
	DDL_CONSTRAINT_CLONE,
	_MAX_INTERNAL_FUNCTIONS,
} InternalFunction;

/******************************
 *
 * Hypertable table definitions
 *
 ******************************/

#define HYPERTABLE_TABLE_NAME "hypertable"

/* Hypertable table attribute numbers */
enum Anum_hypertable
{
	Anum_hypertable_id = 1,
	Anum_hypertable_schema_name,
	Anum_hypertable_table_name,
	Anum_hypertable_associated_schema_name,
	Anum_hypertable_associated_table_prefix,
	Anum_hypertable_num_dimensions,
	Anum_hypertable_chunk_sizing_func_schema,
	Anum_hypertable_chunk_sizing_func_name,
	Anum_hypertable_chunk_target_size,
	Anum_hypertable_compression_state,
	Anum_hypertable_compressed_hypertable_id,
	Anum_hypertable_status,
	_Anum_hypertable_max,
};

#define Natts_hypertable (_Anum_hypertable_max - 1)

typedef struct FormData_hypertable
{
	int32 id;
	NameData schema_name;
	NameData table_name;
	NameData associated_schema_name;
	NameData associated_table_prefix;
	int16 num_dimensions;
	NameData chunk_sizing_func_schema;
	NameData chunk_sizing_func_name;
	int64 chunk_target_size;
	int16 compression_state;
	int32 compressed_hypertable_id;
	int32 status;
} FormData_hypertable;

typedef FormData_hypertable *Form_hypertable;

/* Hypertable primary index attribute numbers */
enum Anum_hypertable_pkey_idx
{
	Anum_hypertable_pkey_idx_id = 1,
	_Anum_hypertable_pkey_max,
};

#define Natts_hypertable_pkey_idx (_Anum_hypertable_pkey_max - 1)

/* Hypertable name (schema,table) index attribute numbers */
enum Anum_hypertable_name_idx
{
	Anum_hypertable_name_idx_table = 1,
	Anum_hypertable_name_idx_schema,
	_Anum_hypertable_name_max,
};

#define Natts_hypertable_name_idx (_Anum_hypertable_name_max - 1)

enum
{
	HYPERTABLE_ID_INDEX = 0,
	HYPERTABLE_NAME_INDEX,
	_MAX_HYPERTABLE_INDEX,
};

/******************************
 *
 * Dimension table definitions
 *
 ******************************/

#define DIMENSION_TABLE_NAME "dimension"

enum Anum_dimension
{
	Anum_dimension_id = 1,
	Anum_dimension_hypertable_id,
	Anum_dimension_column_name,
	Anum_dimension_column_type,
	Anum_dimension_aligned,
	Anum_dimension_num_slices,
	Anum_dimension_partitioning_func_schema,
	Anum_dimension_partitioning_func,
	Anum_dimension_interval_length,
	Anum_dimension_compress_interval_length,
	Anum_dimension_integer_now_func_schema,
	Anum_dimension_integer_now_func,
	_Anum_dimension_max,
};

#define Natts_dimension (_Anum_dimension_max - 1)

typedef struct FormData_dimension
{
	int32 id;
	int32 hypertable_id;
	NameData column_name;
	Oid column_type;
	bool aligned;
	/* closed (space) columns */
	int16 num_slices;
	NameData partitioning_func_schema;
	NameData partitioning_func;
	/* open (time) columns */
	int64 interval_length;
	int64 compress_interval_length;
	NameData integer_now_func_schema;
	NameData integer_now_func;
} FormData_dimension;

typedef FormData_dimension *Form_dimension;

enum Anum_dimension_id_idx
{
	Anum_dimension_id_idx_id = 1,
	_Anum_dimension_id_idx_max,
};

#define Natts_dimension_id_idx (_Anum_dimension_id_idx_max - 1)

enum Anum_dimension_hypertable_id_column_name_idx
{
	Anum_dimension_hypertable_id_column_name_idx_hypertable_id = 1,
	Anum_dimension_hypertable_id_column_name_idx_column_name,
	_Anum_dimension_hypertable_id_idx_max,
};

#define Natts_dimension_hypertable_id_idx (_Anum_dimension_hypertable_id_idx_max - 1)

enum
{
	DIMENSION_ID_IDX = 0,
	DIMENSION_HYPERTABLE_ID_COLUMN_NAME_IDX,
	_MAX_DIMENSION_INDEX,
};

/******************************
 *
 * Dimension slice table definitions
 *
 ******************************/

#define DIMENSION_SLICE_TABLE_NAME "dimension_slice"

enum Anum_dimension_slice
{
	Anum_dimension_slice_id = 1,
	Anum_dimension_slice_dimension_id,
	Anum_dimension_slice_range_start,
	Anum_dimension_slice_range_end,
	_Anum_dimension_slice_max,
};

#define Natts_dimension_slice (_Anum_dimension_slice_max - 1)

typedef struct FormData_dimension_slice
{
	int32 id;
	int32 dimension_id;
	int64 range_start;
	int64 range_end;
} FormData_dimension_slice;

typedef FormData_dimension_slice *Form_dimension_slice;

enum Anum_dimension_slice_id_idx
{
	Anum_dimension_slice_id_idx_id = 1,
	_Anum_dimension_slice_id_idx_max,
};

#define Natts_dimension_slice_id_idx (_Anum_dimension_slice_id_idx_max - 1)

enum Anum_dimension_slice_dimension_id_range_start_range_end_idx
{
	Anum_dimension_slice_dimension_id_range_start_range_end_idx_dimension_id = 1,
	Anum_dimension_slice_dimension_id_range_start_range_end_idx_range_start,
	Anum_dimension_slice_dimension_id_range_start_range_end_idx_range_end,
	_Anum_dimension_slice_dimension_id_range_start_range_end_idx_max,
};

#define Natts_dimension_slice_dimension_id_range_start_range_end_idx                               \
	(_Anum_dimension_slice_dimension_id_range_start_range_end_idx_max - 1)

enum
{
	DIMENSION_SLICE_ID_IDX = 0,
	DIMENSION_SLICE_DIMENSION_ID_RANGE_START_RANGE_END_IDX,
	_MAX_DIMENSION_SLICE_INDEX,
};

/*************************
 *
 * Chunk table definitions
 *
 *************************/

#define CHUNK_TABLE_NAME "chunk"

enum Anum_chunk
{
	Anum_chunk_id = 1,
	Anum_chunk_hypertable_id,
	Anum_chunk_schema_name,
	Anum_chunk_table_name,
	Anum_chunk_compressed_chunk_id,
	Anum_chunk_dropped,
	Anum_chunk_status,
	Anum_chunk_osm_chunk,
	Anum_chunk_creation_time,
	_Anum_chunk_max,
};

#define Natts_chunk (_Anum_chunk_max - 1)

typedef struct FormData_chunk
{
	int32 id;
	int32 hypertable_id;
	NameData schema_name;
	NameData table_name;
	int32 compressed_chunk_id;
	bool dropped;
	int32 status;
	bool osm_chunk;
	TimestampTz creation_time;
} FormData_chunk;

typedef FormData_chunk *Form_chunk;

enum
{
	CHUNK_ID_INDEX = 0,
	CHUNK_HYPERTABLE_ID_INDEX,
	CHUNK_SCHEMA_NAME_INDEX,
	CHUNK_COMPRESSED_CHUNK_ID_INDEX,
	CHUNK_OSM_CHUNK_INDEX,
	CHUNK_HYPERTABLE_ID_CREATION_TIME_INDEX,
	_MAX_CHUNK_INDEX,
};

enum Anum_chunk_idx
{
	Anum_chunk_idx_id = 1,
};

enum Anum_chunk_hypertable_id_idx
{
	Anum_chunk_hypertable_id_idx_hypertable_id = 1,
};

enum Anum_chunk_compressed_chunk_id_idx
{
	Anum_chunk_compressed_chunk_id_idx_compressed_chunk_id = 1,
};

enum Anum_chunk_schema_name_idx
{
	Anum_chunk_schema_name_idx_schema_name = 1,
	Anum_chunk_schema_name_idx_table_name,
};

enum Anum_chunk_osm_chunk_idx
{
	Anum_chunk_osm_chunk_idx_osm_chunk = 1,
	Anum_chunk_osm_chunk_idx_hypertable_id,
};

enum Anum_chunk_hypertable_id_creation_time_idx
{
	Anum_chunk_hypertable_id_creation_time_idx_hypertable_id = 1,
	Anum_chunk_hypertable_id_creation_time_idx_creation_time,
};

/************************************
 *
 * Chunk constraint table definitions
 *
 ************************************/

#define CHUNK_CONSTRAINT_TABLE_NAME "chunk_constraint"

enum Anum_chunk_constraint
{
	Anum_chunk_constraint_chunk_id = 1,
	Anum_chunk_constraint_dimension_slice_id,
	Anum_chunk_constraint_constraint_name,
	Anum_chunk_constraint_hypertable_constraint_name,
	_Anum_chunk_constraint_max,
};

#define Natts_chunk_constraint (_Anum_chunk_constraint_max - 1)

/* Do Not use GET_STRUCT with FormData_chunk_constraint. It contains NULLS */
typedef struct FormData_chunk_constraint
{
	int32 chunk_id;
	int32 dimension_slice_id;
	NameData constraint_name;
	NameData hypertable_constraint_name;
} FormData_chunk_constraint;

typedef FormData_chunk_constraint *Form_chunk_constraint;

enum
{
	CHUNK_CONSTRAINT_CHUNK_ID_CONSTRAINT_NAME_IDX = 0,
	CHUNK_CONSTRAINT_DIMENSION_SLICE_ID_IDX,
	_MAX_CHUNK_CONSTRAINT_INDEX,
};

enum Anum_chunk_constraint_dimension_slice_id_idx
{
	Anum_chunk_constraint_dimension_slice_id_idx_dimension_slice_id = 1,
	_Anum_chunk_constraint_dimension_slice_id_idx_max,
};

enum Anum_chunk_constraint_chunk_id_constraint_name_idx
{
	Anum_chunk_constraint_chunk_id_constraint_name_idx_chunk_id = 1,
	Anum_chunk_constraint_chunk_id_constraint_name_idx_constraint_name,
	_Anum_chunk_constraint_chunk_id_constraint_name_idx_max,
};

/************************************
 *
 * Chunk index table definitions
 *
 ************************************/

#define CHUNK_INDEX_TABLE_NAME "chunk_index"

enum Anum_chunk_index
{
	Anum_chunk_index_chunk_id = 1,
	Anum_chunk_index_index_name,
	Anum_chunk_index_hypertable_id,
	Anum_chunk_index_hypertable_index_name,
	_Anum_chunk_index_max,
};

#define Natts_chunk_index (_Anum_chunk_index_max - 1)

typedef struct FormData_chunk_index
{
	int32 chunk_id;
	NameData index_name;
	int32 hypertable_id;
	NameData hypertable_index_name;
} FormData_chunk_index;

typedef FormData_chunk_index *Form_chunk_index;

enum
{
	CHUNK_INDEX_CHUNK_ID_INDEX_NAME_IDX = 0,
	CHUNK_INDEX_HYPERTABLE_ID_HYPERTABLE_INDEX_NAME_IDX,
	_MAX_CHUNK_INDEX_INDEX,
};

enum Anum_chunk_index_chunk_id_index_name_idx
{
	Anum_chunk_index_chunk_id_index_name_idx_chunk_id = 1,
	Anum_chunk_index_chunk_id_index_name_idx_index_name,
	_Anum_chunk_index_chunk_id_index_name_idx_max,
};

enum Anum_chunk_index_hypertable_id_hypertable_index_name_idx
{
	Anum_chunk_index_hypertable_id_hypertable_index_name_idx_hypertable_id = 1,
	Anum_chunk_index_hypertable_id_hypertable_index_name_idx_hypertable_index_name,
	Anum_chunk_index_hypertable_id_hypertable_index_name_idx_max,
};

/************************************
 *
 * Tablespace table definitions
 *
 ************************************/

#define TABLESPACE_TABLE_NAME "tablespace"

enum Anum_tablespace
{
	Anum_tablespace_id = 1,
	Anum_tablespace_hypertable_id,
	Anum_tablespace_tablespace_name,
	_Anum_tablespace_max,
};

#define Natts_tablespace (_Anum_tablespace_max - 1)

typedef struct FormData_tablespace
{
	int32 id;
	int32 hypertable_id;
	NameData tablespace_name;
} FormData_tablespace;

typedef FormData_tablespace *Form_tablespace;

enum
{
	TABLESPACE_PKEY_IDX = 0,
	TABLESPACE_HYPERTABLE_ID_TABLESPACE_NAME_IDX,
	_MAX_TABLESPACE_INDEX,
};

enum Anum_tablespace_pkey_idx
{
	Anum_tablespace_pkey_idx_tablespace_id = 1,
	_Anum_tablespace_pkey_idx_max,
};

typedef struct FormData_tablespace_pkey_idx
{
	int32 tablespace_id;
} FormData_tablespace_pkey_idx;

enum Anum_tablespace_hypertable_id_tablespace_name_idx
{
	Anum_tablespace_hypertable_id_tablespace_name_idx_hypertable_id = 1,
	Anum_tablespace_hypertable_id_tablespace_name_idx_tablespace_name,
	_Anum_tablespace_hypertable_id_tablespace_name_idx_max,
};

typedef struct FormData_tablespace_hypertable_id_tablespace_name_idx
{
	int32 hypertable_id;
	NameData tablespace_name;
} FormData_tablespace_hypertable_id_tablespace_name_idx;

/************************************
 *
 * bgw_job table definitions
 *
 ************************************/

#define BGW_JOB_TABLE_NAME "bgw_job"

enum Anum_bgw_job
{
	Anum_bgw_job_id = 1,
	Anum_bgw_job_application_name,
	Anum_bgw_job_schedule_interval,
	Anum_bgw_job_max_runtime,
	Anum_bgw_job_max_retries,
	Anum_bgw_job_retry_period,
	Anum_bgw_job_proc_schema,
	Anum_bgw_job_proc_name,
	Anum_bgw_job_owner,
	Anum_bgw_job_scheduled,
	Anum_bgw_job_fixed_schedule,
	Anum_bgw_job_initial_start,
	Anum_bgw_job_hypertable_id,
	Anum_bgw_job_config,
	Anum_bgw_job_check_schema,
	Anum_bgw_job_check_name,
	Anum_bgw_job_timezone,
	_Anum_bgw_job_max,
};

#define Natts_bgw_job (_Anum_bgw_job_max - 1)

/* fixed_schedule needs to come before the varlen fields
 for GETSTRUCT to work */
typedef struct FormData_bgw_job
{
	int32 id;
	NameData application_name;
	Interval schedule_interval;
	Interval max_runtime;
	int32 max_retries;
	Interval retry_period;
	NameData proc_schema;
	NameData proc_name;
	Oid owner;
	bool scheduled;
	bool fixed_schedule;
	TimestampTz initial_start;
	int32 hypertable_id;
	Jsonb *config;
	NameData check_schema;
	NameData check_name;
	text *timezone;
} FormData_bgw_job;

typedef FormData_bgw_job *Form_bgw_job;

enum
{
	BGW_JOB_PKEY_IDX = 0,
	BGW_JOB_PROC_HYPERTABLE_ID_IDX,
	_MAX_BGW_JOB_INDEX,
};

enum Anum_bgw_job_pkey_idx
{
	Anum_bgw_job_pkey_idx_id = 1,
	_Anum_bgw_job_pkey_idx_max,
};

#define Natts_bjw_job_pkey_idx (_Anum_bgw_job_pkey_idx_max - 1)

enum Anum_bgw_job_proc_hypertable_id_idx
{
	Anum_bgw_job_proc_hypertable_id_idx_proc_schema = 1,
	Anum_bgw_job_proc_hypertable_id_idx_proc_name,
	Anum_bgw_job_proc_hypertable_id_idx_hypertable_id,
	_Anum_bgw_job_proc_hypertable_id_idx_max,
};

#define Natts_bgw_job_proc_hypertable_id_idx (_Anum_bgw_job_proc_hypertable_id_idx_max - 1)

/************************************
 *
 * bgw_job_stat table definitions
 *
 ************************************/

#define BGW_JOB_STAT_TABLE_NAME "bgw_job_stat"

enum Anum_bgw_job_stat
{
	Anum_bgw_job_stat_job_id = 1,
	Anum_bgw_job_stat_last_start,
	Anum_bgw_job_stat_last_finish,
	Anum_bgw_job_stat_next_start,
	Anum_bgw_job_stat_last_successful_finish,
	Anum_bgw_job_stat_last_run_success,
	Anum_bgw_job_stat_total_runs,
	Anum_bgw_job_stat_total_duration,
	Anum_bgw_job_stat_total_duration_failures,
	Anum_bgw_job_stat_total_success,
	Anum_bgw_job_stat_total_failures,
	Anum_bgw_job_stat_total_crashes,
	Anum_bgw_job_stat_consecutive_failures,
	Anum_bgw_job_stat_consecutive_crashes,
	Anum_bgw_job_stat_flags,
	_Anum_bgw_job_stat_max,
};

#define Natts_bgw_job_stat (_Anum_bgw_job_stat_max - 1)

typedef struct FormData_bgw_job_stat
{
	int32 id;
	TimestampTz last_start;
	TimestampTz last_finish;
	TimestampTz next_start;
	TimestampTz last_successful_finish;
	bool last_run_success;
	int64 total_runs;
	Interval total_duration;
	Interval total_duration_failures;
	int64 total_success;
	int64 total_failures;
	int64 total_crashes;
	int32 consecutive_failures;
	int32 consecutive_crashes;
	int32 flags;
} FormData_bgw_job_stat;

typedef FormData_bgw_job_stat *Form_bgw_job_stat;

enum
{
	BGW_JOB_STAT_PKEY_IDX = 0,
	_MAX_BGW_JOB_STAT_INDEX,
};

enum Anum_bgw_job_stat_pkey_idx
{
	Anum_bgw_job_stat_pkey_idx_job_id = 1,
	_Anum_bgw_job_stat_pkey_idx_max,
};

#define Natts_bjw_job_stat_pkey_idx (_Anum_bgw_job_stat_pkey_idx_max - 1)

#define BGW_JOB_STAT_HISTORY_TABLE_NAME "bgw_job_stat_history"

enum Anum_bgw_job_stat_history
{
	Anum_bgw_job_stat_history_id = 1,
	Anum_bgw_job_stat_history_job_id,
	Anum_bgw_job_stat_history_pid,
	Anum_bgw_job_stat_history_execution_start,
	Anum_bgw_job_stat_history_execution_finish,
	Anum_bgw_job_stat_history_succeeded,
	Anum_bgw_job_stat_history_data,
	_Anum_bgw_job_stat_history_max,
};

#define Natts_bgw_job_stat_history (_Anum_bgw_job_stat_history_max - 1)

typedef struct FormData_bgw_job_stat_history
{
	int64 id;
	int32 job_id;
	int32 pid;
	TimestampTz execution_start;
	TimestampTz execution_finish;
	bool succeeded;
	Jsonb *data;
} FormData_bgw_job_stat_history;

typedef FormData_bgw_job_stat_history *Form_bgw_job_stat_history;

enum
{
	BGW_JOB_STAT_HISTORY_PKEY_IDX = 0,
	_MAX_BGW_JOB_STAT_HISTORY_INDEX,
};

enum Anum_bgw_job_stat_history_pkey_idx
{
	Anum_bgw_job_stat_history_pkey_idx_id = 1,
	_Anum_bgw_job_stat_history_pkey_idx_max,
};

#define Natts_bjw_job_stat_history_pkey_idx (_Anum_bgw_job_stat_history_pkey_idx_max - 1)

/******************************
 *
 * metadata table definitions
 *
 ******************************/

#define METADATA_TABLE_NAME "metadata"

enum Anum_metadata
{
	Anum_metadata_key = 1,
	Anum_metadata_value,
	Anum_metadata_include_in_telemetry,
	_Anum_metadata_max,
};

#define Natts_metadata (_Anum_metadata_max - 1)

typedef struct FormData_metadata
{
	NameData key;
	text *value;
} FormData_metadata;

typedef FormData_metadata *Form_metadata;

/* metadata primary index attribute numbers */
enum Anum_metadata_pkey_idx
{
	Anum_metadata_pkey_idx_id = 1,
	_Anum_metadata_pkey_max,
};

#define Natts_metadata_pkey_idx (_Anum_metadata_pkey_max - 1)

enum
{
	METADATA_PKEY_IDX = 0,
	_MAX_METADATA_INDEX,
};

/*
 * telemetry_event table definition
 */

#define TELEMETRY_EVENT_TABLE_NAME "telemetry_event"

enum Anum_telemetry_event
{
	Anum_telemetry_event_created = 1,
	Anum_telemetry_event_tag,
	Anum_telemetry_event_body,
	_Anum_telemetry_event_max,
};

#define Natts_telemetry_event_max (_Anum_telemetry_event_max - 1)

/****** BGW_POLICY_CHUNK_STATS TABLE definitions */
#define BGW_POLICY_CHUNK_STATS_TABLE_NAME "bgw_policy_chunk_stats"

enum Anum_bgw_policy_chunk_stats
{
	Anum_bgw_policy_chunk_stats_job_id = 1,
	Anum_bgw_policy_chunk_stats_chunk_id,
	Anum_bgw_policy_chunk_stats_num_times_job_run,
	Anum_bgw_policy_chunk_stats_last_time_job_run,
	_Anum_bgw_policy_chunk_stats_max,
};

#define Natts_bgw_policy_chunk_stats (_Anum_bgw_policy_chunk_stats_max - 1)

typedef struct FormData_bgw_policy_chunk_stats
{
	int32 job_id;
	int32 chunk_id;
	int32 num_times_job_run;
	TimestampTz last_time_job_run;
} FormData_bgw_policy_chunk_stats;

typedef FormData_bgw_policy_chunk_stats *Form_bgw_job_chunk_stats;

enum
{
	BGW_POLICY_CHUNK_STATS_JOB_ID_CHUNK_ID_IDX = 0,
	_MAX_BGW_POLICY_CHUNK_STATS_INDEX,
};

enum Anum_bgw_policy_chunk_stats_job_id_chunk_id_idx
{
	Anum_bgw_policy_chunk_stats_job_id_chunk_id_idx_job_id = 1,
	Anum_bgw_policy_chunk_stats_job_id_chunk_id_idx_chunk_id,
	_Anum_bgw_policy_chunk_stats_job_id_chunk_id_idx_max,
};

typedef struct FormData_bgw_policy_chunk_stats_job_id_chunk_id_idx
{
	int32 job_id;
	int32 chunk_id;
} FormData_bgw_policy_chunk_stats_job_id_chunk_id_idx;

/******************************************
 *
 * continuous_agg table definitions
 *
 ******************************************/
#define CONTINUOUS_AGG_TABLE_NAME "continuous_agg"
typedef enum Anum_continuous_agg
{
	Anum_continuous_agg_mat_hypertable_id = 1,
	Anum_continuous_agg_raw_hypertable_id,
	Anum_continuous_agg_parent_mat_hypertable_id,
	Anum_continuous_agg_user_view_schema,
	Anum_continuous_agg_user_view_name,
	Anum_continuous_agg_partial_view_schema,
	Anum_continuous_agg_partial_view_name,
	Anum_continuous_agg_direct_view_schema,
	Anum_continuous_agg_direct_view_name,
	Anum_continuous_agg_materialize_only,
	Anum_continuous_agg_finalized,
	_Anum_continuous_agg_max,
} Anum_continuous_agg;

#define Natts_continuous_agg (_Anum_continuous_agg_max - 1)

typedef struct FormData_continuous_agg
{
	int32 mat_hypertable_id;
	int32 raw_hypertable_id;
	int32 parent_mat_hypertable_id; /* Nested Continuous Aggregate */
	NameData user_view_schema;
	NameData user_view_name;
	NameData partial_view_schema;
	NameData partial_view_name;
	NameData direct_view_schema;
	NameData direct_view_name;
	bool materialized_only;
	bool finalized;
} FormData_continuous_agg;

typedef FormData_continuous_agg *Form_continuous_agg;

enum
{
	CONTINUOUS_AGG_PARTIAL_VIEW_SCHEMA_PARTIAL_VIEW_NAME_KEY = 0,
	CONTINUOUS_AGG_PKEY,
	CONTINUOUS_AGG_USER_VIEW_SCHEMA_USER_VIEW_NAME_KEY,
	CONTINUOUS_AGG_RAW_HYPERTABLE_ID_IDX,
	_MAX_CONTINUOUS_AGG_INDEX,
};

typedef enum Anum_continuous_agg_partial_view_schema_partial_view_name_key
{
	Anum_continuous_agg_partial_view_schema_partial_view_name_key_partial_view_schema = 1,
	Anum_continuous_agg_partial_view_schema_partial_view_name_key_partial_view_name,
	_Anum_continuous_agg_partial_view_schema_partial_view_name_key_max,
} Anum_continuous_agg_partial_view_schema_partial_view_name_key;

#define Natts_continuous_agg_partial_view_schema_partial_view_name_key                             \
	(_Anum_continuous_agg_partial_view_schema_partial_view_name_key_max - 1)

typedef enum Anum_continuous_agg_pkey
{
	Anum_continuous_agg_pkey_mat_hypertable_id = 1,
	_Anum_continuous_agg_pkey_max,
} Anum_continuous_agg_pkey;

#define Natts_continuous_agg_pkey (_Anum_continuous_agg_pkey_max - 1)

typedef enum Anum_continuous_agg_user_view_schema_user_view_name_key
{
	Anum_continuous_agg_user_view_schema_user_view_name_key_user_view_schema = 1,
	Anum_continuous_agg_user_view_schema_user_view_name_key_user_view_name,
	_Anum_continuous_agg_user_view_schema_user_view_name_key_max,
} Anum_continuous_agg_user_view_schema_user_view_name_key;

#define Natts_continuous_agg_user_view_schema_user_view_name_key                                   \
	(_Anum_continuous_agg_user_view_schema_user_view_name_key_max - 1)

typedef enum Anum_continuous_agg_raw_hypertable_id_idx
{
	Anum_continuous_agg_raw_hypertable_id_idx_raw_hypertable_id = 1,
	_Anum_continuous_agg_raw_hypertable_id_idx_max,
} Anum_continuous_agg_raw_hypertable_id_idx;

#define Natts_continuous_agg_raw_hypertable_id_idx                                                 \
	(_Anum_continuous_agg_raw_hypertable_id_idx_max - 1)

/*** continuous_aggs_bucket_function table definitions ***/

#define CONTINUOUS_AGGS_BUCKET_FUNCTION_TABLE_NAME "continuous_aggs_bucket_function"
typedef enum Anum_continuous_aggs_bucket_function
{
	Anum_continuous_aggs_bucket_function_mat_hypertable_id = 1,
	Anum_continuous_aggs_bucket_function_function,
	Anum_continuous_aggs_bucket_function_bucket_width,
	Anum_continuous_aggs_bucket_function_bucket_origin,
	Anum_continuous_aggs_bucket_function_bucket_offset,
	Anum_continuous_aggs_bucket_function_bucket_timezone,
	Anum_continuous_aggs_bucket_function_bucket_fixed_width,
	_Anum_continuous_aggs_bucket_function_max,
} Anum_continuous_aggs_bucket_function;

#define Natts_continuous_aggs_bucket_function (_Anum_continuous_aggs_bucket_function_max - 1)

enum
{
	CONTINUOUS_AGGS_BUCKET_FUNCTION_PKEY_IDX = 0,
	_MAX_CONTINUOUS_AGGS_BUCKET_FUNCTION_INDEX,
};

typedef enum Anum_continuous_aggs_bucket_function_pkey
{
	Anum_continuous_aggs_bucket_function_pkey_mat_hypertable_id = 1,
	_Anum_continuous_aggs_bucket_function_pkey_max,
} Anum_continuous_aggs_bucket_function_pkey;

#define Natts_continuous_aggs_bucket_function_pkey                                                 \
	(_Anum_continuous_aggs_bucket_function_pkey_max - 1)

/****** CONTINUOUS_AGGS_HYPERTABLE_INVALIDATION_LOG_TABLE definitions*/
#define CONTINUOUS_AGGS_HYPERTABLE_INVALIDATION_LOG_TABLE_NAME                                     \
	"continuous_aggs_hypertable_invalidation_log"
typedef enum Anum_continuous_aggs_hypertable_invalidation_log
{
	Anum_continuous_aggs_hypertable_invalidation_log_hypertable_id = 1,
	Anum_continuous_aggs_hypertable_invalidation_log_lowest_modified_value,
	Anum_continuous_aggs_hypertable_invalidation_log_greatest_modified_value,
	_Anum_continuous_aggs_hypertable_invalidation_log_max,
} Anum_continuous_aggs_hypertable_invalidation_log;

#define Natts_continuous_aggs_hypertable_invalidation_log                                          \
	(_Anum_continuous_aggs_hypertable_invalidation_log_max - 1)

typedef struct FormData_continuous_aggs_hypertable_invalidation_log
{
	int32 hypertable_id;
	int64 lowest_modified_value;
	int64 greatest_modified_value;
} FormData_continuous_aggs_hypertable_invalidation_log;

typedef FormData_continuous_aggs_hypertable_invalidation_log
	*Form_continuous_aggs_hypertable_invalidation_log;

enum
{
	CONTINUOUS_AGGS_HYPERTABLE_INVALIDATION_LOG_IDX = 0,
	_MAX_CONTINUOUS_AGGS_HYPERTABLE_INVALIDATION_LOG_INDEX,
};
typedef enum Anum_continuous_aggs_hypertable_invalidation_log_idx
{
	Anum_continuous_aggs_hypertable_invalidation_log_idx_hypertable_id = 1,
	Anum_continuous_aggs_hypertable_invalidation_log_idx_lowest_modified_value,
	_Anum_continuous_aggs_hypertable_invalidation_log_idx_max,
} Anum_continuous_aggs_hypertable_invalidation_log_idx;

#define Natts_continuous_aggs_hypertable_invalidation_log_idx                                      \
	(_Anum_continuous_aggs_hypertable_invalidation_log_idx_max - 1)

/****** CONTINUOUS_AGGS_INVALIDATION_THRESHOLD_TABLE definitions*/
#define CONTINUOUS_AGGS_INVALIDATION_THRESHOLD_TABLE_NAME "continuous_aggs_invalidation_threshold"
typedef enum Anum_continuous_aggs_invalidation_threshold
{
	Anum_continuous_aggs_invalidation_threshold_hypertable_id = 1,
	Anum_continuous_aggs_invalidation_threshold_watermark,
	_Anum_continuous_aggs_invalidation_threshold_max,
} Anum_continuous_aggs_invalidation_threshold;

#define Natts_continuous_aggs_invalidation_threshold                                               \
	(_Anum_continuous_aggs_invalidation_threshold_max - 1)

typedef struct FormData_continuous_aggs_invalidation_threshold
{
	int32 hypertable_id;
	int64 watermark;
} FormData_continuous_aggs_invalidation_threshold;

typedef FormData_continuous_aggs_invalidation_threshold
	*Form_continuous_aggs_invalidation_threshold;

enum
{
	CONTINUOUS_AGGS_INVALIDATION_THRESHOLD_PKEY = 0,
	_MAX_CONTINUOUS_AGGS_INVALIDATION_THRESHOLD_INDEX,
};
typedef enum Anum_continuous_aggs_invalidation_threshold_pkey
{
	Anum_continuous_aggs_invalidation_threshold_pkey_hypertable_id = 1,
	_Anum_continuous_aggs_invalidation_threshold_pkey_max,
} Anum_continuous_aggs_invalidation_threshold_pkey;

#define Natts_continuous_aggs_invalidation_threshold_pkey                                          \
	(_Anum_continuous_aggs_invalidation_threshold_pkey_max - 1)

/****** CONTINUOUS_AGGS_MATERIALIZATION_INVALIDATION_LOG_TABLE definitions*/
#define CONTINUOUS_AGGS_MATERIALIZATION_INVALIDATION_LOG_TABLE_NAME                                \
	"continuous_aggs_materialization_invalidation_log"
typedef enum Anum_continuous_aggs_materialization_invalidation_log
{
	Anum_continuous_aggs_materialization_invalidation_log_materialization_id = 1,
	Anum_continuous_aggs_materialization_invalidation_log_lowest_modified_value,
	Anum_continuous_aggs_materialization_invalidation_log_greatest_modified_value,
	_Anum_continuous_aggs_materialization_invalidation_log_max,
} Anum_continuous_aggs_materialization_invalidation_log;

#define Natts_continuous_aggs_materialization_invalidation_log                                     \
	(_Anum_continuous_aggs_materialization_invalidation_log_max - 1)

typedef struct FormData_continuous_aggs_materialization_invalidation_log
{
	int32 materialization_id;
	int64 lowest_modified_value;
	int64 greatest_modified_value;
} FormData_continuous_aggs_materialization_invalidation_log;

typedef FormData_continuous_aggs_materialization_invalidation_log
	*Form_continuous_aggs_materialization_invalidation_log;

enum
{
	CONTINUOUS_AGGS_MATERIALIZATION_INVALIDATION_LOG_IDX = 0,
	_MAX_CONTINUOUS_AGGS_MATERIALIZATION_INVALIDATION_LOG_INDEX,
};
typedef enum Anum_continuous_aggs_materialization_invalidation_log_idx
{
	Anum_continuous_aggs_materialization_invalidation_log_idx_materialization_id = 1,
	Anum_continuous_aggs_materialization_invalidation_log_idx_lowest_modified_value,
	_Anum_continuous_aggs_materialization_invalidation_log_idx_max,
} Anum_continuous_aggs_materialization_invalidation_log_idx;

#define Natts_continuous_aggs_materialization_invalidation_log_idx                                 \
	(_Anum_continuous_aggs_materialization_invalidation_log_idx_max - 1)

/****** CONTINUOUS_AGGS_WATERMARK_TABLE definitions*/
#define CONTINUOUS_AGGS_WATERMARK_TABLE_NAME "continuous_aggs_watermark"
typedef enum Anum_continuous_aggs_watermark
{
	Anum_continuous_aggs_watermark_mat_hypertable_id = 1,
	Anum_continuous_aggs_watermark_watermark,
	_Anum_continuous_aggs_watermark_max,
} Anum_continuous_aggs_watermark;

#define Natts_continuous_aggs_watermark (_Anum_continuous_aggs_watermark_max - 1)

typedef struct FormData_continuous_aggs_watermark
{
	int32 mat_hypertable_id;
	int64 watermark;
} FormData_continuous_aggs_watermark;

typedef FormData_continuous_aggs_watermark *Form_continuous_aggs_watermark;

enum
{
	CONTINUOUS_AGGS_WATERMARK_PKEY = 0,
	_MAX_CONTINUOUS_AGGS_WATERMARK_INDEX,
};

typedef enum Anum_continuous_aggs_watermark_pkey
{
	Anum_continuous_aggs_watermark_pkey_mat_hypertable_id = 1,
	_Anum_continuous_aggs_watermark_pkey_max,
} Anum_continuous_aggs_watermark_pkey;

#define Natts_continuous_aggs_watermark_pkey (_Anum_continuous_aggs_watermark_pkey_max - 1)

#define COMPRESSION_SETTINGS_TABLE_NAME "compression_settings"

typedef enum Anum_compression_settings
{
	Anum_compression_settings_relid = 1,
	Anum_compression_settings_segmentby,
	Anum_compression_settings_orderby,
	Anum_compression_settings_orderby_desc,
	Anum_compression_settings_orderby_nullsfirst,
	_Anum_compression_settings_max,
} Anum_compression_settings;

#define Natts_compression_settings (_Anum_compression_settings_max - 1)

typedef struct FormData_compression_settings
{
	Oid relid;
	ArrayType *segmentby;
	ArrayType *orderby;
	ArrayType *orderby_desc;
	ArrayType *orderby_nullsfirst;
} FormData_compression_settings;

typedef FormData_compression_settings *Form_compression_settings;

enum
{
	COMPRESSION_SETTINGS_PKEY = 0,
	_MAX_COMPRESSION_SETTINGS_INDEX,
};

typedef enum Anum_compression_settings_pkey
{
	Anum_compression_settings_pkey_relid = 1,
	_Anum_compression_settings_pkey_max,
} Anum_compression_settings_pkey;

#define Natts_compression_chunk_size_pkey (_Anum_compression_chunk_size_pkey_max - 1)

#define COMPRESSION_CHUNK_SIZE_TABLE_NAME "compression_chunk_size"
typedef enum Anum_compression_chunk_size
{
	Anum_compression_chunk_size_chunk_id = 1,
	Anum_compression_chunk_size_compressed_chunk_id,
	Anum_compression_chunk_size_uncompressed_heap_size,
	Anum_compression_chunk_size_uncompressed_toast_size,
	Anum_compression_chunk_size_uncompressed_index_size,
	Anum_compression_chunk_size_compressed_heap_size,
	Anum_compression_chunk_size_compressed_toast_size,
	Anum_compression_chunk_size_compressed_index_size,
	Anum_compression_chunk_size_numrows_pre_compression,
	Anum_compression_chunk_size_numrows_post_compression,
	Anum_compression_chunk_size_numrows_frozen_immediately,
	_Anum_compression_chunk_size_max,
} Anum_compression_chunk_size;

#define Natts_compression_chunk_size (_Anum_compression_chunk_size_max - 1)

typedef struct FormData_compression_chunk_size
{
	int32 chunk_id;
	int32 compressed_chunk_id;
	int64 uncompressed_heap_size;
	int64 uncompressed_toast_size;
	int64 uncompressed_index_size;
	int64 compressed_heap_size;
	int64 compressed_toast_size;
	int64 compressed_index_size;
	int64 numrows_pre_compression;
	int64 numrows_post_compression;
	int64 numrows_frozen_immediately;
} FormData_compression_chunk_size;

typedef FormData_compression_chunk_size *Form_compression_chunk_size;

enum
{
	COMPRESSION_CHUNK_SIZE_PKEY = 0,
	_MAX_COMPRESSION_CHUNK_SIZE_INDEX,
};
typedef enum Anum_compression_chunk_size_pkey
{
	Anum_compression_chunk_size_pkey_chunk_id = 1,
	_Anum_compression_chunk_size_pkey_max,
} Anum_compression_chunk_size_pkey;

#define Natts_compression_chunk_size_pkey (_Anum_compression_chunk_size_pkey_max - 1)

/*
 * The maximum number of indexes a catalog table can have.
 * This needs to be bumped in case of new catalog tables that have more indexes.
 */
#define _MAX_TABLE_INDEXES 6

typedef enum CacheType
{
	CACHE_TYPE_HYPERTABLE,
	CACHE_TYPE_BGW_JOB,
	CACHE_TYPE_EXTENSION,
	_MAX_CACHE_TYPES
} CacheType;

typedef struct CatalogTableInfo
{
	const char *schema_name;
	const char *name;
	Oid id;
	Oid serial_relid;
	Oid index_ids[_MAX_TABLE_INDEXES];
} CatalogTableInfo;

typedef struct CatalogDatabaseInfo
{
	NameData database_name;
	Oid database_id;
	Oid schema_id;
	Oid owner_uid;
} CatalogDatabaseInfo;

typedef struct Catalog
{
	CatalogTableInfo tables[_MAX_CATALOG_TABLES];
	Oid extension_schema_id[_TS_MAX_SCHEMA];

	struct
	{
		Oid inval_proxy_id;
	} caches[_MAX_CACHE_TYPES];

	struct
	{
		Oid function_id;
	} functions[_MAX_INTERNAL_FUNCTIONS];

	bool initialized;
} Catalog;

typedef struct CatalogSecurityContext
{
	Oid saved_uid;
	int saved_security_context;
} CatalogSecurityContext;

#define HYPERTABLE_STATUS_DEFAULT 0
/* flag set when hypertable has an attached OSM chunk */
#define HYPERTABLE_STATUS_OSM 1
/*
 * Currently, the time slice range metadata is updated in
 * the timescaledb catalog with the min and max of the range managed by OSM.
 * However, this range has to be contiguous in order to
 * update our catalog with its min and max value. If it is not contiguous,
 * then we cannot store the min and max in our catalog because tuple routing
 * will not work properly with gaps in the range.
 * When attempting to insert into one of the gaps, which do not in fact contain
 * tiered data, we error out because this is perceived as an attempt to insert
 * into tiered chunks, which are immutable.
 * When the range is noncontiguous, we store [INT64_MAX - 1, INT64_MAX) and set
 * this flag.
 * This flag also serves to allow or block the ordered append optimization. When
 * the range covered by OSM is contiguous, then it is possible to do ordered
 * append.
 */
#define HYPERTABLE_STATUS_OSM_CHUNK_NONCONTIGUOUS 2

extern void ts_catalog_table_info_init(CatalogTableInfo *tables, int max_table,
									   const TableInfoDef *table_ary,
									   const TableIndexDef *index_ary, const char **serial_id_ary);

extern TSDLLEXPORT CatalogDatabaseInfo *ts_catalog_database_info_get(void);
extern TSDLLEXPORT Catalog *ts_catalog_get(void);
extern void ts_catalog_reset(void);
extern bool ts_is_catalog_table(Oid relid);

/* Functions should operate on a passed-in Catalog struct */
static inline Oid
catalog_get_table_id(Catalog *catalog, CatalogTable tableid)
{
	return catalog->tables[tableid].id;
}

static inline Oid
catalog_get_index(Catalog *catalog, CatalogTable tableid, int indexid)
{
	return (indexid == INVALID_INDEXID) ? InvalidOid : catalog->tables[tableid].index_ids[indexid];
}

extern TSDLLEXPORT int64 ts_catalog_table_next_seq_id(const Catalog *catalog, CatalogTable table);
extern Oid ts_catalog_get_cache_proxy_id(Catalog *catalog, CacheType type);

/* Functions that modify the actual catalog table on disk */
extern TSDLLEXPORT bool ts_catalog_database_info_become_owner(CatalogDatabaseInfo *database_info,
															  CatalogSecurityContext *sec_ctx);
extern TSDLLEXPORT void ts_catalog_restore_user(CatalogSecurityContext *sec_ctx);

extern TSDLLEXPORT void ts_catalog_insert_only(Relation rel, HeapTuple tuple);
extern TSDLLEXPORT void ts_catalog_insert(Relation rel, HeapTuple tuple);
extern TSDLLEXPORT void ts_catalog_insert_values(Relation rel, TupleDesc tupdesc, Datum *values,
												 bool *nulls);
extern TSDLLEXPORT void ts_catalog_insert_datums(Relation rel, TupleDesc tupdesc,
												 NullableDatum *datums);
extern TSDLLEXPORT void ts_catalog_update_tid_only(Relation rel, ItemPointer tid, HeapTuple tuple);
extern TSDLLEXPORT void ts_catalog_update_tid(Relation rel, ItemPointer tid, HeapTuple tuple);
extern TSDLLEXPORT void ts_catalog_update(Relation rel, HeapTuple tuple);
extern TSDLLEXPORT void ts_catalog_delete_tid_only(Relation rel, ItemPointer tid);
extern TSDLLEXPORT void ts_catalog_delete_tid(Relation rel, ItemPointer tid);
extern TSDLLEXPORT void ts_catalog_invalidate_cache(Oid catalog_relid, CmdType operation);
extern TSDLLEXPORT ResultRelInfo *ts_catalog_open_indexes(Relation heapRel);
extern TSDLLEXPORT void ts_catalog_close_indexes(ResultRelInfo *indstate);
extern TSDLLEXPORT void ts_catalog_index_insert(ResultRelInfo *indstate, HeapTuple heapTuple);

bool TSDLLEXPORT ts_catalog_scan_one(CatalogTable table, int indexid, ScanKeyData *scankey,
									 int num_keys, tuple_found_func tuple_found, LOCKMODE lockmode,
									 char *policy_type, void *data);
void TSDLLEXPORT ts_catalog_scan_all(CatalogTable table, int indexid, ScanKeyData *scankey,
									 int num_keys, tuple_found_func tuple_found, LOCKMODE lockmode,
									 void *data);
