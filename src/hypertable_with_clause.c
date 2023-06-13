/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <fmgr.h>
#include <access/htup_details.h>
#include <catalog/dependency.h>
#include <catalog/namespace.h>
#include <catalog/pg_type.h>
#include <catalog/pg_trigger.h>
#include <commands/trigger.h>
#include <storage/lmgr.h>
#include <utils/builtins.h>
#include <utils/lsyscache.h>
#include <parser/parser.h>
#include <parser/parse_func.h>

#include "extension.h"
#include "utils.h"
#include "compat/compat.h"
#include "hypertable.h"
#include "hypertable_cache.h"
#include "hypertable_with_clause.h"
#include "compression_with_clause.h"
#include "cross_module_fn.h"

typedef enum HypertableOption
{
	HypertableEnabled,

	/* create_hypetable function arguments
	 * in the same order */
	HypertableTimeColumn,
	HypertablePartitioningColumn,
	HypertableNumberPartitions,
	HypertableAssociatedSchemaName,
	HypertableAssociatedTablePrefix,
	HypertableChunkTimeInterval,
	HypertableCreateDefaultIndexes,
	HypertableIfNotExists,
	HypertablePartitioningFunc,
	HypertableMigrateData,
	HypertableChunkTargetSize,
	HypertableChunkSizingFunc,
	HypertableTimePartitioningFunc,
	HypertableReplicationFactor,
	HypertableDataNodes,
	HypertableDistributed,

	/* compression options */
	HypertableCompressEnabled,
	HypertableCompressSegmentBy,
	HypertableCompressOrderBy,
	HypertableCompressChunkTimeInterval,

	/* set_chunk_interval */
	HypertableDimension

} HypertableOption;

#define HypertableOptionsMax (HypertableDistributed + 1)

/*
	relation                REGCLASS,
	time_column_name        NAME,
	partitioning_column     NAME = NULL,
	number_partitions       INTEGER = NULL,
	associated_schema_name  NAME = NULL,
	associated_table_prefix NAME = NULL,
	chunk_time_interval     ANYELEMENT = NULL::bigint,
	create_default_indexes  BOOLEAN = TRUE,
	if_not_exists           BOOLEAN = FALSE,
	partitioning_func       REGPROC = NULL,
	migrate_data            BOOLEAN = FALSE,
	chunk_target_size       TEXT = NULL,
	chunk_sizing_func       REGPROC = '_timescaledb_internal.calculate_chunk_interval'::regproc,
	time_partitioning_func  REGPROC = NULL,
	replication_factor      INTEGER = NULL,
	data_nodes              NAME[] = NULL,
	distributed             BOOLEAN = NULL
*/

static const WithClauseDefinition hypertable_with_clause_def[] = {
		[HypertableEnabled] = {
			.arg_name = "hypertable",
			.type_id = BOOLOID,
			.default_val = BoolGetDatum(true),
		},
		[HypertableTimeColumn] = {
			.arg_name = "time_column",
			.type_id = NAMEOID,
		},
		[HypertablePartitioningColumn] = {
			.arg_name = "partitioning_column",
			.type_id = NAMEOID,
		},
		[HypertableNumberPartitions] = {
			.arg_name = "number_partitions",
			.type_id = INT4OID,
		},
		[HypertableAssociatedSchemaName] = {
			.arg_name = "associated_schema_name",
			.type_id = NAMEOID,
		},
		[HypertableAssociatedTablePrefix] = {
			.arg_name = "associated_table_prefix",
			.type_id = NAMEOID,
		},
		[HypertableChunkTimeInterval] = {
			.arg_name = "chunk_time_interval",
			.type_id = TEXTOID,
		},
		[HypertableCreateDefaultIndexes] = {
			.arg_name = "create_default_indexes",
			.type_id = BOOLOID,
		},
		[HypertableIfNotExists] = {
			.arg_name = "if_not_exists",
			.type_id = BOOLOID,
			.default_val = BoolGetDatum(true),
		},
		[HypertablePartitioningFunc] = {
			.arg_name = "partitioning_func",
			.type_id = REGPROCOID,
		},
		[HypertableMigrateData] = {
			.arg_name = "migrate_data",
			.type_id = BOOLOID,
		},
		[HypertableChunkTargetSize] = {
			.arg_name = "chunk_target_size",
			.type_id = TEXTOID,
		},
		[HypertableChunkSizingFunc] = {
			.arg_name = "chunk_sizing_func",
			.type_id = REGPROCOID,
		},
		[HypertableTimePartitioningFunc] = {
			.arg_name = "time_partitioning_func",
			.type_id = REGPROCOID,
		},
		[HypertableReplicationFactor] = {
			.arg_name = "replication_factor",
			.type_id = INT4OID,
		},
		[HypertableDataNodes] = {
			.arg_name = "data_nodes",
			.type_id = NAMEARRAYOID,
		},
		[HypertableDistributed] = {
			.arg_name = "distributed",
			.type_id = BOOLOID,
		},
		[HypertableCompressEnabled] = {
			.arg_name = "compress",
			.type_id = BOOLOID,
			.default_val = BoolGetDatum(false),
		},
		[HypertableCompressSegmentBy] = {
			 .arg_name = "compress_segmentby",
			 .type_id = TEXTOID,
		},
		[HypertableCompressOrderBy] = {
			 .arg_name = "compress_orderby",
			 .type_id = TEXTOID,
		},
		[HypertableCompressChunkTimeInterval] = {
			 .arg_name = "compress_chunk_time_interval",
			 .type_id = INTERVALOID,
		},
		[HypertableDimension] = {
			 .arg_name = "dimension",
			 .type_id = TEXTOID,
		}
};

static Oid
get_chunk_sizing_func_oid(void)
{
	Oid argtype[] = { INT4OID, INT8OID, INT8OID };
	return ts_get_function_oid("calculate_chunk_interval", "_timescaledb_internal", 3, argtype);
}

static Oid
get_create_hypertable_func_oid(void)
{
	Oid argtype[] = { REGCLASSOID,	 NAMEOID,	 NAMEOID, INT4OID,		NAMEOID, NAMEOID,
					  ANYELEMENTOID, BOOLOID,	 BOOLOID, REGPROCOID,	BOOLOID, TEXTOID,
					  REGPROCOID,	 REGPROCOID, INT4OID, NAMEARRAYOID, BOOLOID };
	return ts_get_function_oid("create_hypertable", ts_extension_schema_name(), 17, argtype);
}

static Oid
get_set_chunk_interval_func_oid(void)
{
	Oid argtype[] = { REGCLASSOID, ANYELEMENTOID, NAMEOID };
	return ts_get_function_oid("set_chunk_time_interval", ts_extension_schema_name(), 3, argtype);
}

void
ts_hypertable_create_using_set_clause(Oid relid, const List *defelems)
{
	WithClauseResult *args;
	WithClauseResult *args_compress;
	Cache *hcache;
	Hypertable *ht;

	args = ts_with_clauses_parse(defelems,
								 hypertable_with_clause_def,
								 TS_ARRAY_LEN(hypertable_with_clause_def));

	if (args[HypertableTimeColumn].is_default)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("time column cannot be NULL")));

	/* set default chunk sizing function, if it was not provided */
	if (args[HypertableChunkSizingFunc].is_default)
	{
		args[HypertableChunkSizingFunc].is_default = false;
		args[HypertableChunkSizingFunc].parsed = get_chunk_sizing_func_oid();
	}

	/* convert chunk_time_interval either to interval time or integer */
	Datum interval = 0;
	Oid interval_type = 0;
	if (!args[HypertableChunkTimeInterval].is_default)
	{
		char *arg = TextDatumGetCString(args[HypertableChunkTimeInterval].parsed);
		char *arg_end = NULL;
		int64 interval_int;

		errno = 0;
		interval_int = strtoll(arg, &arg_end, 10);
		if (errno == ERANGE || *arg_end != '\0')
		{
			/* integer conversion was not success, assuming it is Interval type */
			interval = DirectFunctionCall3(interval_in, CStringGetDatum(arg), InvalidOid, -1);
			interval_type = INTERVALOID;
		}
		else
		{
			interval = Int64GetDatum(interval_int);
			interval_type = INT8OID;
		}
	}

	/* find create_hypertable function */
	Oid create_hypertable_oid = get_create_hypertable_func_oid();

	/* prepare function call context */
	FmgrInfo flinfo;
	fmgr_info(create_hypertable_oid, &flinfo);

	LOCAL_FCINFO(fcinfo, HypertableOptionsMax);
	InitFunctionCallInfoData(*fcinfo, &flinfo, HypertableOptionsMax, InvalidOid, NULL, NULL);

	/* set arguments to the function call */
	FC_SET_ARG(fcinfo, 0, ObjectIdGetDatum(relid));
	int i = 1;
	for (; i < HypertableOptionsMax; i++)
	{
		if (args[i].is_default)
			FC_SET_NULL(fcinfo, i);
		else
			FC_SET_ARG(fcinfo, i, args[i].parsed);
	}

	/* call internval function to create hypertable */
	ts_hypertable_create_internal(fcinfo, interval, interval_type, false);

	/* handle compression options */
	if (args[HypertableCompressEnabled].is_default)
	{
		if (!args[HypertableCompressChunkTimeInterval].is_default ||
			!args[HypertableCompressOrderBy].is_default ||
			!args[HypertableCompressSegmentBy].is_default)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("the option timescaledb.compress must be set to true to enable "
							"compression")));
		return;
	}

	/* copy options and make them compatible with
	 * CompressHypertableOption format */
	args_compress = palloc0(sizeof(WithClauseResult) * CompressOptionMax);
	memcpy(args_compress,
		   &args[HypertableCompressEnabled],
		   sizeof(WithClauseResult) * CompressOptionMax);

	/* compress hypertable */
	hcache = ts_hypertable_cache_pin();
	ht = ts_hypertable_cache_get_entry(hcache, relid, 0);
	ts_cm_functions->process_compress_table(ht, args_compress);
	ts_cache_release(hcache);
}

void
ts_hypertable_alter_using_set_clause(Hypertable *ht, const List *defelems)
{
	WithClauseResult *args;
	WithClauseResult *args_compress;

	args = ts_with_clauses_parse(defelems,
								 hypertable_with_clause_def,
								 TS_ARRAY_LEN(hypertable_with_clause_def));

	/* set chunk_time_interval either to interval time or integer */
	if (!args[HypertableChunkTimeInterval].is_default)
	{
		char *arg = TextDatumGetCString(args[HypertableChunkTimeInterval].parsed);
		char *arg_end = NULL;
		Oid interval_type = 0;
		Datum interval = 0;
		int64 interval_int;

		errno = 0;
		interval_int = strtoll(arg, &arg_end, 10);
		if (errno == ERANGE || *arg_end != '\0')
		{
			/* integer conversion was not success, assuming it is Interval type */
			interval = DirectFunctionCall3(interval_in, CStringGetDatum(arg), InvalidOid, -1);
			interval_type = INTERVALOID;
		}
		else
		{
			interval = Int64GetDatum(interval_int);
			interval_type = INT8OID;
		}

		/* find set_chunk_interval function */
		Oid set_chunk_interval_oid = get_set_chunk_interval_func_oid();

		/* prepare function call context */
		FmgrInfo flinfo;
		fmgr_info(set_chunk_interval_oid, &flinfo);

		LOCAL_FCINFO(fcinfo, 3);
		InitFunctionCallInfoData(*fcinfo, &flinfo, 3, InvalidOid, NULL, NULL);

		/* set arguments to the function call */

		/* hypertable */
		FC_SET_ARG(fcinfo, 0, ObjectIdGetDatum(ht->main_table_relid));

		/* dimenision */
		if (args[HypertableDimension].is_default)
			FC_SET_NULL(fcinfo, 2);
		else
			FC_SET_ARG(fcinfo, 2, args[HypertableDimension].parsed);

		ts_dimension_set_interval_internal(fcinfo, interval, interval_type);
	}

	/* handle compression options */
	if (args[HypertableCompressEnabled].is_default)
	{
		if (!args[HypertableCompressChunkTimeInterval].is_default ||
			!args[HypertableCompressOrderBy].is_default ||
			!args[HypertableCompressSegmentBy].is_default)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("the option timescaledb.compress must be set to true to enable "
							"compression")));
		return;
	}

	/* copy options and make them compatible with
	 * CompressHypertableOption format */
	args_compress = palloc0(sizeof(WithClauseResult) * CompressOptionMax);
	memcpy(args_compress,
		   &args[HypertableCompressEnabled],
		   sizeof(WithClauseResult) * CompressOptionMax);

	/* compress hypertable */
	ts_cm_functions->process_compress_table(ht, args_compress);
}
