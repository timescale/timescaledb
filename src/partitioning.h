/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_PARTITIONING_H
#define TIMESCALEDB_PARTITIONING_H

#define KEYSPACE_PT_NO_PARTITIONING -1

#include <postgres.h>
#include <access/attnum.h>
#include <access/htup_details.h>
#include <utils/typcache.h>
#include <fmgr.h>

#include "catalog.h"
#include "dimension.h"

#define OPEN_START_TIME -1
#define OPEN_END_TIME PG_INT64_MAX

#define DEFAULT_PARTITIONING_FUNC_SCHEMA INTERNAL_SCHEMA_NAME
#define DEFAULT_PARTITIONING_FUNC_NAME "get_partition_hash"

typedef struct PartitioningFunc
{
	char schema[NAMEDATALEN];
	char name[NAMEDATALEN];
	Oid rettype;

	/*
	 * Function manager info to call the partitioning function on the
	 * partitioning column's text representation.
	 */
	FmgrInfo func_fmgr;
} PartitioningFunc;

typedef struct PartitioningInfo
{
	char column[NAMEDATALEN];
	AttrNumber column_attnum;
	DimensionType dimtype;
	PartitioningFunc partfunc;
} PartitioningInfo;

extern Oid ts_partitioning_func_get_closed_default(void);
extern bool ts_partitioning_func_is_valid(regproc funcoid, DimensionType dimtype, Oid argtype);

extern PartitioningInfo *ts_partitioning_info_create(const char *schema, const char *partfunc,
													 const char *partcol, DimensionType dimtype,
													 Oid relid);
extern List *ts_partitioning_func_qualified_name(PartitioningFunc *pf);
extern TSDLLEXPORT Datum ts_partitioning_func_apply(PartitioningInfo *pinfo, Oid collation,
													Datum value);

/* NOTE: assume the tuple belongs to the root table, use ts_partitioning_func_apply for chunk tuples
 */
extern TSDLLEXPORT Datum ts_partitioning_func_apply_tuple(PartitioningInfo *pinfo, HeapTuple tuple,
														  TupleDesc desc, bool *isnull);

#endif /* TIMESCALEDB_PARTITIONING_H */
