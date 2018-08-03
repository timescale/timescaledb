#ifndef TIMESCALEDB_PARTITIONING_H
#define TIMESCALEDB_PARTITIONING_H

#define KEYSPACE_PT_NO_PARTITIONING -1

#include <postgres.h>
#include <access/attnum.h>
#include <access/htup_details.h>
#include <utils/typcache.h>
#include <fmgr.h>

#include "catalog.h"

#define OPEN_START_TIME -1
#define OPEN_END_TIME PG_INT64_MAX

#define DEFAULT_PARTITIONING_FUNC_SCHEMA INTERNAL_SCHEMA_NAME
#define DEFAULT_PARTITIONING_FUNC_NAME "get_partition_hash"

typedef struct PartitioningFunc
{
	char		schema[NAMEDATALEN];
	char		name[NAMEDATALEN];

	/*
	 * Function manager info to call the partitioning function on the
	 * partitioning column's text representation.
	 */
	FmgrInfo	func_fmgr;
} PartitioningFunc;


typedef struct PartitioningInfo
{
	char		column[NAMEDATALEN];
	AttrNumber	column_attnum;
	PartitioningFunc partfunc;
} PartitioningInfo;


extern Oid	partitioning_func_get_default(void);
extern bool partitioning_func_is_default(const char *schema, const char *funcname);
extern bool partitioning_func_is_valid(regproc funcoid);

extern PartitioningInfo *partitioning_info_create(const char *schema,
						 const char *partfunc,
						 const char *partcol,
						 Oid relid);
extern List *partitioning_func_qualified_name(PartitioningFunc *pf);
extern int32 partitioning_func_apply(PartitioningInfo *pinfo, Datum value);
extern int32 partitioning_func_apply_tuple(PartitioningInfo *pinfo, HeapTuple tuple, TupleDesc desc);

#endif							/* TIMESCALEDB_PARTITIONING_H */
