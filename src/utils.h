#ifndef TIMESCALEDB_UTILS_H
#define TIMESCALEDB_UTILS_H

#include <postgres.h>
#include <fmgr.h>
#include <nodes/primnodes.h>
#include <catalog/pg_proc.h>

/*
 * Convert a column value into the internal time representation.
 */
extern int64 time_value_to_internal(Datum time_val, Oid type);

extern FmgrInfo *create_fmgr(char *schema, char *function_name, int num_args);
extern RangeVar *makeRangeVarFromRelid(Oid relid);
extern int	int_cmp(const void *a, const void *b);

#define DATUM_GET(values, attno) \
	values[attno-1]

#endif							/* TIMESCALEDB_UTILS_H */
