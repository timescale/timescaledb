/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_ERROR_UTILS_H
#define TIMESCALEDB_ERROR_UTILS_H

#define GETARG_NOTNULL_OID(var, arg, name)                                                         \
	{                                                                                              \
		var = PG_ARGISNULL(arg) ? InvalidOid : PG_GETARG_OID(arg);                                 \
		if (!OidIsValid(var))                                                                      \
			ereport(ERROR,                                                                         \
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),                                     \
					 errmsg("%s cannot be NULL", name)));                                          \
	}

#define GETARG_NOTNULL_NULLABLE(var, arg, name, type)                                              \
	{                                                                                              \
		if (PG_ARGISNULL(arg))                                                                     \
			ereport(ERROR,                                                                         \
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),                                     \
					 errmsg("%s cannot be NULL", name)));                                          \
		var = PG_GETARG_##type(arg);                                                               \
	}

#endif /* TIMESCALEDB_ERROR_UTILS_H */
