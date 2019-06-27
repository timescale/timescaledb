/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <postgres.h>
#include <catalog/pg_type.h>
#include <utils/builtins.h>
#include <utils/lsyscache.h>
#include <utils/syscache.h>
#include <miscadmin.h>
#include <access/xact.h>
#include <cache.h>

#include "errors.h"
#include "hypertable.h"
#include "dimension.h"
#include "license.h"
#include "utils.h"
#include "hypertable_cache.h"

Datum
hypertable_set_integer_now_func(PG_FUNCTION_ARGS)
{
	Oid table_relid = PG_GETARG_OID(0);
	Oid now_func_oid = PG_GETARG_OID(1);
	bool replace_if_exists = PG_GETARG_BOOL(2);
	Hypertable *hypertable;
	Cache *hcache;
	Dimension *open_dim;
	Oid open_dim_type;

	license_enforce_enterprise_enabled();
	license_print_expiration_warning_if_needed();
	ts_hypertable_permissions_check(table_relid, GetUserId());

	hcache = ts_hypertable_cache_pin();
	hypertable = ts_hypertable_cache_get_entry(hcache, table_relid);
	/* First verify that the hypertable corresponds to a valid table */
	if (hypertable == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_TS_HYPERTABLE_NOT_EXIST),
				 errmsg("could not set integer_now function because \"%s\" is not a hypertable",
						get_rel_name(table_relid))));

	/* validate that the open dimension uses numeric type */
	open_dim = hyperspace_get_open_dimension(hypertable->space, 0);

	if (!replace_if_exists)
		if (*NameStr(open_dim->fd.integer_now_func_schema) != '\0' ||
			*NameStr(open_dim->fd.integer_now_func) != '\0')
			ereport(ERROR,
					(errcode(ERRCODE_DUPLICATE_OBJECT),
					 errmsg("integer_now_func is already set for hypertable \"%s\"",
							get_rel_name(table_relid))));

	open_dim_type = ts_dimension_get_partition_type(open_dim);
	if (!IS_INTEGER_TYPE(open_dim_type))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("integer_now_func can only be set for hypertables "
						"that have integer time dimensions")));

	ts_interval_now_func_validate(now_func_oid, open_dim_type);

	dimension_update(table_relid,
					 &open_dim->fd.column_name,
					 DIMENSION_TYPE_OPEN,
					 NULL,
					 NULL,
					 NULL,
					 &now_func_oid);

	ts_cache_release(hcache);
	PG_RETURN_NULL();
}
Datum
hypertable_valid_ts_interval(PG_FUNCTION_ARGS)
{
	/* this function does all the necessary validation and if successfull,
	returns the interval which is not necessary here */
	ts_interval_from_tuple(PG_GETARG_DATUM(0));
	PG_RETURN_BOOL(true);
}
