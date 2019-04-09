/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <catalog/pg_type.h>
#include <access/htup_details.h>
#include <access/tuptoaster.h>
#include <nodes/relation.h>
#include <utils/lsyscache.h>
#include <utils/syscache.h>
#include <utils/builtins.h>
#include <utils/memutils.h>

#include "guc.h"
#include "fdw/utils.h"
#include "data_format.h"
#include "stmt_params.h"

#define MAX_PG_STMT_PARAMS                                                                         \
	USHRT_MAX /* PostgreSQL limitation of max parameters in the statement                          \
			   */

typedef struct StmtParams
{
	FmgrInfo *conv_funcs;
	const char **values;
	int *formats;
	int *lengths;
	int num_params;
	int num_tuples;
	int converted_tuples;
	bool ctid;
	List *target_attr_nums;
	MemoryContext mctx;	/* where we allocate param values */
	MemoryContext tmp_ctx; /* used for converting values */
} StmtParams;

/*
 * ctid should be set to true if we're going to send it
 * num_tuples is used for batching
 * mctx memory context where we'll allocate StmtParams with all the values
 */
StmtParams *
stmt_params_create(List *target_attr_nums, bool ctid, TupleDesc tuple_desc, int num_tuples)
{
	StmtParams *params;
	ListCell *lc;
	Oid typefnoid;
	bool isbinary;
	int idx = 0;
	int tup_cnt;
	MemoryContext old;
	MemoryContext new;
	MemoryContext tmp_ctx;

	new = AllocSetContextCreate(CurrentMemoryContext,
								"stmt params mem context",
								ALLOCSET_DEFAULT_SIZES);
	old = MemoryContextSwitchTo(new);
	tmp_ctx = AllocSetContextCreate(new, "stmt params conversion", ALLOCSET_DEFAULT_SIZES);

	params = palloc(sizeof(StmtParams));

	Assert(num_tuples > 0);
	params->num_params = ctid ? list_length(target_attr_nums) + 1 : list_length(target_attr_nums);
	if (params->num_params * num_tuples > MAX_PG_STMT_PARAMS)
		elog(ERROR, "too many parameters in prepared statement. Max is %d", MAX_PG_STMT_PARAMS);
	params->conv_funcs = palloc(sizeof(FmgrInfo) * params->num_params);
	params->formats = palloc(sizeof(int) * params->num_params * num_tuples);
	params->lengths = palloc(sizeof(int) * params->num_params * num_tuples);
	params->values = palloc(sizeof(char *) * params->num_params * num_tuples);
	params->ctid = ctid;
	params->target_attr_nums = target_attr_nums;
	params->num_tuples = num_tuples;
	params->converted_tuples = 0;
	params->mctx = new;
	params->tmp_ctx = tmp_ctx;

	if (params->ctid)
	{
		typefnoid = data_format_get_type_output_func(TIDOID,
													 &isbinary,
													 !ts_guc_enable_connection_binary_data);
		fmgr_info(typefnoid, &params->conv_funcs[idx]);
		params->formats[idx] = isbinary ? FORMAT_BINARY : FORMAT_TEXT;
		idx++;
	}

	foreach (lc, target_attr_nums)
	{
		int attr_num = lfirst_int(lc);
		Form_pg_attribute attr = TupleDescAttr(tuple_desc, AttrNumberGetAttrOffset(attr_num));
		Assert(!attr->attisdropped);

		typefnoid = data_format_get_type_output_func(attr->atttypid,
													 &isbinary,
													 !ts_guc_enable_connection_binary_data);
		params->formats[idx] = isbinary ? FORMAT_BINARY : FORMAT_TEXT;

		fmgr_info(typefnoid, &params->conv_funcs[idx++]);
	}

	Assert(params->num_params == idx);

	for (tup_cnt = 1; tup_cnt < params->num_tuples; tup_cnt++)
		memcpy(params->formats + tup_cnt * params->num_params,
			   params->formats,
			   sizeof(int) * params->num_params);

	MemoryContextSwitchTo(old);
	return params;
}

static bool
all_values_in_binary_format(int *formats, int num_params)
{
	int i;

	for (i = 0; i < num_params; i++)
		if (formats[i] != FORMAT_BINARY)
			return false;
	return true;
}

/*
 * tupleid is ctid. If ctid was set to true tupleid has to be provided
 */
void
stmt_params_convert_values(StmtParams *params, TupleTableSlot *slot, ItemPointer tupleid)
{
	MemoryContext old;
	int idx;
	ListCell *lc;
	int nest_level;
	bool all_binary;
	int param_idx = 0;

	Assert(params->num_params > 0);
	idx = params->converted_tuples * params->num_params;

	Assert(params->converted_tuples < params->num_tuples);

	old = MemoryContextSwitchTo(params->tmp_ctx);

	if (tupleid != NULL)
	{
		bytea *output_bytes;
		Assert(params->ctid);
		if (params->formats[idx] == FORMAT_BINARY)
		{
			output_bytes =
				SendFunctionCall(&params->conv_funcs[param_idx], PointerGetDatum(tupleid));
			params->values[idx] = VARDATA(output_bytes);
			params->lengths[idx] = (int) VARSIZE(output_bytes) - VARHDRSZ;
		}
		else
			params->values[idx] =
				OutputFunctionCall(&params->conv_funcs[param_idx], PointerGetDatum(tupleid));

		idx++;
		param_idx++;
	}
	else if (params->ctid)
		elog(ERROR, "was configured to use ctid, but tupleid is NULL");

	all_binary = all_values_in_binary_format(params->formats, params->num_params);
	if (!all_binary)
		nest_level = set_transmission_modes();

	foreach (lc, params->target_attr_nums)
	{
		int attr_num = lfirst_int(lc);
		Datum value;
		bool isnull;

		value = slot_getattr(slot, attr_num, &isnull);

		if (isnull)
			params->values[idx] = NULL;
		else if (params->formats[idx] == FORMAT_TEXT)
			params->values[idx] = OutputFunctionCall(&params->conv_funcs[param_idx], value);
		else if (params->formats[idx] == FORMAT_BINARY)
		{
			bytea *output_bytes = SendFunctionCall(&params->conv_funcs[param_idx], value);
			params->values[idx] = VARDATA(output_bytes);
			params->lengths[idx] = VARSIZE(output_bytes) - VARHDRSZ;
		}
		else
			elog(ERROR, "unexpected parameter format: %d", params->formats[idx]);
		idx++;
		param_idx++;
	}

	params->converted_tuples++;

	if (!all_binary)
		reset_transmission_modes(nest_level);

	MemoryContextSwitchTo(old);
}

void
stmt_params_reset(StmtParams *params)
{
	MemoryContextReset(params->tmp_ctx);
	params->converted_tuples = 0;
}

/*
 * Free params memory context and child context we've used for converting values to binary or text
 */
void
stmt_params_free(StmtParams *params)
{
	MemoryContextDelete(params->mctx);
}

const int *
stmt_params_formats(StmtParams *stmt_params)
{
	return stmt_params->formats;
}

const int *
stmt_params_lengths(StmtParams *stmt_params)
{
	return stmt_params->lengths;
}

const char *const *
stmt_params_values(StmtParams *stmt_params)
{
	return stmt_params->values;
}

const int
stmt_params_num_params(StmtParams *stmt_params)
{
	return stmt_params->num_params;
}

const int
stmt_params_total_values(StmtParams *stmt_params)
{
	return stmt_params->converted_tuples * stmt_params->num_params;
}

const int
stmt_params_converted_tuples(StmtParams *stmt_params)
{
	return stmt_params->converted_tuples;
}
