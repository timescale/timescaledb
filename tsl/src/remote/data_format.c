/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <catalog/pg_type.h>
#include <utils/syscache.h>
#include <utils/lsyscache.h>
#include <access/htup_details.h>
#include <utils/builtins.h>
#include <access/sysattr.h>
#include <funcapi.h>

#include "guc.h"
#include "data_format.h"

static Oid
get_type_in_out_func(Oid type, bool *is_binary, bool force_text, Oid *type_io_param, bool out)
{
	HeapTuple type_tuple;
	Form_pg_type pg_type;
	Oid func;

	type_tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(type));
	if (!HeapTupleIsValid(type_tuple))
		elog(ERROR, "cache lookup failed for type %u", type);
	pg_type = (Form_pg_type) GETSTRUCT(type_tuple);

	if (!pg_type->typisdefined)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("type %s is only a shell", format_type_be(type))));

	if (out)
		if (OidIsValid(pg_type->typsend) && !force_text)
		{
			func = pg_type->typsend;
			*is_binary = true;
		}
		else
		{
			func = pg_type->typoutput;
			*is_binary = false;
		}
	else
	{
		if (OidIsValid(pg_type->typreceive) && !force_text)
		{
			func = pg_type->typreceive;
			*is_binary = true;
		}
		else
		{
			func = pg_type->typinput;
			*is_binary = false;
		}
		*type_io_param = getTypeIOParam(type_tuple);
	}

	ReleaseSysCache(type_tuple);
	if (!OidIsValid(func))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_FUNCTION),
				 errmsg("no binary or text in/out function available for type %s",
						format_type_be(type))));
	return func;
}

/**
 * Returns either type binary or text output function (text output is secondary)
 **/
Oid
data_format_get_type_output_func(Oid type, bool *is_binary, bool force_text)
{
	return get_type_in_out_func(type, is_binary, force_text, NULL, true);
}

Oid
data_format_get_type_input_func(Oid type, bool *is_binary, bool force_text, Oid *type_io_param)
{
	return get_type_in_out_func(type, is_binary, force_text, type_io_param, false);
}

AttConvInMetadata *
data_format_create_att_conv_in_metadata(TupleDesc tupdesc)
{
	AttConvInMetadata *att_conv_metadata;
	int i = 0;
	bool prev = true, isbinary = true;

	att_conv_metadata = palloc(sizeof(AttConvInMetadata));

	BlessTupleDesc(tupdesc);

	att_conv_metadata->conv_funcs = palloc(tupdesc->natts * sizeof(FmgrInfo));
	att_conv_metadata->ioparams = palloc(tupdesc->natts * sizeof(Oid));
	att_conv_metadata->typmods = palloc(tupdesc->natts * sizeof(int32));

	while (i < tupdesc->natts)
	{
		Oid funcoid;

		if (!TupleDescAttr(tupdesc, i)->attisdropped)
		{
			funcoid =
				data_format_get_type_input_func(TupleDescAttr(tupdesc, i)->atttypid,
												&isbinary,
												!ts_guc_enable_connection_binary_data || !isbinary,
												&att_conv_metadata->ioparams[i]);
			if (prev == !isbinary)
			{
				i = 0; /* in/out functions has to be eiher all binary or all text (PostgreSQL
						  limitation). Lets restart function discovery process */
				prev = false;
				continue;
			}

			fmgr_info(funcoid, &att_conv_metadata->conv_funcs[i]);
			att_conv_metadata->typmods[i] = TupleDescAttr(tupdesc, i)->atttypmod;
		}
		i++;
	}

	att_conv_metadata->binary = isbinary;
	return att_conv_metadata;
}
