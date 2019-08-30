/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

#include <postgres.h>

#include "cross_module_fn.h"
#include "compat.h"
#include "base64_compat.h"
#include "license_guc.h"

TS_FUNCTION_INFO_V1(ts_segment_meta_min_max_send);
TS_FUNCTION_INFO_V1(ts_segment_meta_min_max_recv);
TS_FUNCTION_INFO_V1(ts_segment_meta_min_max_out);
TS_FUNCTION_INFO_V1(ts_segment_meta_min_max_in);

TS_FUNCTION_INFO_V1(ts_segment_meta_get_min);
TS_FUNCTION_INFO_V1(ts_segment_meta_get_max);
TS_FUNCTION_INFO_V1(ts_segment_meta_has_null);

Datum
ts_segment_meta_min_max_send(PG_FUNCTION_ARGS)
{
	Datum meta = PG_GETARG_DATUM(0);
	PG_RETURN_DATUM(PointerGetDatum(ts_cm_functions->segment_meta_min_max_send(meta)));
}

Datum
ts_segment_meta_min_max_recv(PG_FUNCTION_ARGS)
{
	StringInfo buf = (StringInfo) PG_GETARG_POINTER(0);
	PG_RETURN_DATUM(ts_cm_functions->segment_meta_min_max_recv(buf));
}

Datum
ts_segment_meta_min_max_out(PG_FUNCTION_ARGS)
{
	Datum meta = PG_GETARG_DATUM(0);
	bytea *bytes = ts_cm_functions->segment_meta_min_max_send(meta);

	int raw_len = VARSIZE_ANY_EXHDR(bytes);
	const char *raw_data = VARDATA(bytes);
	int encoded_len = pg_b64_enc_len(raw_len);
	char *encoded = palloc(encoded_len + 1);
	encoded_len = pg_b64_encode(raw_data, raw_len, encoded);
	encoded[encoded_len] = '\0';

	PG_RETURN_CSTRING(encoded);
}

Datum
ts_segment_meta_min_max_in(PG_FUNCTION_ARGS)
{
	const char *input = PG_GETARG_CSTRING(0);
	size_t input_len = strlen(input);
	int decoded_len;
	char *decoded;
	StringInfoData data;

	/* Load TSL explicitly in case this is called during parsing */
	ts_license_enable_module_loading();

	if (input_len > PG_INT32_MAX)
		elog(ERROR, "input too long");

	decoded_len = pg_b64_dec_len(input_len);
	decoded = palloc(decoded_len + 1);
	decoded_len = pg_b64_decode(input, input_len, decoded);
	decoded[decoded_len] = '\0';
	data = (StringInfoData){
		.data = decoded,
		.len = decoded_len,
		.maxlen = decoded_len,
	};

	PG_RETURN_DATUM(ts_cm_functions->segment_meta_min_max_recv(&data));
}

Datum
ts_segment_meta_get_min(PG_FUNCTION_ARGS)
{
	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();
	PG_RETURN_DATUM(PointerGetDatum(
		ts_cm_functions->segment_meta_get_min(PG_GETARG_DATUM(0),
											  get_fn_expr_argtype(fcinfo->flinfo, 1))));
}

Datum
ts_segment_meta_get_max(PG_FUNCTION_ARGS)
{
	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();
	PG_RETURN_DATUM(PointerGetDatum(
		ts_cm_functions->segment_meta_get_max(PG_GETARG_DATUM(0),
											  get_fn_expr_argtype(fcinfo->flinfo, 1))));
}

Datum
ts_segment_meta_has_null(PG_FUNCTION_ARGS)
{
	if (PG_ARGISNULL(0))
		PG_RETURN_BOOL(true);
	PG_RETURN_BOOL(ts_cm_functions->segment_meta_has_null(PG_GETARG_DATUM(0)));
}
