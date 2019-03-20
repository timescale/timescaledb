/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_TSL_REMOTE_DATA_FORMAT_H
#define TIMESCALEDB_TSL_REMOTE_DATA_FORMAT_H

#include <postgres.h>
#include <fmgr.h>
#include <access/tupdesc.h>

#define FORMAT_TEXT 0
#define FORMAT_BINARY 1

/* Metadata to convert PG result into tuples */
typedef struct AttConvInMetadata
{
	FmgrInfo *conv_funcs; /* in functions for converting */
	Oid *ioparams;
	int32 *typmods;
	bool binary; /* if we use function with binary input */
} AttConvInMetadata;

extern AttConvInMetadata *data_format_create_att_conv_in_metadata(TupleDesc tupdesc);

extern Oid data_format_get_type_output_func(Oid type, bool *is_binary, bool force_text);
extern Oid data_format_get_type_input_func(Oid type, bool *is_binary, bool force_text,
										   Oid *type_io_param);

#endif
