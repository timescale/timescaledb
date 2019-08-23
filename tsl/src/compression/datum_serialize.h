/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_TSL_COMPRESSION_DATUM_SERIALIZE_H
#define TIMESCALEDB_TSL_COMPRESSION_DATUM_SERIALIZE_H

#include <postgres.h>
#include <lib/stringinfo.h>

/* SERIALIZATION */
typedef struct DatumSerializer DatumSerializer;
DatumSerializer *create_datum_serializer(Oid type);

/* serialize to bytes in memory. */
Size datum_get_bytes_size(DatumSerializer *serializer, Size start_offset, Datum val);
char *datum_to_bytes_and_advance(DatumSerializer *serializer, char *start, Size *max_size,
								 Datum val);

/* serialize to a binary string (for send functions) */
void type_append_to_binary_string(Oid type_oid, StringInfo data);
void datum_append_to_binary_string(DatumSerializer *serializer, StringInfo data, Datum datum);

/* DESERIALIZATION */
typedef struct DatumDeserializer DatumDeserializer;
DatumDeserializer *create_datum_deserializer(Oid type);

/* deserialization from bytes in memory */
Datum bytes_to_datum_and_advance(DatumDeserializer *deserializer, char **bytes);

/* deserialization from binary strings (for recv functions) */
Datum binary_string_to_datum(DatumDeserializer *deserializer, StringInfo data);
Oid binary_string_get_type(StringInfo data);

#endif
