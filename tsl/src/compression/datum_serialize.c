
/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <utils/sortsupport.h>
#include <utils/typcache.h>
#include <utils/builtins.h>
#include <utils/datum.h>
#include <access/tupmacs.h>
#include <access/htup_details.h>
#include <utils/lsyscache.h>
#include <utils/syscache.h>
#include <catalog/namespace.h>
#include <catalog/pg_type.h>
#include <libpq/pqformat.h>

#include "datum_serialize.h"
#include "compat.h"

typedef struct DatumSerializer
{
	Oid type_oid;
	bool type_by_val;
	int16 type_len;
	char type_align;
	char type_storage;
	Oid type_send;
	Oid type_out;

	/* lazy load */
	bool send_info_set;
	FmgrInfo send_flinfo;
	bool use_binary_send;
} DatumSerializer;

DatumSerializer *
create_datum_serializer(Oid type_oid)
{
	DatumSerializer *res = palloc(sizeof(*res));
	/* we use the syscache and not the type cache here b/c we need the
	 * send/recv in/out functions that aren't in type cache */
	Form_pg_type type;
	HeapTuple tup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(type_oid));
	if (!HeapTupleIsValid(tup))
		elog(ERROR, "cache lookup failed for type %u", type_oid);
	type = (Form_pg_type) GETSTRUCT(tup);

	*res = (DatumSerializer){
		.type_oid = type_oid,
		.type_by_val = type->typbyval,
		.type_len = type->typlen,
		.type_align = type->typalign,
		.type_storage = type->typstorage,
		.type_send = type->typsend,
		.type_out = type->typoutput,
		.use_binary_send = OidIsValid(type->typsend),
	};

	ReleaseSysCache(tup);
	return res;
}

bool
datum_serializer_value_may_be_toasted(DatumSerializer *serializer)
{
	return serializer->type_len == -1;
}

static inline void
load_send_fn(DatumSerializer *ser)
{
	if (ser->send_info_set)
		return;

	ser->send_info_set = true;

	if (ser->use_binary_send)
		fmgr_info(ser->type_send, &ser->send_flinfo);
	else
		fmgr_info(ser->type_out, &ser->send_flinfo);
}

#define TYPE_IS_PACKABLE(typlen, typstorage) ((typlen) == -1 && (typstorage) != 'p')

/* Inspired by datum_compute_size in rangetypes.c */
Size
datum_get_bytes_size(DatumSerializer *serializer, Size start_offset, Datum val)
{
	Size data_length = start_offset;

	if (serializer->type_len == -1)
	{
		/* varlena */
		Pointer ptr = DatumGetPointer(val);

		if (VARATT_IS_EXTERNAL(ptr))
		{
			/*
			 * Throw error, because we should never get a toasted datum.
			 * Caller should have detoasted it.
			 */
			elog(ERROR, "datum should be detoasted before passed to datum_get_bytes_size");
		}
	}

	if (TYPE_IS_PACKABLE(serializer->type_len, serializer->type_storage) &&
		VARATT_CAN_MAKE_SHORT(DatumGetPointer(val)))
	{
		/*
		 * we're anticipating converting to a short varlena header, so adjust
		 * length and don't count any alignment (the case where the Datum is already
		 * in short format is handled by att_align_datum)
		 */
		data_length += VARATT_CONVERTED_SHORT_SIZE(DatumGetPointer(val));
	}
	else
	{
		data_length =
			att_align_datum(data_length, serializer->type_align, serializer->type_len, val);
		data_length = att_addlength_datum(data_length, serializer->type_len, val);
	}

	return data_length;
}

BinaryStringEncoding
datum_serializer_binary_string_encoding(DatumSerializer *serializer)
{
	return (serializer->use_binary_send ? BINARY_ENCODING : TEXT_ENCODING);
}

static void
check_allowed_data_len(Size data_length, Size max_size)
{
	if (max_size < data_length)
		elog(ERROR, "trying to serialize more data than was allocated");
}

static inline char *
align_and_zero(char *ptr, char type_align, Size *max_size)
{
	char *new_pos = (char *) att_align_nominal(ptr, type_align);
	if (new_pos != ptr)
	{
		Size padding = new_pos - ptr;
		check_allowed_data_len(padding, *max_size);
		memset(ptr, 0, padding);
		*max_size = *max_size - padding;
	}
	return new_pos;
}

/* Inspired by datum_write in rangetypes.c. This reduces the max_size by the data length before
 * exiting */
char *
datum_to_bytes_and_advance(DatumSerializer *serializer, char *ptr, Size *max_size, Datum datum)
{
	Size data_length;

	if (serializer->type_by_val)
	{
		/* pass-by-value */
		ptr = align_and_zero(ptr, serializer->type_align, max_size);
		data_length = serializer->type_len;
		check_allowed_data_len(data_length, *max_size);
		store_att_byval(ptr, datum, data_length);
	}
	else if (serializer->type_len == -1)
	{
		/* varlena */
		Pointer val = DatumGetPointer(datum);

		if (VARATT_IS_EXTERNAL(val))
		{
			/*
			 * Throw error, because we should never get a toast datum.
			 *  Caller should have detoasted it.
			 */
			elog(ERROR, "datum should be detoasted before passed to datum_to_bytes_and_advance");
			data_length = 0; /* keep compiler quiet */
		}
		else if (VARATT_IS_SHORT(val))
		{
			/* no alignment for short varlenas */
			data_length = VARSIZE_SHORT(val);
			check_allowed_data_len(data_length, *max_size);
			memcpy(ptr, val, data_length);
		}
		else if (TYPE_IS_PACKABLE(serializer->type_len, serializer->type_storage) &&
				 VARATT_CAN_MAKE_SHORT(val))
		{
			/* convert to short varlena -- no alignment */
			data_length = VARATT_CONVERTED_SHORT_SIZE(val);
			check_allowed_data_len(data_length, *max_size);
			SET_VARSIZE_SHORT(ptr, data_length);
			memcpy(ptr + 1, VARDATA(val), data_length - 1);
		}
		else
		{
			/* full 4-byte header varlena */
			ptr = align_and_zero(ptr, serializer->type_align, max_size);
			data_length = VARSIZE(val);
			check_allowed_data_len(data_length, *max_size);
			memcpy(ptr, val, data_length);
		}
	}
	else if (serializer->type_len == -2)
	{
		/* cstring ... never needs alignment */
		Assert(serializer->type_align == 'c');
		data_length = strlen(DatumGetCString(datum)) + 1;
		check_allowed_data_len(data_length, *max_size);
		memcpy(ptr, DatumGetPointer(datum), data_length);
	}
	else
	{
		/* fixed-length pass-by-reference */
		ptr = align_and_zero(ptr, serializer->type_align, max_size);
		Assert(serializer->type_len > 0);
		data_length = serializer->type_len;
		check_allowed_data_len(data_length, *max_size);
		memcpy(ptr, DatumGetPointer(datum), data_length);
	}

	ptr += data_length;
	*max_size = *max_size - data_length;

	return ptr;
}

typedef struct DatumDeserializer
{
	bool type_by_val;
	int16 type_len;
	char type_align;
	char type_storage;

	Oid type_recv;

	Oid type_in;
	Oid type_io_param;
	int32 type_mod;
	/* lazy load */
	bool recv_info_set;
	FmgrInfo recv_flinfo;
	bool use_binary_recv;
} DatumDeserializer;

DatumDeserializer *
create_datum_deserializer(Oid type_oid)
{
	DatumDeserializer *res = palloc(sizeof(*res));
	/* we use the syscache and not the type cache here b/c we need the
	 * send/recv in/out functions that aren't in type cache */
	Form_pg_type type;
	HeapTuple tup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(type_oid));
	if (!HeapTupleIsValid(tup))
		elog(ERROR, "cache lookup failed for type %u", type_oid);
	type = (Form_pg_type) GETSTRUCT(tup);

	*res = (DatumDeserializer){
		.type_by_val = type->typbyval,
		.type_len = type->typlen,
		.type_align = type->typalign,
		.type_storage = type->typstorage,
		.type_recv = type->typreceive,
		.type_in = type->typinput,
		.type_io_param = getTypeIOParam(tup),
		.type_mod = type->typtypmod,
	};

	ReleaseSysCache(tup);
	return res;
}

static inline void
load_recv_fn(DatumDeserializer *des, bool use_binary)
{
	if (des->recv_info_set && des->use_binary_recv == use_binary)
		return;

	des->recv_info_set = true;
	des->use_binary_recv = use_binary;

	if (des->use_binary_recv)
		fmgr_info(des->type_recv, &des->recv_flinfo);
	else
		fmgr_info(des->type_in, &des->recv_flinfo);
}

/* Loosely based on `range_deserialize` in rangetypes.c */
Datum
bytes_to_datum_and_advance(DatumDeserializer *deserializer, const char **ptr)
{
	Datum res;

	/* att_align_pointer can handle the case where an unaligned short-varlen follows any other
	 * varlen by detecting padding. padding bytes _must always_ be set to 0, while the first byte of
	 * a varlen header is _never_ 0. This means that if the next byte is non-zero, it must be the
	 * start of a short-varlen, otherwise we need to align the pointer.
	 */

	*ptr =
		(Pointer) att_align_pointer(*ptr, deserializer->type_align, deserializer->type_len, *ptr);
	res = fetch_att(*ptr, deserializer->type_by_val, deserializer->type_len);
	*ptr = att_addlength_pointer(*ptr, deserializer->type_len, *ptr);
	return res;
}

void
type_append_to_binary_string(Oid type_oid, StringInfo buffer)
{
	Form_pg_type type_tuple;
	HeapTuple tup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(type_oid));
	char *namespace_name;
	if (!HeapTupleIsValid(tup))
		elog(ERROR, "cache lookup failed for type %u", type_oid);

	type_tuple = (Form_pg_type) GETSTRUCT(tup);

	namespace_name = get_namespace_name(type_tuple->typnamespace);

	pq_sendstring(buffer, namespace_name);
	pq_sendstring(buffer, NameStr(type_tuple->typname));

	ReleaseSysCache(tup);
}

Oid
binary_string_get_type(StringInfo buffer)
{
	const char *element_type_namespace = pq_getmsgstring(buffer);
	const char *element_type_name = pq_getmsgstring(buffer);
	Oid namespace_oid;
	Oid type_oid;

	namespace_oid = LookupExplicitNamespace(element_type_namespace, false);

	type_oid = GetSysCacheOid2Compat(TYPENAMENSP,
									 Anum_pg_type_oid,
									 PointerGetDatum(element_type_name),
									 ObjectIdGetDatum(namespace_oid));
	if (!OidIsValid(type_oid))
		elog(ERROR, "could not find type %s.%s", element_type_namespace, element_type_name);

	return type_oid;
}

void
datum_append_to_binary_string(DatumSerializer *serializer, BinaryStringEncoding encoding,
							  StringInfo buffer, Datum datum)
{
	load_send_fn(serializer);

	if (encoding == MESSAGE_SPECIFIES_ENCODING)
		pq_sendbyte(buffer, serializer->use_binary_send);
	else if (encoding != datum_serializer_binary_string_encoding(serializer))
		elog(ERROR, "incorrect encoding chosen in datum_append_to_binary_string");

	if (serializer->use_binary_send)
	{
		bytea *output = SendFunctionCall(&serializer->send_flinfo, datum);
		pq_sendint32(buffer, VARSIZE_ANY_EXHDR(output));
		pq_sendbytes(buffer, VARDATA(output), VARSIZE_ANY_EXHDR(output));
	}
	else
	{
		char *output = OutputFunctionCall(&serializer->send_flinfo, datum);
		pq_sendstring(buffer, output);
	}
}

Datum
binary_string_to_datum(DatumDeserializer *deserializer, BinaryStringEncoding encoding,
					   StringInfo buffer)
{
	Datum res;
	bool use_binary_recv = false;

	switch (encoding)
	{
		case BINARY_ENCODING:
			use_binary_recv = true;
			break;
		case TEXT_ENCODING:
			use_binary_recv = false;
			break;
		case MESSAGE_SPECIFIES_ENCODING:
			use_binary_recv = pq_getmsgbyte(buffer) != 0;
			break;
	}

	load_recv_fn(deserializer, use_binary_recv);

	if (use_binary_recv)
	{
		uint32 data_size = pq_getmsgint32(buffer);
		const char *bytes = pq_getmsgbytes(buffer, data_size);
		StringInfoData d = {
			.data = (char *) bytes,
			.len = data_size,
			.maxlen = data_size,
		};
		res = ReceiveFunctionCall(&deserializer->recv_flinfo,
								  &d,
								  deserializer->type_io_param,
								  deserializer->type_mod);
	}
	else
	{
		const char *string = pq_getmsgstring(buffer);
		res = InputFunctionCall(&deserializer->recv_flinfo,
								(char *) string,
								deserializer->type_io_param,
								deserializer->type_mod);
	}
	return res;
}
