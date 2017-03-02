/* -*- Mode: C; tab-width: 4; indent-tabs-mode: t; c-basic-offset: 4 -*- */
#include "pgmurmur3.h"

#include "utils/builtins.h"

/* adapted from https://github.com/markokr/pghashlib/  */ 

PG_FUNCTION_INFO_V1(pg_murmur3_hash_string);

Datum
pg_murmur3_hash_string(PG_FUNCTION_ARGS)
{
	struct varlena *data;
	uint64_t io[MAX_IO_VALUES];

	memset(io, 0, sizeof(io));

	/* request aligned data on weird architectures */
#ifdef HLIB_UNALIGNED_READ_OK
	data = PG_GETARG_VARLENA_PP(0);
#else
	data = PG_GETARG_VARLENA_P(0);
#endif

	io[0] = PG_GETARG_INT32(1);

	hlib_murmur3(VARDATA_ANY(data), VARSIZE_ANY_EXHDR(data), io);
	PG_FREE_IF_COPY(data, 0);

	PG_RETURN_INT32(io[0]);
}

/* _iobeamdb_catalog.get_partition_for_key(key TEXT, mod_factor INT) RETURNS SMALLINT */
PG_FUNCTION_INFO_V1(get_partition_for_key);

Datum
get_partition_for_key(PG_FUNCTION_ARGS)
{
// SELECT ((_iobeamdb_internal.murmur3_hash_string(key, 1 :: INT4) & x'7fffffff' :: INTEGER) % mod_factor) :: SMALLINT INTO ret;
    struct varlena *data;
	int32 mod;
    Datum hash_d;
	int32 hash_i;
	int16 res;
	/* request aligned data on weird architectures */
#ifdef HLIB_UNALIGNED_READ_OK
	data = PG_GETARG_VARLENA_PP(0);
#else
	data = PG_GETARG_VARLENA_P(0);
#endif
	mod = PG_GETARG_INT32(1);

	hash_d = DirectFunctionCall2(pg_murmur3_hash_string, PointerGetDatum(data), Int32GetDatum(1));
	hash_i = DatumGetInt32(hash_d);

	res = (int16) ((hash_i & 0x7fffffff) % mod);

	PG_RETURN_INT16(res);
}
