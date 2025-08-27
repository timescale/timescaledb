/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <fmgr.h>
#include <port/pg_bswap.h>
#include <utils/timestamp.h>
#include <utils/uuid.h>

#include "compat/compat.h"
#include "uuid.h"

/*
 * Generates a v4 UUID. Based on function pg_random_uuid() in the pgcrypto contrib module.
 *
 * Note that clib on Mac has a uuid_generate() function, so we call this ts_uuid_create().
 */
pg_uuid_t *
ts_uuid_create(void)
{
	/*
	 * PG9.6 doesn't expose the internals of pg_uuid_t, so we just treat it as
	 * a byte array
	 */
	unsigned char *gen_uuid = palloc0(UUID_LEN);
	bool rand_success = false;

	rand_success = pg_backend_random((char *) gen_uuid, UUID_LEN);

	/*
	 * If pg_backend_random() cannot find sources of randomness, then we use
	 * the current timestamp as a "random source".
	 * Timestamps are 8 bytes, so we copy this into bytes 9-16 of the UUID.
	 * If we see all 0s in bytes 0-8 (other than version + * variant), we know
	 * that there is something wrong with the RNG on this instance.
	 */
	if (!rand_success)
	{
		TimestampTz ts = GetCurrentTimestamp();

		memcpy(&gen_uuid[8], &ts, sizeof(TimestampTz));
	}

	gen_uuid[6] = (gen_uuid[6] & 0x0f) | 0x40; /* "version" field */
	gen_uuid[8] = (gen_uuid[8] & 0x3f) | 0x80; /* "variant" field */

	return (pg_uuid_t *) gen_uuid;
}

TS_FUNCTION_INFO_V1(ts_uuid_generate);

Datum
ts_uuid_generate(PG_FUNCTION_ARGS)
{
	PG_RETURN_UUID_P(ts_uuid_create());
}

/*
 * Create a UUIDv7 from a unix timestamp in microseconds.
 *
 * Optionally produce a boundary UUID with all otherwise random bits set to
 * zero that can be used in range queries. The version can also be set to zero
 * in order to produce partition ranges that excludes the UUID version.
 */
pg_uuid_t *
ts_create_uuid_v7_from_unixtime_us(int64 unixtime_us, bool boundary, bool set_version)
{
	pg_uuid_t *uuid;
	uint64_t timestamp_be = pg_hton64((unixtime_us / 1000) << 16);

	if (boundary)
	{
		uuid = (pg_uuid_t *) palloc0(UUID_LEN);
	}
	else
	{
		uuid = (pg_uuid_t *) palloc(UUID_LEN);
		pg_backend_random(&((char *) uuid)[8], UUID_LEN - 8);
	}

	/* Fill the first 48 bits with the timestamp */
	memcpy(uuid->data, &timestamp_be, 6);

	/* The microseconds part of the timestamp, scaled to 12 bits, same as in PG18 */
	uint32 ts_micros = (unixtime_us % 1000) * (1 << 12) / 1000;

	/*
	 * Sub milliseconds timestamps are optional. We store the microseconds part in the
	 * rand_a field as described in the UUID v7 specification. Following the PG18 logic
	 * here.
	 */
	uuid->data[6] = (unsigned char) (ts_micros >> 8);
	uuid->data[7] = (unsigned char) ts_micros;

	if (set_version)
	{
		/* Set version 7 (0111) in bits 6-7 of byte 6, keep random bits 0-5 */
		uuid->data[6] = (uuid->data[6] & 0x0F) | 0x70;

		/* Set variant (10) in bits 4-5 of byte 8, keep random bits 0-3 and 6-7 */
		uuid->data[8] = (uuid->data[8] & 0x3F) | 0x80;
	}

	return uuid;
}

pg_uuid_t *
ts_create_uuid_v7_from_timestamptz(TimestampTz ts, bool boundary)
{
	int64 epoch_diff_us = ((int64) (POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * USECS_PER_DAY);
	int64 unixtime_us = ts + epoch_diff_us;

	return ts_create_uuid_v7_from_unixtime_us(unixtime_us, boundary, true);
}

TS_FUNCTION_INFO_V1(ts_uuid_generate_v7);

Datum
ts_uuid_generate_v7(PG_FUNCTION_ARGS)
{
	PG_RETURN_UUID_P(ts_create_uuid_v7_from_timestamptz(GetCurrentTimestamp(), false));
}

TS_FUNCTION_INFO_V1(ts_uuid_v7_from_timestamptz);

Datum
ts_uuid_v7_from_timestamptz(PG_FUNCTION_ARGS)
{
	TimestampTz timestamp = PG_GETARG_TIMESTAMPTZ(0);

	PG_RETURN_UUID_P(ts_create_uuid_v7_from_timestamptz(timestamp, false));
}

TS_FUNCTION_INFO_V1(ts_uuid_v7_from_timestamptz_boundary);

Datum
ts_uuid_v7_from_timestamptz_boundary(PG_FUNCTION_ARGS)
{
	TimestampTz timestamp = PG_GETARG_TIMESTAMPTZ(0);

	PG_RETURN_UUID_P(ts_create_uuid_v7_from_timestamptz(timestamp, true));
}

#define UUID_VARIANT(uuid) ((uuid)->data[8] & 0xc0)
#define IS_RFC9562_VARIANT(uuid) (UUID_VARIANT(uuid) == 0x80)
#define UUID_VERSION(uuid) (((uuid)->data[6] & 0xf0) >> 4)

/*
 * Extract the millisecond Unix epoch timestamp from the UUIDv7, with optional
 * extra sub-millisecond fraction in microseconds.
 */
bool
ts_uuid_v7_extract_unixtime(const pg_uuid_t *uuid, uint64 *unixtime_ms, uint16 *extra_us)
{
	bool is_uuidv7 = false;

	/* Check that the variant field corresponds to RFC9562 */
	if (IS_RFC9562_VARIANT(uuid))
	{
		/* Get the version from the UUID */
		is_uuidv7 = (UUID_VERSION(uuid) == 7);
	}

	/* Big endian timestamp in milliseconds from Unix Epoch */
	uint64 timestamp_be = 0;
	memcpy(&timestamp_be, uuid->data, 6);

	/* The timestamp is now milliseconds from Unix Epoch (1970-01-01)*/
	*unixtime_ms = (pg_ntoh64(timestamp_be)) >> 16;

	if (extra_us)
	{
		/* Optionally, get the sub ms part as microseconds, reversing the scaling */
		*extra_us = (((uuid->data[6] & 0xF) << 8) | uuid->data[7]) * 1000 / (1 << 12);
	}

	return is_uuidv7;
}

TS_FUNCTION_INFO_V1(ts_timestamptz_from_uuid_v7);

Datum
ts_timestamptz_from_uuid_v7(PG_FUNCTION_ARGS)
{
	pg_uuid_t *uuid = PG_GETARG_UUID_P(0);
	uint64 unixtime_millis = 0;

	if (!ts_uuid_v7_extract_unixtime(uuid, &unixtime_millis, NULL))
		PG_RETURN_NULL();

	/* Milliseconds timestamp from PG Epoch (2000-01-01) */
	uint64 timestamp_millis =
		(unixtime_millis -
		 ((uint64) (POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * SECS_PER_DAY) * 1000ULL);

	/* Convert to microseconds */
	TimestampTz ts = timestamp_millis * 1000;

	PG_RETURN_TIMESTAMPTZ(ts);
}

TS_FUNCTION_INFO_V1(ts_timestamptz_from_uuid_v7_with_microseconds);

Datum
ts_timestamptz_from_uuid_v7_with_microseconds(PG_FUNCTION_ARGS)
{
	pg_uuid_t *uuid = PG_GETARG_UUID_P(0);
	uint64 unixtime_millis = 0;
	uint16 extra_micros = 0;

	if (!ts_uuid_v7_extract_unixtime(uuid, &unixtime_millis, &extra_micros))
		PG_RETURN_NULL();

	/* Milliseconds timestamp from PG Epoch (2000-01-01) */
	uint64 timestamp_millis =
		(unixtime_millis -
		 ((uint64) (POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * SECS_PER_DAY) * 1000ULL);

	/* Add up the whole to get microseconds */
	TimestampTz ts = timestamp_millis * 1000 + extra_micros;

	PG_RETURN_TIMESTAMPTZ(ts);
}

TS_FUNCTION_INFO_V1(ts_uuid_version);

Datum
ts_uuid_version(PG_FUNCTION_ARGS)
{
	pg_uuid_t *uuid = PG_GETARG_UUID_P(0);
	int version;

	/* Check that the variant field corresponds to RFC9562 */
	if (!IS_RFC9562_VARIANT(uuid))
		PG_RETURN_NULL();

	version = UUID_VERSION(uuid); /* Get the version from the UUID */

	PG_RETURN_INT32(version);
}
