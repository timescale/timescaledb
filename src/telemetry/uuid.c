#include <postgres.h>
#include <access/heapam.h>
#include <access/htup_details.h>
#include <access/htup.h>
#include <utils/builtins.h>
#include <utils/fmgroids.h>
#include <utils/snapmgr.h>
#include <utils/backend_random.h>
#include <utils/timestamp.h>

#include "compat.h"
#include "catalog.h"
#include "installation_metadata.h"
#include "uuid.h"

TS_FUNCTION_INFO_V1(generate_uuid_external);

/* Generates a v4 UUID. Based on function pg_random_uuid() in the pgcyrpto contrib module. */
static pg_uuid_t *
generate_uuid()
{
	int64 ts;
	pg_uuid_t	*gen_uuid = (pg_uuid_t *) palloc(sizeof(pg_uuid_t));
	bool rand_success = pg_backend_random((char *) gen_uuid->data, UUID_LEN);

	/*
	 * If pg_backend_random cannot find sources of randomness, then we use the current
     * timestamp as a "random source". Timestamps are 8 bytes, so we copy this into bytes 9-16 of the UUID.
	 * If we see all 0s in bytes 0-8 (other than version + variant), we know that there is
     * something wrong with the RNG on this node.
	 */
	if (!rand_success) {
		ts = GetCurrentTimestamp();
		memcpy((void *)gen_uuid->data[9], &ts, 8);
	}

	gen_uuid->data[6] = (gen_uuid->data[6] & 0x0f) | 0x40;	/* "version" field */
	gen_uuid->data[8] = (gen_uuid->data[8] & 0x3f) | 0x80;	/* "variant" field */
	return gen_uuid;
}

Datum
generate_uuid_external(PG_FUNCTION_ARGS) {
	return CStringGetTextDatum(DirectFunctionCall1(uuid_out, UUIDPGetDatum(generate_uuid())));
}


pg_uuid_t *
get_uuid()
{
	const char	   *uuid = installation_metadata_get_value(INSTALLATION_METADATA_UUID_KEY_NAME);

	if (!uuid)
		uuid = installation_metadata_insert(INSTALLATION_METADATA_UUID_KEY_NAME, DatumGetCString(DirectFunctionCall1(uuid_out, UUIDPGetDatum(generate_uuid()))));

	return DatumGetUUIDP(DirectFunctionCall1(uuid_in, CStringGetDatum(uuid)));
}

pg_uuid_t *
get_exported_uuid()
{
	const char	   *exported_uuid = installation_metadata_get_value(INSTALLATION_METADATA_EXPORTED_UUID_KEY_NAME);
	if (!exported_uuid)
		exported_uuid = installation_metadata_insert(INSTALLATION_METADATA_EXPORTED_UUID_KEY_NAME, DatumGetCString(DirectFunctionCall1(uuid_out, UUIDPGetDatum(generate_uuid()))));
	return DatumGetUUIDP(DirectFunctionCall1(uuid_in, CStringGetDatum(exported_uuid)));
}

const char *
get_install_timestamp()
{
	const char	   *timestamp = installation_metadata_get_value(INSTALLATION_METADATA_TIMESTAMP_KEY_NAME);

	if (timestamp == NULL)
	{
		timestamp = DatumGetCString(DirectFunctionCall1(timestamptz_out,
														TimestampTzGetDatum(GetCurrentTimestamp())));
		timestamp = installation_metadata_insert(INSTALLATION_METADATA_TIMESTAMP_KEY_NAME, timestamp);
	}
	return timestamp;
}
