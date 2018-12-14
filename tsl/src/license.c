/*
 * Copyright (c) 2018  Timescale, Inc. All Rights Reserved.
 *
 * This file is licensed under the Timescale License,
 * see LICENSE-TIMESCALE at the top of the tsl directory.
 */

/*
 * A license is a type follwed by the license contents, see Readme.module.md
 * for more detail.
 */
#include <postgres.h>

#include <access/xact.h>
#include <utils/builtins.h>
#include <utils/elog.h>
#include <utils/json.h>
#include <utils/jsonb.h>
#include <utils/jsonapi.h>
#include <utils/datetime.h>

#include <license_guc.h>
#include <base64_compat.h>
#include <jsonb_utils.h>

#include "license.h"

#define LICENSE_MAX_ID_LEN 40
#define LICENSE_MAX_KIND_LEN 16

typedef struct LicenseInfo
{
	/*
	 * Leaving id and kind a strings for now since they're only used for
	 * telemetry. If we start introspecting on one of them we should switch it
	 * to a more structured type.
	 */
	char		id[LICENSE_MAX_ID_LEN];
	char		kind[LICENSE_MAX_KIND_LEN];
	TimestampTz start_time;
	TimestampTz end_time;
	bool		enterprise_features_enabled;
} LicenseInfo;

static bool license_deserialize_enterprise(char *license, LicenseInfo *license_out);
static bool license_info_init_from_base64(char *license_key, LicenseInfo *out);
static void license_info_init_from_jsonb(Jsonb *json_license, LicenseInfo *out);
static bool validate_license_info(const LicenseInfo *license);

static LicenseInfo current_license =
{
	.id = {0},
	.kind = {0},
	.end_time = DT_NOBEGIN,
	.enterprise_features_enabled = false,
};

static const LicenseInfo no_license = {
	.id = "",
	.kind = {""},
	.start_time = DT_NOBEGIN,
	.end_time = DT_NOBEGIN,
	.enterprise_features_enabled = false,
};

static const LicenseInfo community_license = {
	.id = "",
	.kind = {""},
	.start_time = DT_NOBEGIN,
	.end_time = DT_NOEND,
	.enterprise_features_enabled = false
};


TS_FUNCTION_INFO_V1(tsl_license_update_check);

PGDLLEXPORT Datum
tsl_license_update_check(PG_FUNCTION_ARGS)
{
	bool		license_deserialized;
	char	   *license_key = NULL;
	LicenseInfo **guc_extra = NULL;

	Assert(!PG_ARGISNULL(0));
	Assert(!PG_ARGISNULL(1));

	license_key = PG_GETARG_CSTRING(0);
	guc_extra = (LicenseInfo **) PG_GETARG_POINTER(1);
	Assert(guc_extra != NULL);

	/*
	 * According to the postgres guc documentation, string `extra`s MUST be
	 * allocated with `malloc`. (postgres attempts to `free` unneeded
	 * guc-extras with the system `free` upon transaction commit, and there's
	 * no guarantee that any MemoryContext uses the correct allocator.)
	 */
	*guc_extra = malloc(sizeof(LicenseInfo));

	license_deserialized = license_deserialize_enterprise(license_key, *guc_extra);
	PG_RETURN_BOOL(license_deserialized && validate_license_info(*guc_extra));
}

/*
 * Return if a license is valid, optionally outputting the deserialized form.
 */
static bool
license_deserialize_enterprise(char *license_key, LicenseInfo *license_out)
{
	LicenseInfo license_temp = {};
	const LicenseInfo *license_info = NULL;
	size_t		license_key_len = strlen(license_key);

	if (license_key_len < 1)
		return false;

	switch (license_key[0])
	{
		case LICENSE_TYPE_APACHE_ONLY:
			license_info = &no_license;
			break;
		case LICENSE_TYPE_COMMUNITY:
			license_info = &community_license;
			break;
		case LICENSE_TYPE_ENTERPRISE:
			if (license_key_len < 2)
				return false;

			/*
			 * Second byte of an enterprise license key is the version.
			 * Hardcoding this for now since we only have one version. the
			 * byte corresponding to '0' is reserved in case we need to extend
			 * the length of the version number.
			 */
			if (license_key[1] != '1')
				return false;
			if (license_info_init_from_base64(license_key + 2, &license_temp))
				license_info = &license_temp;
			break;
		default:
			return false;
	}
	if (NULL == license_info)
		return false;

	if (license_out != NULL)
		memmove(license_out, license_info, sizeof(*license_info));

	return true;
}

static bool
validate_license_info(const LicenseInfo *license)
{
	if (license->enterprise_features_enabled)
	{
		if (!(strcmp(license->kind, "trial") || strcmp(license->kind, "commercial")))
			return false;
	}

	if (timestamp_cmp_internal(license->end_time, license->start_time) < 0)
		return false;

	return true;
}

/*****************************************************************************
 *****************************************************************************/

static char *base64_decode(char *license_key);

static bool
license_info_init_from_base64(char *license_key, LicenseInfo *out)
{
	char	   *expanded = base64_decode(license_key);

	PG_TRY();
	{
		Datum		json_key = DirectFunctionCall1(jsonb_in, CStringGetDatum(expanded));

		license_info_init_from_jsonb((Jsonb *) DatumGetPointer(json_key), out);

	}
	PG_CATCH();
	{
#ifdef TS_DEBUG
		EmitErrorReport();
#endif
		return false;
	}
	PG_END_TRY();
	return true;
}

static char *
base64_decode(char *license_key)
{
	int			raw_len = strlen(license_key);
	int			decoded_buffer_len = pg_b64_dec_len(raw_len) + 1;
	char	   *decoded = palloc(decoded_buffer_len);
	int			decoded_len = pg_b64_decode(license_key, raw_len, decoded);

	Assert(decoded_len < decoded_buffer_len);
	decoded[decoded_len] = '\0';
	return decoded;
}

/*****************************************************************************
 *****************************************************************************/

/*
 * JSON license decoding
 */

#define ID_FIELD "id"
#define KIND_FIELD "kind"
#define START_TIME_FIELD "start_time"
#define END_TIME_FIELD "end_time"
#define FIELD_NOT_FOUND_ERRSTRING "invalid license key for TimescaleDB, could not find field \"%s\""

static char *json_get_id(Jsonb *license);
static char *json_get_kind(Jsonb *license);
static TimestampTz json_get_start_time(Jsonb *license);
static TimestampTz json_get_end_time(Jsonb *license);
static void
license_info_init_from_jsonb(Jsonb *json_license, LicenseInfo *out)
{
	StrNCpy(out->id, json_get_id(json_license), sizeof(out->id));
	StrNCpy(out->kind, json_get_kind(json_license), sizeof(out->kind));
	out->start_time = json_get_start_time(json_license);
	out->end_time = json_get_end_time(json_license);
	out->enterprise_features_enabled = true;
}

static char *
json_get_id(Jsonb *license)
{
	return ts_jsonb_get_str_field(license, cstring_to_text(ID_FIELD));
}

static char *
json_get_kind(Jsonb *license)
{
	return ts_jsonb_get_str_field(license, cstring_to_text(KIND_FIELD));
}

static TimestampTz
json_get_start_time(Jsonb *license)
{
	bool		found = false;
	TimestampTz start_time = ts_jsonb_get_time_field(license, cstring_to_text(START_TIME_FIELD), &found);

	if (!found)
		elog(ERRCODE_FEATURE_NOT_SUPPORTED, FIELD_NOT_FOUND_ERRSTRING, START_TIME_FIELD);
	return start_time;
}

static TimestampTz
json_get_end_time(Jsonb *license)
{
	bool		found = false;
	TimestampTz end_time = ts_jsonb_get_time_field(license, cstring_to_text(END_TIME_FIELD), &found);

	if (!found)
		elog(ERRCODE_FEATURE_NOT_SUPPORTED, FIELD_NOT_FOUND_ERRSTRING, END_TIME_FIELD);
	return end_time;
}

/*****************************************************************************
 *****************************************************************************
 *****************************************************************************/

static bool license_info_is_expired(const LicenseInfo *license);

/*
 * We don't want to expose the LicenseInfo struct to the Apache code, so we use
 * this intermediate function to translate from the `void *` to `LicenseInfo *`.
 * There are some cases where the guc extra may not be set, so if this function
 * receives a NULL license, it will translate that to `no_license`
 */
void
tsl_license_on_assign(const char *newval, const void *license)
{
	/*
	 * A NULL extra means that we're reverting to the top of the license guc
	 * stack.
	 */
	if (license == NULL)
	{
		license_switch_to(&no_license);
		return;
	}

	license_switch_to(license);
}

void
license_switch_to(const LicenseInfo *license)
{
	Assert(license != NULL);

	if (license_info_is_expired(license))
	{
		ereport(WARNING, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						  errmsg("Timescale License expired"),
						  errhint("Your license expired on %s. Renew your license to continue using enterprise features.", DatumGetCString(DirectFunctionCall1(timestamptz_out, license->end_time)))));
	}
	else if (!TIMESTAMP_NOT_FINITE(license->end_time))
	{
		ereport(WARNING, (errcode(ERRCODE_WARNING),
						  errmsg("your Timescale License expires on %s", DatumGetCString(DirectFunctionCall1(timestamptz_out, license->end_time)))));
	}

	current_license = *license;
}

bool
license_info_is_expired(const LicenseInfo *license)
{
	TimestampTz current_time = GetCurrentTransactionStartTimestamp();

	return timestamp_cmp_internal(license->end_time, current_time) < 0;
}

/*****************************************************************************
 *****************************************************************************
 *****************************************************************************/

/* Getters for current license */

bool
license_is_expired()
{
	return license_info_is_expired(&current_license);
}

bool
license_enterprise_enabled(void)
{
	return current_license.enterprise_features_enabled;
}

char *
license_kind_str(void)
{
	return current_license.kind;
}

char *
license_id_str(void)
{
	return current_license.id;
}

TimestampTz
license_start_time(void)
{
	return current_license.start_time;
}

TimestampTz
license_end_time(void)
{
	return current_license.end_time;
}
