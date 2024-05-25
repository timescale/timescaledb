/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#pragma once

#include <postgres.h>
#include <fmgr.h>
#include <utils/builtins.h>

#include "compat/compat.h"
#include "net/conn.h"
#include "net/http.h"
#include "utils.h"
#include "version.h"

#define REQ_LICENSE_INFO "license"
#define REQ_LICENSE_EDITION "edition"

#define TELEMETRY_SCHEME "https"
#define TELEMETRY_HOST "telemetry.timescale.com"
#define TELEMETRY_PATH "/v1/metrics"

#define MAX_VERSION_STR_LEN 128

typedef struct BgwJobTypeCount
{
	int32 policy_cagg;
	int32 policy_cagg_fixed;
	int32 policy_compression;
	int32 policy_compression_fixed;
	int32 policy_reorder;
	int32 policy_reorder_fixed;
	int32 policy_retention;
	int32 policy_retention_fixed;
	int32 policy_telemetry;
	int32 user_defined_action;
	int32 user_defined_action_fixed;
} BgwJobTypeCount;

typedef struct VersionResult
{
	const char *versionstr;
	const char *errhint;
} VersionResult;

extern HttpRequest *ts_build_version_request(const char *host, const char *path);
extern Connection *ts_telemetry_connect(const char *host, const char *service);
extern bool ts_validate_server_version(const char *json, VersionResult *result);
extern void ts_check_version_response(const char *json);

/*
 *	This function is intended as the main function for a BGW.
 *  Its job is to send metrics and fetch the most up-to-date version of
 *  Timescale via HTTPS.
 */
extern bool ts_telemetry_main(const char *host, const char *path, const char *service);
extern TSDLLEXPORT bool ts_telemetry_main_wrapper(void);
extern TSDLLEXPORT Datum ts_telemetry_get_report_jsonb(PG_FUNCTION_ARGS);
