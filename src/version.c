/*
 * Copyright (c) 2016-2018  Timescale, Inc. All Rights Reserved.
 *
 * This file is licensed under the Apache License,
 * see LICENSE-APACHE at the top level directory.
 */
#include <postgres.h>
#include <string.h>
#include <access/htup_details.h>
#include <utils/builtins.h>
#include <funcapi.h>
#include <fmgr.h>

#include "fmgr.h"
#include "compat.h"
#include "gitcommit.h"
#include "version.h"
#include "config.h"

#define STR_EXPAND(x) #x
#define STR(x) STR_EXPAND(x)

static const char *git_commit = STR(EXT_GIT_COMMIT);

TS_FUNCTION_INFO_V1(ts_get_git_commit);

Datum
ts_get_git_commit(PG_FUNCTION_ARGS)
{
	size_t		var_size = VARHDRSZ + strlen(git_commit);
	text	   *version_text = (text *) palloc(var_size);

	SET_VARSIZE(version_text, var_size);

	memcpy((void *) VARDATA(version_text),
		   (void *) git_commit,
		   var_size - VARHDRSZ);

	PG_RETURN_TEXT_P(version_text);
}

#ifdef WIN32

#include <Windows.h>

bool
ts_version_get_os_info(VersionOSInfo *info)
{
	DWORD		bufsize;
	void	   *buffer;
	VS_FIXEDFILEINFO *vinfo = NULL;
	UINT		vinfo_len = 0;

	memset(info, 0, sizeof(VersionOSInfo));

	bufsize = GetFileVersionInfoSizeA(TEXT("kernel32.dll"), NULL);

	if (bufsize == 0)
		return false;

	buffer = palloc(bufsize);

	if (!GetFileVersionInfoA(TEXT("kernel32.dll"), 0, bufsize, buffer))
		goto error;

	if (!VerQueryValueA(buffer, TEXT("\\"), &vinfo, &vinfo_len))
		goto error;

	snprintf(info->sysname, VERSION_INFO_LEN - 1, "Windows");
	snprintf(info->version, VERSION_INFO_LEN - 1, "%u", HIWORD(vinfo->dwProductVersionMS));
	snprintf(info->release, VERSION_INFO_LEN - 1, "%u", LOWORD(vinfo->dwProductVersionMS));

	pfree(buffer);

	return true;
error:
	pfree(buffer);

	return false;
}

#elif defined(UNIX)

#include <sys/utsname.h>

bool
ts_version_get_os_info(VersionOSInfo *info)
{
	/* Get the OS name  */
	struct utsname os_info;

	uname(&os_info);

	memset(info, 0, sizeof(VersionOSInfo));
	strncpy(info->sysname, os_info.sysname, VERSION_INFO_LEN - 1);
	strncpy(info->version, os_info.version, VERSION_INFO_LEN - 1);
	strncpy(info->release, os_info.release, VERSION_INFO_LEN - 1);

	return true;
}
#else
bool
ts_version_get_os_info(VersionOSInfo *info)
{
	memset(info, 0, sizeof(VersionOSInfo));
	return false;
}
#endif							/* WIN32 */

TS_FUNCTION_INFO_V1(ts_get_os_info);

Datum
ts_get_os_info(PG_FUNCTION_ARGS)
{
	TupleDesc	tupdesc;
	Datum		values[3];
	bool		nulls[3] = {false};
	HeapTuple	tuple;
	VersionOSInfo info;

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("function returning record called in context "
						"that cannot accept type record")));

	if (ts_version_get_os_info(&info))
	{
		values[0] = CStringGetTextDatum(info.sysname);
		values[1] = CStringGetTextDatum(info.version);
		values[2] = CStringGetTextDatum(info.release);
	}
	else
		memset(nulls, true, sizeof(nulls));

	tuple = heap_form_tuple(tupdesc, values, nulls);

	return HeapTupleGetDatum(tuple);
}
