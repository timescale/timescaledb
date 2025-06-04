/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

/*
 * This module (header file plus implementation file) needs to be linked with
 * both the plugin and with the extension, so it should not have any
 * unresolved linking dependencies on the extension.
 */

#pragma once

#include <postgres.h>

#include <access/attnum.h>

/*
 * Invalidation entry.
 *
 * We use the relid of the hypertable rather than the hypertable id to keep
 * processing fast and also avoid linking dependencies on the timescaledb
 * extension.
 *
 * The translation from hypertable relid to hypertable id will be done on the
 * receiving end before writing the records to the materialization log.
 *
 * The lowest and highest modified values are still in microseconds since the
 * epoch, but the libraries for this do not require any dynamic linking so we
 * can just build the plugin with these files directly.
 *
 * In order to deal with versions correctly, the entry consists of a union of
 * structures representing the different versions of the record. There is a
 * version number added as well so that we can deal with necessary changes.
 *
 * If you want to add a new version, step the version by adding one to
 * INVALIDATION_MESSAGE_CURRENT_VERSION add a new structure with the name
 * "ver<version>". Then you need to update the decode function as well to
 * handle the new version.
 */
typedef struct InvalidationMessageHeader
{
	uint16 version;
	uint16 padding;

} InvalidationMessageHeader;

typedef struct InvalidationMessageBodyV1
{
	InvalidationMessageHeader hdr;
	Oid hypertable_relid;
	int64 lowest_modified;
	int64 highest_modified;
} InvalidationMessageBodyV1;

typedef union InvalidationMessage
{
	InvalidationMessageHeader hdr;
	InvalidationMessageBodyV1 ver1;
} InvalidationMessage;

#define INVALIDATION_MESSAGE_CURRENT_VERSION 1

extern PGDLLEXPORT Datum ts_invalidation_read_record(PG_FUNCTION_ARGS);

/* Parameters to these two functions are such that the updated entry is
 * last. This is to avoid mixing the functions up and let the compiler
 * complain. */
extern PGDLLEXPORT void ts_invalidation_record_decode(StringInfo record, InvalidationMessage *msg);
extern PGDLLEXPORT void ts_invalidation_record_encode(InvalidationMessage *msg, StringInfo record);
