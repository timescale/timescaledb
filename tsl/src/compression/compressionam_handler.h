/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_TSL_COMPRESSIONAM_HANDLER_H
#define TIMESCALEDB_TSL_COMPRESSIONAM_HANDLER_H

#include <postgres.h>
#include <access/tableam.h>
#include <access/xact.h>
#include <fmgr.h>
#include <nodes/pathnodes.h>

#include "hypertable.h"

/* Scan key flag (skey.h) to indicate that a table scan should only return
 * tuples from the non-compressed relation. Bits 16-31 are reserved for
 * individual access methods, so use bit 16. */
#define SK_NO_COMPRESSED 0x8000

extern const TableAmRoutine *compressionam_routine(void);
extern void compressionam_handler_start_conversion(Oid relid, bool to_other_am);
extern void compressionam_alter_access_method_begin(Oid relid, bool to_other_am);
extern void compressionam_alter_access_method_finish(Oid relid, bool to_other_am);
extern Datum compressionam_handler(PG_FUNCTION_ARGS);
extern void compressionam_xact_event(XactEvent event, void *arg);

typedef struct ColumnCompressionSettings
{
	NameData attname;
	AttrNumber attnum;
	AttrNumber cattnum; /* Attribute number in the compressed relation */
	Oid typid;
	bool is_orderby;
	bool is_segmentby;
	bool orderby_desc;
	bool nulls_first;
} ColumnCompressionSettings;

/*
 * Compression info cache struct for access method.
 *
 * This struct is cached in a relcache entry's rd_amcache pointer and needs to
 * have a structure that can be palloc'ed in a single memory chunk.
 */
typedef struct CompressionAmInfo
{
	int32 hypertable_id;		  /* TimescaleDB ID of parent hypertable */
	int32 relation_id;			  /* TimescaleDB ID of relation (chunk ID) */
	int32 compressed_relation_id; /* TimescaleDB ID of compressed relation (chunk ID) */
	Oid compressed_relid;		  /* Relid of compressed relation */
	int num_columns;
	int num_segmentby;
	int num_orderby;
	int num_keys;
	/* Per-column information follows. */
	ColumnCompressionSettings columns[FLEXIBLE_ARRAY_MEMBER];
} CompressionAmInfo;

extern CompressionAmInfo *RelationGetCompressionAmInfo(Relation rel);

#endif /* TIMESCALEDB_TSL_COMPRESSIONAM_HANDLER_H */
