/*
 * NOTE: adaptive chunking is still in BETA
 */
#ifndef TIMESCALEDB_CHUNK_ADAPTIVE_H
#define TIMESCALEDB_CHUNK_ADAPTIVE_H

#include <postgres.h>

typedef struct ChunkSizingInfo
{
	Oid table_relid;
	/* Set manually */
	Oid func;
	text *target_size;
	const char *colname;  /* The column of the dimension we are adapting
						   * on */
	bool check_for_index; /* Set if we should check for an index on
						   * the dimension we are adapting on */

	/* Validated info */
	NameData func_name;
	NameData func_schema;
	int64 target_size_bytes;
} ChunkSizingInfo;

extern void ts_chunk_adaptive_sizing_info_validate(ChunkSizingInfo *info);
extern void ts_chunk_sizing_func_validate(regproc func, ChunkSizingInfo *info);
extern TSDLLEXPORT ChunkSizingInfo *ts_chunk_sizing_info_get_default_disabled(Oid table_relid);

#endif /* TIMESCALEDB_CHUNK_ADAPTIVE_H */
