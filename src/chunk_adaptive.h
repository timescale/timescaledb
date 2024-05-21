/*
 * NOTE: adaptive chunking is still in BETA
 */
#pragma once

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
extern bool ts_chunk_get_minmax(Oid relid, Oid atttype, AttrNumber attnum, const char *call_context,
								Datum minmax[2]);
extern TSDLLEXPORT ChunkSizingInfo *ts_chunk_sizing_info_get_default_disabled(Oid table_relid);

extern TSDLLEXPORT int64 ts_chunk_calculate_initial_chunk_target_size(void);
