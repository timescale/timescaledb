#ifndef TIMESCALEDB_CHUNK_ADAPTIVE_H
#define TIMESCALEDB_CHUNK_ADAPTIVE_H

#include <postgres.h>

typedef struct ChunkSizingInfo
{
	/* Set manually */
	Oid			func;
	text	   *target_size;

	/* Validated info */
	NameData	func_name;
	NameData	func_schema;
	int64		target_size_bytes;
} ChunkSizingInfo;

void		chunk_adaptive_validate_sizing_info(ChunkSizingInfo *info);

#endif							/* TIMESCALEDB_CHUNK_ADAPTIVE_H */
