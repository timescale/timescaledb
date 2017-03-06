#ifndef _IOBEAMDB_CHUNK_H_
#define _IOBEAMDB_CHUNK_H_

#include <postgres.h>
#include <fmgr.h>

extern Datum local_chunk_size(PG_FUNCTION_ARGS);

#endif   /* _IOBEAMDB_CHUNK_H_ */
