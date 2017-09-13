#ifndef TIMESCALEDB_TRIGGER_H
#define TIMESCALEDB_TRIGGER_H

#include <postgres.h>
#include <catalog/pg_trigger.h>
#include "hypertable.h"
#include "chunk.h"

extern Form_pg_trigger trigger_by_oid(Oid trigger_oid, bool missing_ok);
extern bool trigger_is_chunk_trigger(const Form_pg_trigger trigger);
extern char *trigger_name(const Form_pg_trigger trigger);

void		trigger_create_on_chunk(Oid trigger_oid, char *chunk_schema_name, char *chunk_table_name);
void		trigger_create_on_all_chunks(Hypertable *ht, Chunk *chunk);

#endif   /* TIMESCALEDB_TRIGGER_H */
