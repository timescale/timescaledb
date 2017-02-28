#include <postgres.h>
#include <utils/builtins.h>
#include <utils/catcache.h>

#include "chunk_cache.h"
#include "cache.h"
#include "hypertable_cache.h"
#include "utils.h"
#include "metadata_queries.h"
#include "partitioning.h"


/*
 * Chunk Insert Plan Cache:
 *
 * Hashtable of chunk_id =>  chunk_insert_plan_htable_entry.
 *
 * This cache stores plans for the execution of the command for moving stuff
 * from the copy table to the tables associated with the chunk.
 *
 * Retrieval: each chunk has one associated plan. If the chunk's start/end time
 * changes then the old plan is freed and a new plan is regenerated
 *
 * NOTE: chunks themselves do not have a cache since they need to be locked for
 * each insert anyway...
 *
 */
typedef struct chunk_insert_plan_htable_entry
{
	int32		chunk_id;
	int64		start_time;
	int64		end_time;
	SPIPlanPtr	move_from_copyt_plan;
} chunk_insert_plan_htable_entry;

typedef struct ChunkCacheQueryCtx
{
	CacheQueryCtx cctx;
	hypertable_cache_entry *hci;
	epoch_and_partitions_set *pe_entry;
	Partition   *part;
	int32		chunk_id;
	int64		chunk_start_time;
	int64		chunk_end_time;
} ChunkCacheQueryCtx;

static void *
chunk_insert_plan_cache_get_key(CacheQueryCtx *ctx)
{
	return &((ChunkCacheQueryCtx *) ctx)->chunk_id;
}

static void *chunk_insert_plan_cache_create_entry(Cache *cache, CacheQueryCtx *ctx);
static void *chunk_insert_plan_cache_update_entry(Cache *cache, CacheQueryCtx *ctx);

static void chunk_insert_plan_cache_pre_invalidate(Cache *cache);
static char *get_copy_table_insert_sql(ChunkCacheQueryCtx *ctx);

static Cache chunk_insert_plan_cache = {
	.hctl = {
		.keysize = sizeof(int32),
		.entrysize = sizeof(chunk_insert_plan_htable_entry),
		.hcxt = NULL,
	},
	.htab = NULL,
	.name = CHUNK_CACHE_INVAL_PROXY_TABLE,
	.numelements = 16,
	.flags = HASH_ELEM | HASH_CONTEXT | HASH_BLOBS,
	.get_key = chunk_insert_plan_cache_get_key,
	.create_entry = chunk_insert_plan_cache_create_entry,
	.update_entry = chunk_insert_plan_cache_update_entry,
	.pre_invalidate_hook = chunk_insert_plan_cache_pre_invalidate,
	.post_invalidate_hook = cache_init,
};

static void
chunk_insert_plan_cache_pre_invalidate(Cache *cache)
{
	chunk_insert_plan_htable_entry *entry;
	HASH_SEQ_STATUS scan;

	hash_seq_init(&scan, cache->htab);

	while ((entry = hash_seq_search(&scan)))
	{
		SPI_freeplan(entry->move_from_copyt_plan);
	}
}

static void *
chunk_insert_plan_cache_create_entry(Cache *cache, CacheQueryCtx *ctx)
{
	ChunkCacheQueryCtx *cctx = (ChunkCacheQueryCtx *) ctx;
	chunk_insert_plan_htable_entry *pe = ctx->entry;
	char	   *insert_sql;

	insert_sql = get_copy_table_insert_sql(cctx);
	pe->chunk_id = cctx->chunk_id;
	pe->start_time = cctx->chunk_start_time;
	pe->end_time = cctx->chunk_end_time;
	pe->move_from_copyt_plan = prepare_plan(insert_sql, 0, NULL);

	return pe;
}

static void *
chunk_insert_plan_cache_update_entry(Cache *cache, CacheQueryCtx *ctx)
{
	ChunkCacheQueryCtx *cctx = (ChunkCacheQueryCtx *) ctx;
	chunk_insert_plan_htable_entry *pe = ctx->entry;
	char	   *insert_sql;

	if (pe->start_time == cctx->chunk_start_time &&
		pe->end_time == cctx->chunk_end_time)
		return pe;

	insert_sql = get_copy_table_insert_sql(cctx);
	SPI_freeplan(pe->move_from_copyt_plan);
	pe->move_from_copyt_plan = prepare_plan(insert_sql, 0, NULL);

	return pe;
}

void
invalidate_chunk_cache_callback(void)
{
	CACHE1_elog(WARNING, "DESTROY chunk_insert plan cache");
	cache_invalidate(&chunk_insert_plan_cache);
}

static chunk_insert_plan_htable_entry *
get_chunk_insert_plan_cache_entry(hypertable_cache_entry *hci, epoch_and_partitions_set *pe_entry,
								  Partition *part, int32 chunk_id, int64 chunk_start_time,
								  int64 chunk_end_time)
{
	ChunkCacheQueryCtx ctx = {
		.hci = hci,
		.pe_entry = pe_entry,
		.part = part,
		.chunk_id = chunk_id,
		.chunk_start_time = chunk_start_time,
		.chunk_end_time = chunk_end_time,
	};

	return cache_fetch(&chunk_insert_plan_cache, &ctx.cctx);
}

chunk_cache_entry *
get_chunk_cache_entry(hypertable_cache_entry *hci, epoch_and_partitions_set *pe_entry,
					  Partition *part, int64 time_pt, bool lock)
{
	chunk_insert_plan_htable_entry *move_plan;

	chunk_row  *chunk = fetch_chunk_row(NULL, part->id, time_pt, lock);
	chunk_cache_entry *entry = palloc(sizeof(chunk_cache_entry));

	entry->chunk = chunk;
	entry->id = chunk->id;
	move_plan = get_chunk_insert_plan_cache_entry(hci, pe_entry, part, chunk->id,
										 chunk->start_time, chunk->end_time);
	entry->move_from_copyt_plan = move_plan->move_from_copyt_plan;
	return entry;
}

static char *
get_copy_table_insert_sql(ChunkCacheQueryCtx *ctx)
{
	StringInfo	where_clause = makeStringInfo();
	StringInfo	insert_clauses = makeStringInfo();
	StringInfo	sql_insert = makeStringInfo();
	ListCell   *cell;
	int			i;
	crn_set    *crn = fetch_crn_set(NULL, ctx->chunk_id);

	appendStringInfo(where_clause, "WHERE TRUE");

	if (ctx->pe_entry->num_partitions > 1)
	{
		appendStringInfo(where_clause, " AND (%s.%s(%s::TEXT, %d) BETWEEN %d AND %d)",
			 quote_identifier(ctx->pe_entry->partitioning->partfunc.schema),
					quote_identifier(ctx->pe_entry->partitioning->partfunc.name),
				  quote_identifier(ctx->pe_entry->partitioning->column),
						 ctx->pe_entry->partitioning->partfunc.modulos,
						 ctx->part->keyspace_start,
						 ctx->part->keyspace_end);
	}


	if (ctx->chunk_start_time != OPEN_START_TIME)
	{
		appendStringInfo(where_clause, " AND (%1$s >= %2$s) ",
					 quote_identifier(ctx->hci->time_column_name),
				   internal_time_to_column_literal_sql(ctx->chunk_start_time,
										  ctx->hci->time_column_type));
	}

	if (ctx->chunk_end_time != OPEN_END_TIME)
	{
		appendStringInfo(where_clause, " AND (%1$s <= %2$s) ",
					 quote_identifier(ctx->hci->time_column_name),
					 internal_time_to_column_literal_sql(ctx->chunk_end_time,
										  ctx->hci->time_column_type));
	}

	i = 0;
	foreach(cell, crn->tables)
	{
		crn_row    *tab = lfirst(cell);

		i = i + 1;
		appendStringInfo(insert_clauses, "i_%d AS (INSERT INTO %s.%s SELECT * FROM selected)",
						 i,
						 quote_identifier(tab->schema_name.data),
						 quote_identifier(tab->table_name.data)
			);
	}
	pfree(crn);
	crn = NULL;

	appendStringInfo(sql_insert, "\
						 WITH selected AS ( DELETE FROM ONLY %1$s %2$s RETURNING * ), \
						 %3$s \
						 SELECT 1", copy_table_name(ctx->hci->id),
					 where_clause->data,
					 insert_clauses->data);

	return sql_insert->data;
}

void
_chunk_cache_init(void)
{
	CreateCacheMemoryContext();
	cache_init(&chunk_insert_plan_cache);
}

void
_chunk_cache_fini(void)
{
	chunk_insert_plan_cache.post_invalidate_hook = NULL;
	cache_invalidate(&chunk_insert_plan_cache);
}
