#include <postgres.h>
#include <nodes/readfuncs.h>

#include "chunk_dispatch_info.h"

static void
chunk_dispatch_info_copy(struct ExtensibleNode *newnode,
						 const struct ExtensibleNode *oldnode)
{
	ChunkDispatchInfo *newinfo = (ChunkDispatchInfo *) newnode;
	const ChunkDispatchInfo *oldinfo = (const ChunkDispatchInfo *) oldnode;

	newinfo->hypertable_relid = oldinfo->hypertable_relid;
}

static bool
chunk_dispatch_info_equal(const struct ExtensibleNode *an,
						  const struct ExtensibleNode *bn)
{
	const ChunkDispatchInfo *a = (const ChunkDispatchInfo *) an;
	const ChunkDispatchInfo *b = (const ChunkDispatchInfo *) bn;

	return a->hypertable_relid == b->hypertable_relid;
}

static void
chunk_dispatch_info_out(struct StringInfoData *str,
						const struct ExtensibleNode *node)
{
	const ChunkDispatchInfo *info = (const ChunkDispatchInfo *) node;

	appendStringInfo(str, " :hypertableOid %d", info->hypertable_relid);
}

static void
chunk_dispatch_info_read(struct ExtensibleNode *node)
{
	ChunkDispatchInfo *info = (ChunkDispatchInfo *) node;
	int			length;
	char	   *token;

	/* Skip :hypertableOid */
	token = pg_strtok(&length);

	/* Read OID */
	token = pg_strtok(&length);

	if (token == NULL)
		elog(ERROR, "missing hypertable relation ID");

	info->hypertable_relid = strtol(token, NULL, 10);

	/* Skip :Query */
	token = pg_strtok(&length);

	if (token == NULL)
		elog(ERROR, "missing query node");
}

static ExtensibleNodeMethods chunk_dispatch_info_methods = {
	.extnodename = "ChunkDispatchInfo",
	.node_size = sizeof(ChunkDispatchInfo),
	.nodeCopy = chunk_dispatch_info_copy,
	.nodeEqual = chunk_dispatch_info_equal,
	.nodeOut = chunk_dispatch_info_out,
	.nodeRead = chunk_dispatch_info_read,
};

ChunkDispatchInfo *
chunk_dispatch_info_create(Oid hypertable_relid, Query *parse)
{
	ChunkDispatchInfo *info = (ChunkDispatchInfo *) newNode(sizeof(ChunkDispatchInfo),
															T_ExtensibleNode);

	info->enode.extnodename = chunk_dispatch_info_methods.extnodename;
	info->hypertable_relid = hypertable_relid;
	return info;
}

void
_chunk_dispatch_info_init(void)
{
	RegisterExtensibleNodeMethods(&chunk_dispatch_info_methods);
}

void
_chunk_dispatch_info_fini(void)
{
}
