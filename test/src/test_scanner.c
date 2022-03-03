/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>

#include "scanner.h"
#include "scan_iterator.h"
#include "chunk.h"
#include "test_utils.h"

TS_TEST_FN(ts_test_scanner)
{
	ScanIterator it;
	Relation chunkrel;
	int32 chunk_id[2] = { -1, -1 };
	int i = 0;

	/* Test pre-open relation */
	it = ts_chunk_scan_iterator_create(CurrentMemoryContext);
	chunkrel = table_open(it.ctx.table, AccessShareLock);
	it.ctx.tablerel = chunkrel;

	/* Explicit start scan to test that we can call it twice without
	 * issue. The loop will also call it */
	ts_scan_iterator_start_scan(&it);

	ts_scanner_foreach(&it)
	{
		TupleInfo *ti = ts_scan_iterator_tuple_info(&it);
		FormData_chunk fd;

		ts_chunk_formdata_fill(&fd, ti);

		elog(NOTICE, "1. Scan: \"%s.%s\"", NameStr(fd.schema_name), NameStr(fd.table_name));

		if (i < lengthof(chunk_id) && chunk_id[i] == -1)
		{
			chunk_id[i] = fd.id;
			i++;
		}
	}

	ts_scan_iterator_end(&it);

	/* Add a chunk filter and scan again */
	ts_scan_iterator_scan_key_init(&it,
								   Anum_chunk_idx_id,
								   BTEqualStrategyNumber,
								   F_INT4EQ,
								   Int32GetDatum(chunk_id[0]));

	ts_scanner_foreach(&it)
	{
		TupleInfo *ti = ts_scan_iterator_tuple_info(&it);
		FormData_chunk fd;

		ts_chunk_formdata_fill(&fd, ti);

		elog(NOTICE,
			 "2. Scan with filter: \"%s.%s\"",
			 NameStr(fd.schema_name),
			 NameStr(fd.table_name));
	}

	/* Rescan */
	ts_scan_iterator_scan_key_reset(&it);
	ts_scan_iterator_scan_key_init(&it,
								   Anum_chunk_idx_id,
								   BTEqualStrategyNumber,
								   F_INT4EQ,
								   Int32GetDatum(chunk_id[1]));
	ts_scan_iterator_rescan(&it);

	ts_scanner_foreach(&it)
	{
		TupleInfo *ti = ts_scan_iterator_tuple_info(&it);
		FormData_chunk fd;

		ts_chunk_formdata_fill(&fd, ti);

		elog(NOTICE, "3. ReScan: \"%s.%s\"", NameStr(fd.schema_name), NameStr(fd.table_name));
	}

	ts_scan_iterator_end(&it);
	table_close(chunkrel, AccessShareLock);

	/* Do another scan, but an index scan this time */
	it.ctx.tablerel = NULL;
	it.ctx.index = catalog_get_index(ts_catalog_get(), CHUNK, CHUNK_ID_INDEX);

	ts_scanner_foreach(&it)
	{
		TupleInfo *ti = ts_scan_iterator_tuple_info(&it);
		FormData_chunk fd;

		ts_chunk_formdata_fill(&fd, ti);

		elog(NOTICE, "4. IndexScan: \"%s.%s\"", NameStr(fd.schema_name), NameStr(fd.table_name));
	}

	ts_scan_iterator_close(&it);

	PG_RETURN_VOID();
}
