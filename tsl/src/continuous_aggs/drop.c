/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <catalog/pg_type.h>
#include <executor/spi.h>
#include <lib/stringinfo.h>
#include <utils/builtins.h>

#include "drop.h"

#include <continuous_agg.h>

#include "create.h"

void
ts_continuous_agg_drop_chunks_by_chunk_id(int32 raw_hypertable_id, Chunk **chunks_ptr,
										  Size num_chunks)
{
	ListCell *lc;
	Oid arg_type = INT4OID;
	List *continuous_aggs = ts_continuous_aggs_find_by_raw_table_id(raw_hypertable_id);
	StringInfo command = makeStringInfo();
	CatalogSecurityContext sec_ctx;
	Chunk *chunks = *chunks_ptr;

	ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);

	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "could not connect to SPI deleting materialization");

	foreach (lc, continuous_aggs)
	{
		int32 i;
		SPIPlanPtr delete_plan;
		ContinuousAgg *agg = lfirst(lc);
		Hypertable *mat_table = ts_hypertable_get_by_id(agg->data.mat_hypertable_id);

		resetStringInfo(command);

		appendStringInfo(command,
						 "DELETE FROM %s.%s AS D WHERE "
						 "D.%s = $1",
						 quote_identifier(NameStr(mat_table->fd.schema_name)),
						 quote_identifier(NameStr(mat_table->fd.table_name)),
						 quote_identifier(CONTINUOUS_AGG_CHUNK_ID_COL_NAME));

		delete_plan = SPI_prepare(command->data, 1, &arg_type);
		if (delete_plan == NULL)
			elog(ERROR, "could not prepare delete materialization");

		for (i = 0; i < num_chunks; i++)
		{
			Datum arg = Int32GetDatum(chunks[i].fd.id);
			int res = SPI_execute_plan(delete_plan, &arg, NULL, false, 0);
			if (res < 0)
				elog(ERROR, "could not delete from the materialization");
		}

		SPI_freeplan(delete_plan);
	}

	SPI_finish();

	ts_catalog_restore_user(&sec_ctx);
}
