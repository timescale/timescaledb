/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <executor/spi.h>
#include <utils/guc.h>

#include "replication.h"

ReplicationInfo
ts_telemetry_replication_info_gather(void)
{
	int res;
	bool isnull;
	Datum data;
	ReplicationInfo info = {
		.got_num_wal_senders = false,
		.got_is_wal_receiver = false,
	};

	if (SPI_connect() != SPI_OK_CONNECT)
		return info;

	/* Lock down search_path */
	int save_nestlevel = NewGUCNestLevel();
	RestrictSearchPath();

	res = SPI_execute("SELECT cast(count(pid) as int) from pg_catalog.pg_stat_get_wal_senders() "
					  "WHERE pid is not null",
					  true, /* read_only */
					  0		/*count*/
	);

	if (res >= 0)
	{
		data = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull);
		info.num_wal_senders = DatumGetInt32(data);
		info.got_num_wal_senders = true;
	}

	/* use count() > 0 in case they start having pg_stat_get_wal_receiver()
	 * return no rows when the DB isn't a replica */
	res = SPI_execute("SELECT count(pid) > 0 from pg_catalog.pg_stat_get_wal_receiver() WHERE pid "
					  "is not null",
					  true, /* read_only */
					  0		/*count*/
	);
	if (res >= 0)
	{
		data = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull);
		info.is_wal_receiver = DatumGetBool(data);
		info.got_is_wal_receiver = true;
	}

	res = SPI_finish();
	if (res != SPI_OK_FINISH)
		elog(ERROR, "SPI_finish failed: %s", SPI_result_code_string(res));

	/* Restore search_path */
	AtEOXact_GUC(false, save_nestlevel);

	return info;
}
