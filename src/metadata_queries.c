#include <postgres.h>
#include <catalog/pg_type.h>
#include <commands/trigger.h>
#include <executor/spi.h>
#include <utils/builtins.h>

#include "metadata_queries.h"
#include "chunk.h"
#include "dimension.h"
#include "hypertable.h"

/* Utility function to prepare an SPI plan */
static SPIPlanPtr
prepare_plan(const char *src, int nargs, Oid *argtypes)
{
	SPIPlanPtr	plan;

	if (SPI_connect() < 0)
	{
		elog(ERROR, "Could not connect for prepare");
	}
	plan = SPI_prepare(src, nargs, argtypes);
	if (NULL == plan)
	{
		elog(ERROR, "Could not prepare plan");
	}
	if (SPI_keepplan(plan) != 0)
	{
		elog(ERROR, "Could not keep plan");
	}
	if (SPI_finish() < 0)
	{
		elog(ERROR, "Could not finish for prepare");
	}

	return plan;
}

#define DEFINE_PLAN(fnname, query, num, args)	\
	static SPIPlanPtr fnname() {				\
		static SPIPlanPtr plan = NULL;			\
		if(plan != NULL)						\
			return plan;						\
		plan = prepare_plan(query, num, args);	\
		return plan;							\
	}

/* old_schema, old_name, new_schema, new_name */
#define RENAME_HYPERTABLE_ARGS (Oid[]) {NAMEOID, NAMEOID, TEXTOID, TEXTOID}
#define RENAME_HYPERTABLE "SELECT * FROM _timescaledb_internal.rename_hypertable($1, $2, $3, $4)"

/* plan to rename hypertable */
DEFINE_PLAN(rename_hypertable_plan, RENAME_HYPERTABLE, 4, RENAME_HYPERTABLE_ARGS)

/* schema, name */
#define TRUNCATE_HYPERTABLE_ARGS (Oid[]) {NAMEOID, NAMEOID}
#define TRUNCATE_HYPERTABLE "SELECT * FROM _timescaledb_internal.truncate_hypertable($1, $2)"

/* plan to truncate hypertable */
DEFINE_PLAN(truncate_hypertable_plan, TRUNCATE_HYPERTABLE, 2, TRUNCATE_HYPERTABLE_ARGS)

static void
hypertable_rename_spi_connected(Hypertable *ht, const char *new_schema_name, const char *new_table_name, SPIPlanPtr plan)
{
	int			ret;
	Datum		args[4];

	args[0] = PointerGetDatum(NameStr(ht->fd.schema_name));
	args[1] = PointerGetDatum(NameStr(ht->fd.table_name));

	args[2] = CStringGetTextDatum(new_schema_name);
	args[3] = CStringGetTextDatum(new_table_name);

	ret = SPI_execute_plan(plan, args, NULL, false, 4);

	if (ret <= 0)
		elog(ERROR, "Got an SPI error %d", ret);

	return;
}

void
spi_hypertable_rename(Hypertable *ht, const char *new_schema_name, const char *new_table_name)
{
	SPIPlanPtr	plan = rename_hypertable_plan();


	if (strlen(new_schema_name) > sizeof(NameData) - 1)
	{
		elog(ERROR, "New schema name '%s' is too long", new_schema_name);
	}

	if (strlen(new_table_name) > sizeof(NameData) - 1)
	{
		elog(ERROR, "New schema name '%s' is too long", new_table_name);
	}

	if (SPI_connect() < 0)
		elog(ERROR, "Got an SPI connect error");

	hypertable_rename_spi_connected(ht, new_schema_name, new_table_name, plan);

	SPI_finish();

	return;
}

static void
hypertable_truncate_spi_connected(Hypertable *ht, SPIPlanPtr plan)
{
	int			ret;
	Datum		args[2];

	args[0] = PointerGetDatum(NameStr(ht->fd.schema_name));
	args[1] = PointerGetDatum(NameStr(ht->fd.table_name));

	ret = SPI_execute_plan(plan, args, NULL, false, 2);

	if (ret <= 0)
		elog(ERROR, "Got an SPI error %d", ret);

	return;
}

void
spi_hypertable_truncate(Hypertable *ht)
{
	SPIPlanPtr	plan = truncate_hypertable_plan();

	if (SPI_connect() < 0)
		elog(ERROR, "Got an SPI connect error");

	hypertable_truncate_spi_connected(ht, plan);

	SPI_finish();

	return;
}
