/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include "telemetry.h"
#include <utils/builtins.h>
#include <jsonb_utils.h>
#include "hypertable.h"
#include "license.h"
#include "telemetry/telemetry.h"
#if PG11_GE
#include "dist_util.h"
#include "data_node.h"
#endif

#define LICENSE_EDITION_COMMUNITY "community"
#define LICENSE_EDITION_ENTERPRISE "enterprise"
#define LICENSE_KIND_KEY "kind"
#define LICENSE_ID_KEY "id"
#define LICENSE_START_TIME_KEY "start_time"
#define LICENSE_END_TIME_KEY "end_time"

#define DISTRIBUTED_DB_KEY "distributed_db"
#define DISTRIBUTED_MEMBER_KEY "distributed_member"
#define NUM_DISTRIBUTED_HYPERTABLES_KEY "num_distributed_hypertables"
#define NUM_DISTRIBUTED_HYPERTABLES_MEMBERS_KEY "num_distributed_hypertables_members"
#define NUM_REPLICATED_DISTRIBUTED_HYPERTABLES_KEY "num_replicated_distributed_hypertables"
#define NUM_DATA_NODES_KEY "num_data_nodes"

static void
tsl_telemetry_add_license_info(JsonbParseState *parseState)
{
	if (!license_enterprise_enabled())
		ts_jsonb_add_str(parseState, REQ_LICENSE_EDITION, LICENSE_EDITION_COMMUNITY);
	else
	{
		char *start_time = DatumGetCString(
			DirectFunctionCall1(timestamptz_out, TimestampTzGetDatum(license_start_time())));
		char *end_time = DatumGetCString(
			DirectFunctionCall1(timestamptz_out, TimestampTzGetDatum(license_end_time())));

		ts_jsonb_add_str(parseState, REQ_LICENSE_EDITION, LICENSE_EDITION_ENTERPRISE);
		ts_jsonb_add_str(parseState, LICENSE_KIND_KEY, license_kind_str());
		ts_jsonb_add_str(parseState, LICENSE_ID_KEY, license_id_str());
		ts_jsonb_add_str(parseState, LICENSE_START_TIME_KEY, start_time);
		ts_jsonb_add_str(parseState, LICENSE_END_TIME_KEY, end_time);
	}
}

#if PG11_GE

static void
tsl_telemetry_add_distributed_database_info(JsonbParseState *parseState)
{
	DistUtilMembershipStatus status = dist_util_membership();
	HypertablesStat stat;

	ts_jsonb_add_str(parseState, DISTRIBUTED_MEMBER_KEY, dist_util_membership_str(status));

	if (status == DIST_MEMBER_NONE)
		return;

	memset(&stat, 0, sizeof(stat));
	ts_number_of_hypertables(&stat);

	ts_jsonb_add_str(parseState,
					 NUM_DATA_NODES_KEY,
					 psprintf("%d", list_length(data_node_get_node_name_list())));

	ts_jsonb_add_str(parseState,
					 NUM_DISTRIBUTED_HYPERTABLES_KEY,
					 psprintf("%d", stat.num_hypertables_distributed));

	ts_jsonb_add_str(parseState,
					 NUM_REPLICATED_DISTRIBUTED_HYPERTABLES_KEY,
					 psprintf("%d", stat.num_hypertables_distributed_and_replicated));

	ts_jsonb_add_str(parseState,
					 NUM_DISTRIBUTED_HYPERTABLES_MEMBERS_KEY,
					 psprintf("%d", stat.num_hypertables_distributed_members));
}

#endif

void
tsl_telemetry_add_info(JsonbParseState **parseState)
{
	JsonbValue license_key;

	/* license */
	license_key.type = jbvString;
	license_key.val.string.val = REQ_LICENSE_INFO;
	license_key.val.string.len = strlen(REQ_LICENSE_INFO);
	pushJsonbValue(parseState, WJB_KEY, &license_key);
	pushJsonbValue(parseState, WJB_BEGIN_OBJECT, NULL);
	tsl_telemetry_add_license_info(*parseState);
	pushJsonbValue(parseState, WJB_END_OBJECT, NULL);

#if PG11_GE
	{
		JsonbValue distributed_db_key;

		/* distributed_db */
		distributed_db_key.type = jbvString;
		distributed_db_key.val.string.val = DISTRIBUTED_DB_KEY;
		distributed_db_key.val.string.len = strlen(DISTRIBUTED_DB_KEY);
		pushJsonbValue(parseState, WJB_KEY, &distributed_db_key);
		pushJsonbValue(parseState, WJB_BEGIN_OBJECT, NULL);
		tsl_telemetry_add_distributed_database_info(*parseState);
		pushJsonbValue(parseState, WJB_END_OBJECT, NULL);
	}
#endif
}
