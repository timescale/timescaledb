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
#include "telemetry/telemetry.h"
#include "dist_util.h"
#include "data_node.h"

#define DISTRIBUTED_DB_KEY "distributed_db"
#define DISTRIBUTED_MEMBER_KEY "distributed_member"
#define NUM_DISTRIBUTED_HYPERTABLES_KEY "num_distributed_hypertables"
#define NUM_DISTRIBUTED_HYPERTABLES_MEMBERS_KEY "num_distributed_hypertables_members"
#define NUM_REPLICATED_DISTRIBUTED_HYPERTABLES_KEY "num_replicated_distributed_hypertables"
#define NUM_DATA_NODES_KEY "num_data_nodes"

static void
tsl_telemetry_add_distributed_database_info(JsonbParseState *parse_state)
{
	DistUtilMembershipStatus status = dist_util_membership();
	HypertablesStat stat;

	ts_jsonb_add_str(parse_state, DISTRIBUTED_MEMBER_KEY, dist_util_membership_str(status));

	if (status == DIST_MEMBER_NONE)
		return;

	memset(&stat, 0, sizeof(stat));
	ts_number_of_hypertables(&stat);

	ts_jsonb_add_str(parse_state,
					 NUM_DATA_NODES_KEY,
					 psprintf("%d", list_length(data_node_get_node_name_list())));

	ts_jsonb_add_str(parse_state,
					 NUM_DISTRIBUTED_HYPERTABLES_KEY,
					 psprintf("%d", stat.num_hypertables_distributed));

	ts_jsonb_add_str(parse_state,
					 NUM_REPLICATED_DISTRIBUTED_HYPERTABLES_KEY,
					 psprintf("%d", stat.num_hypertables_distributed_and_replicated));

	ts_jsonb_add_str(parse_state,
					 NUM_DISTRIBUTED_HYPERTABLES_MEMBERS_KEY,
					 psprintf("%d", stat.num_hypertables_distributed_members));
}

void
tsl_telemetry_add_info(JsonbParseState **parse_state)
{
	JsonbValue distributed_db_key;

	/* distributed_db */
	distributed_db_key.type = jbvString;
	distributed_db_key.val.string.val = DISTRIBUTED_DB_KEY;
	distributed_db_key.val.string.len = strlen(DISTRIBUTED_DB_KEY);
	pushJsonbValue(parse_state, WJB_KEY, &distributed_db_key);
	pushJsonbValue(parse_state, WJB_BEGIN_OBJECT, NULL);
	tsl_telemetry_add_distributed_database_info(*parse_state);
	pushJsonbValue(parse_state, WJB_END_OBJECT, NULL);
}
