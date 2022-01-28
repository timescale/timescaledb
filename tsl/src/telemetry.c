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

#define NUM_DATA_NODES_KEY "num_data_nodes"
#define DISTRIBUTED_MEMBER_KEY "distributed_member"
void
tsl_telemetry_add_info(JsonbParseState **parse_state)
{
	DistUtilMembershipStatus status = dist_util_membership();

	ts_jsonb_add_str(*parse_state, DISTRIBUTED_MEMBER_KEY, dist_util_membership_str(status));

	if (status == DIST_MEMBER_ACCESS_NODE)
		ts_jsonb_add_int64(*parse_state,
						   NUM_DATA_NODES_KEY,
						   list_length(data_node_get_node_name_list()));
}
