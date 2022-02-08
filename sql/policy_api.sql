-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- Add a retention policy to a hypertable or continuous aggregate.
-- The retention_window (typically an INTERVAL) determines the
-- window beyond which data is dropped at the time
-- of execution of the policy (e.g., '1 week'). Note that the retention
-- window will always align with chunk boundaries, thus the window
-- might be larger than the given one, but never smaller. In other
-- words, some data beyond the retention window
-- might be kept, but data within the window will never be deleted.
CREATE OR REPLACE FUNCTION @extschema@.add_retention_policy(
       relation REGCLASS,
       drop_after "any",
       if_not_exists BOOL = false
)
RETURNS INTEGER AS '@MODULE_PATHNAME@', 'ts_policy_retention_add'
LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE FUNCTION @extschema@.remove_retention_policy(
    relation REGCLASS,
    if_exists BOOL = false
) RETURNS VOID
AS '@MODULE_PATHNAME@', 'ts_policy_retention_remove'
LANGUAGE C VOLATILE STRICT;

/* reorder policy */
CREATE OR REPLACE FUNCTION @extschema@.add_reorder_policy(hypertable REGCLASS, index_name NAME, if_not_exists BOOL = false) RETURNS INTEGER
AS '@MODULE_PATHNAME@', 'ts_policy_reorder_add'
LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE FUNCTION @extschema@.remove_reorder_policy(hypertable REGCLASS, if_exists BOOL = false) RETURNS VOID
AS '@MODULE_PATHNAME@', 'ts_policy_reorder_remove'
LANGUAGE C VOLATILE STRICT;

/* compression policy */
CREATE OR REPLACE FUNCTION @extschema@.add_compression_policy(hypertable REGCLASS, compress_after "any", if_not_exists BOOL = false)
RETURNS INTEGER
AS '@MODULE_PATHNAME@', 'ts_policy_compression_add'
LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE FUNCTION @extschema@.remove_compression_policy(hypertable REGCLASS, if_exists BOOL = false) RETURNS BOOL
AS '@MODULE_PATHNAME@', 'ts_policy_compression_remove'
LANGUAGE C VOLATILE STRICT;

/* continuous aggregates policy */
CREATE OR REPLACE FUNCTION @extschema@.add_continuous_aggregate_policy(continuous_aggregate REGCLASS, start_offset "any", end_offset "any", schedule_interval INTERVAL, if_not_exists BOOL = false)
RETURNS INTEGER
AS '@MODULE_PATHNAME@', 'ts_policy_refresh_cagg_add'
LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION @extschema@.remove_continuous_aggregate_policy(continuous_aggregate REGCLASS, if_not_exists BOOL = false)
RETURNS VOID
AS '@MODULE_PATHNAME@', 'ts_policy_refresh_cagg_remove'
LANGUAGE C VOLATILE STRICT;
