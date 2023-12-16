-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- This file is always prepended to all upgrade scripts.

-- ERROR if trying to update the extension on PG16 using Multi-Node
DO $$
DECLARE
  data_nodes TEXT;
  dist_hypertables TEXT;
BEGIN
  IF current_setting('server_version_num')::int >= 160000 THEN
    SELECT string_agg(format('%I.%I', hypertable_schema, hypertable_name), ', ')
    INTO dist_hypertables
    FROM timescaledb_information.hypertables
    WHERE is_distributed IS TRUE;

    IF dist_hypertables IS NOT NULL THEN
      RAISE USING
        ERRCODE = 'feature_not_supported',
        MESSAGE = 'cannot upgrade because multi-node is not supported on PostgreSQL >= 16',
        DETAIL = 'The following distributed hypertables should be migrated to regular: '||dist_hypertables;
    END IF;

    SELECT string_agg(format('%I', node_name), ', ')
    INTO data_nodes
    FROM timescaledb_information.data_nodes;

    IF data_nodes IS NOT NULL THEN
      RAISE USING
        ERRCODE = 'feature_not_supported',
        MESSAGE = 'cannot upgrade because multi-node is not supported on PostgreSQL >= 16',
        DETAIL = 'The following data nodes should be removed: '||data_nodes;
    END IF;
  END IF;
END $$;

