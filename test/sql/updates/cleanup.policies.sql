-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

PERFORM remove_reorder_policy('policy_test_timestamptz');
PERFORM remove_retention_policy('policy_test_timestamptz');
PERFORM remove_compression_policy('policy_test_timestamptz');

