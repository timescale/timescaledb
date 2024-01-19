-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- This file contains utility functions that are only used in debug
-- builds for debugging and testing.

CREATE OR REPLACE FUNCTION debug_waitpoint_enable(TEXT) RETURNS VOID LANGUAGE C VOLATILE STRICT
AS '@MODULE_PATHNAME@', 'ts_debug_point_enable';

CREATE OR REPLACE FUNCTION debug_waitpoint_release(TEXT) RETURNS VOID LANGUAGE C VOLATILE STRICT
AS '@MODULE_PATHNAME@', 'ts_debug_point_release';

CREATE OR REPLACE FUNCTION debug_waitpoint_id(TEXT) RETURNS BIGINT LANGUAGE C VOLATILE STRICT
AS '@MODULE_PATHNAME@', 'ts_debug_point_id';
