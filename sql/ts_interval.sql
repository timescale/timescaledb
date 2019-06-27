-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.


CREATE OR REPLACE FUNCTION set_integer_now_func(hypertable REGCLASS, integer_now_func REGPROC, replace_if_exists BOOL = false) RETURNS VOID
AS '@MODULE_PATHNAME@', 'ts_set_integer_now_func'
LANGUAGE C VOLATILE STRICT;