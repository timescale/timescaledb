-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

CREATE OR REPLACE FUNCTION ts_hypercore_handler(internal) RETURNS table_am_handler
AS '@MODULE_PATHNAME@', 'ts_hypercore_handler' LANGUAGE C;

CREATE OR REPLACE FUNCTION ts_hypercore_proxy_handler(internal) RETURNS index_am_handler
AS '@MODULE_PATHNAME@', 'ts_hypercore_proxy_handler' LANGUAGE C;

