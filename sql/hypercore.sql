-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

CREATE FUNCTION ts_hypercore_handler(internal) RETURNS table_am_handler
AS '@MODULE_PATHNAME@', 'ts_hypercore_handler' LANGUAGE C;

CREATE ACCESS METHOD hypercore TYPE TABLE HANDLER ts_hypercore_handler;
COMMENT ON ACCESS METHOD hypercore IS 'Storage engine using hybrid row/columnar compression';

CREATE FUNCTION ts_hypercore_proxy_handler(internal) RETURNS index_am_handler
AS '@MODULE_PATHNAME@', 'ts_hypercore_proxy_handler' LANGUAGE C;

CREATE ACCESS METHOD hypercore_proxy TYPE INDEX HANDLER ts_hypercore_proxy_handler;
COMMENT ON ACCESS METHOD hypercore_proxy IS 'Hypercore proxy index access method';

-- An index AM needs at least one operator class for the column type
-- that the index will be defined on. To create the index, at least
-- one column needs to be defined. For "hypercore_proxy", the "count" column
-- on the hypercore's internal compressed relation is used since it
-- is always present. Since "count" has type int, we need a
-- corresponding operator class.
CREATE OPERATOR CLASS int4_ops
DEFAULT FOR TYPE int4 USING hypercore_proxy AS
	OPERATOR 1 = (int4, int4),
	FUNCTION 1 hashint4(int4);
