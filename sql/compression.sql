-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

CREATE FUNCTION ts_hyperstore_handler(internal) RETURNS table_am_handler
AS '@MODULE_PATHNAME@', 'ts_hyperstore_handler' LANGUAGE C;

CREATE ACCESS METHOD hyperstore TYPE TABLE HANDLER ts_hyperstore_handler;
COMMENT ON ACCESS METHOD hyperstore IS 'TimescaleDB columnar compression';

CREATE FUNCTION ts_hsproxy_handler(internal) RETURNS index_am_handler
AS '@MODULE_PATHNAME@', 'ts_hsproxy_handler' LANGUAGE C;

CREATE ACCESS METHOD hsproxy TYPE INDEX HANDLER ts_hsproxy_handler;
COMMENT ON ACCESS METHOD hsproxy IS 'Hyperstore proxy index access method';

-- An index AM needs at least one operator class for the column type
-- that the index will be defined on. To create the index, at least
-- one column needs to be defined. For "hsproxy", the "count" column
-- on the hyperstore's internal compressed relation is used since it
-- is always present. Since "count" has type int, we need a
-- corresponding operator class.
CREATE OPERATOR CLASS int4_ops
DEFAULT FOR TYPE int4 USING hsproxy AS
	OPERATOR 1 = (int4, int4),
	FUNCTION 1 hashint4(int4);
