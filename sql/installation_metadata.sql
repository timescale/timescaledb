-- Copyright (c) 2016-2018  Timescale, Inc. All Rights Reserved.
--
-- This file is licensed under the Apache License, see LICENSE-APACHE
-- at the top level directory of the timescaledb distribution.

CREATE OR REPLACE FUNCTION _timescaledb_internal.generate_uuid() RETURNS UUID
AS '@MODULE_PATHNAME@', 'ts_uuid_generate' LANGUAGE C VOLATILE STRICT;

-- Insert uuid and install_timestamp on database creation. Don't
-- create exported_uuid because it gets exported and installed during
-- pg_dump, which would cause a conflict.
INSERT INTO _timescaledb_catalog.installation_metadata
SELECT 'uuid', _timescaledb_internal.generate_uuid() ON CONFLICT DO NOTHING;
INSERT INTO _timescaledb_catalog.installation_metadata
SELECT 'install_timestamp', now() ON CONFLICT DO NOTHING;
