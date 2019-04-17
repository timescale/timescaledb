-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

CREATE OR REPLACE FUNCTION timescaledb_pre_restore() RETURNS BOOL AS 
$BODY$
DECLARE 
    db text; 
BEGIN
    SELECT current_database() INTO db;
    EXECUTE format($$ALTER DATABASE %I SET timescaledb.restoring ='on'$$, db);
    SET SESSION timescaledb.restoring = 'on';
    PERFORM _timescaledb_internal.stop_background_workers();
    RETURN true;
END
$BODY$ 
LANGUAGE PLPGSQL;


CREATE OR REPLACE FUNCTION timescaledb_post_restore() RETURNS BOOL AS 
$BODY$
DECLARE 
    db text; 
BEGIN
    SELECT current_database() INTO db;
    EXECUTE format($$ALTER DATABASE %I SET timescaledb.restoring ='off'$$, db);
    SET SESSION timescaledb.restoring='off';
    PERFORM _timescaledb_internal.start_background_workers();
    RETURN true;
END
$BODY$ 
LANGUAGE PLPGSQL;