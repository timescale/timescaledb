CREATE OR REPLACE FUNCTION get_hostname()
    RETURNS TEXT LANGUAGE SQL STABLE AS
$BODY$
SELECT current_setting('iobeam.hostname');
$BODY$;

