-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

CREATE OR REPLACE FUNCTION assert_true(
    val boolean
)
 RETURNS VOID LANGUAGE PLPGSQL IMMUTABLE AS
$BODY$
BEGIN
    IF !val THEN
        RAISE 'Assert failed';
    END IF;
END
$BODY$;


CREATE OR REPLACE FUNCTION assert_equal(
    val1 anyelement,
    val2 anyelement
)
 RETURNS VOID LANGUAGE PLPGSQL IMMUTABLE AS
$BODY$
BEGIN
    IF val1 != val2 THEN
        RAISE 'Assert failed';
    END IF;
END
$BODY$;

\c :DBNAME :ROLE_SUPERUSER

CREATE OR REPLACE FUNCTION allow_downgrade_to_apache()
RETURNS VOID
AS :MODULE_PATHNAME, 'ts_allow_downgrade_to_apache'
LANGUAGE C;

\c :DBNAME :ROLE_DEFAULT_PERM_USER
