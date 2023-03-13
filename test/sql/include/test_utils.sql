-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

CREATE OR REPLACE FUNCTION assert_true(
    val boolean
)
 RETURNS VOID LANGUAGE PLPGSQL IMMUTABLE AS
$BODY$
BEGIN
    IF val IS NOT TRUE THEN
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
    IF (val1 = val2) IS NOT TRUE THEN
        RAISE 'Assert failed: % = %',val1,val2;
    END IF;
END
$BODY$;
