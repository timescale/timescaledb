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
