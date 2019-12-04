-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

SELECT * FROM mat_before;

INSERT INTO conditions_before
SELECT generate_series('2018-12-01 00:00'::timestamp, '2018-12-31 00:00'::timestamp, '1 day'), 'POR', 165, 75, 40, 70, NULL, (1,2)::custom_type, 2, true;

--cause invalidations way in the past
INSERT INTO conditions_before
SELECT generate_series('2017-12-01 00:00'::timestamp, '2017-12-31 00:00'::timestamp, '1 day'), 'POR', 1065, 75, 40, 70, NULL, (1,2)::custom_type, 2, true;

SELECT * FROM mat_before;

REFRESH MATERIALIZED VIEW mat_before;

--the max of the temp for the POR should now be 165
SELECT * FROM mat_before;
