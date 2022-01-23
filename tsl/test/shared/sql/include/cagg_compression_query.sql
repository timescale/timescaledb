-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

SELECT * FROM :TEST_TABLE1 m INNER JOIN :TEST_TABLE2 d USING (device_id) ORDER BY m.bucket, m.device_id;

SELECT * FROM :TEST_TABLE1 m NATURAL JOIN :TEST_TABLE2 d ORDER BY m.bucket, m.device_id;

SELECT * FROM :TEST_TABLE1 m LEFT JOIN :TEST_TABLE2 d USING (device_id) ORDER BY m.bucket, m.device_id;

SELECT * FROM :TEST_TABLE1 m RIGHT JOIN :TEST_TABLE2 d USING (device_id) ORDER BY m.bucket, m.device_id;

SELECT * FROM :TEST_TABLE1 m FULL JOIN :TEST_TABLE2 d USING (device_id) ORDER BY m.bucket, m.device_id;

SELECT * FROM :TEST_TABLE1 m CROSS JOIN :TEST_TABLE2 d ORDER BY m.bucket, m.device_id, d.device_id;

SELECT * FROM :TEST_TABLE1 m JOIN LATERAL (SELECT * FROM :TEST_TABLE2 d WHERE d.device_id = m.device_id) AS d ON true ORDER BY m.bucket, m.device_id;
