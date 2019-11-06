-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

--expects QUERY1 and QUERY2 to be set, expects data can be compared
with query1 AS (
  SELECT row_number() OVER(ORDER BY q.*) row_number, * FROM (:QUERY1) as q
),
query2 AS (
  SELECT row_number() OVER (ORDER BY v.*) row_number, * FROM (:QUERY2) as v
)
SELECT count(*) FILTER (WHERE query1.row_number IS DISTINCT FROM query2.row_number OR query1.show_chunks IS DISTINCT FROM query2.drop_chunks) AS "Different Rows",
coalesce(max(query1.row_number), 0) AS "Total Rows from Query 1", coalesce(max(query2.row_number), 0) AS "Total Rows from Query 2"
FROM query1 FULL OUTER JOIN query2 ON (query1.row_number = query2.row_number);
