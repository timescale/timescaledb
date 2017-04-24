\o /dev/null
\ir include/insert_two_partitions.sql
\o

COPY (SELECT * FROM "two_Partitions" ORDER BY "timeCustom", device_id) TO STDOUT;


