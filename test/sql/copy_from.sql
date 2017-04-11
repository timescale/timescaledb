\o /dev/null
\ir include/insert_two_partitions.sql
\o

COPY (SELECT * FROM "two_Partitions" ) TO STDOUT;


