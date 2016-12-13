*.csv files in this directory will be imported to the schema expected_outputs in tables named as the files excluding the file suffix.
The column delimiter is semi colon to allow commas within jsons values.

the first row of the file is interpreted as column names and types

ex:

file: example.csv

s text;i int
foo;2
bar;3

becomes a table in the test_outputs schema

select * from test_outputs.example;
  s  | i 
-----+---
 foo | 2
 bar | 3
(2 rows)

\d test_outputs.example
  Table "test_outputs.example"
 Column |  Type   | Modifiers 
--------+---------+-----------
 s      | text    | 
 i      | integer | 

