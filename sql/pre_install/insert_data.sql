-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

--insert data for compression_algorithm --
insert into _timescaledb_catalog.compression_algorithm( id, version, name, description) values
( 0, 1, 'COMPRESSION_ALGORITHM_NONE', 'no compression'),
( 1, 1, 'COMPRESSION_ALGORITHM_ARRAY', 'array'),
( 2, 1, 'COMPRESSION_ALGORITHM_DICTIONARY', 'dictionary'),
( 3, 1, 'COMPRESSION_ALGORITHM_GORILLA', 'gorilla'),
( 4, 1, 'COMPRESSION_ALGORITHM_DELTADELTA', 'deltadelta');
