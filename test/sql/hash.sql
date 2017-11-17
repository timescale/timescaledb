-- Test hashing Const values. We should expect the same hash value for
-- all integer types when values are compatible
SELECT _timescaledb_internal.get_partition_hash(1::int);
SELECT _timescaledb_internal.get_partition_hash(1::bigint);
SELECT _timescaledb_internal.get_partition_hash(1::smallint);
SELECT _timescaledb_internal.get_partition_hash(true);

-- Floating point types should also hash the same for compatible values
SELECT _timescaledb_internal.get_partition_hash(1.0::real);
SELECT _timescaledb_internal.get_partition_hash(1.0::double precision);
-- Float aliases
SELECT _timescaledb_internal.get_partition_hash(1.0::float);
SELECT _timescaledb_internal.get_partition_hash(1.0::float4);
SELECT _timescaledb_internal.get_partition_hash(1.0::float8);
SELECT _timescaledb_internal.get_partition_hash(1.0::numeric);

-- 'name' and '"char"' are internal PostgreSQL types, which are not
-- intended for use by the general user. They are included here only
-- for completeness
-- https://www.postgresql.org/docs/10/static/datatype-character.html#datatype-character-special-table
SELECT _timescaledb_internal.get_partition_hash('c'::name);
SELECT _timescaledb_internal.get_partition_hash('c'::"char");

-- String and character hashes should also have the same output for
-- compatible values
SELECT _timescaledb_internal.get_partition_hash('c'::char);
SELECT _timescaledb_internal.get_partition_hash('c'::varchar(2));
SELECT _timescaledb_internal.get_partition_hash('c'::text);
-- 'c' is 0x63 in ASCII
SELECT _timescaledb_internal.get_partition_hash(E'\\x63'::bytea);

-- Time and date types
SELECT _timescaledb_internal.get_partition_hash(interval '1 day');
SELECT _timescaledb_internal.get_partition_hash('2017-03-22T09:18:23'::timestamp);
SELECT _timescaledb_internal.get_partition_hash('2017-03-22T09:18:23'::timestamptz);
SELECT _timescaledb_internal.get_partition_hash('2017-03-22'::date);
SELECT _timescaledb_internal.get_partition_hash('10:00:00'::time);
SELECT _timescaledb_internal.get_partition_hash('10:00:00-1'::timetz);

-- Other types
SELECT _timescaledb_internal.get_partition_hash(ARRAY[1,2,3]);
SELECT _timescaledb_internal.get_partition_hash('08002b:010203'::macaddr);
SELECT _timescaledb_internal.get_partition_hash('192.168.100.128/25'::cidr);
SELECT _timescaledb_internal.get_partition_hash('192.168.100.128'::inet);
SELECT _timescaledb_internal.get_partition_hash('2001:4f8:3:ba:2e0:81ff:fe22:d1f1'::inet);
SELECT _timescaledb_internal.get_partition_hash('2001:4f8:3:ba:2e0:81ff:fe22:d1f1/128'::cidr);
SELECT _timescaledb_internal.get_partition_hash('{ "foo": "bar" }'::jsonb);
SELECT _timescaledb_internal.get_partition_hash('4b6a5eec-b344-11e7-abc4-cec278b6b50a'::uuid);
SELECT _timescaledb_internal.get_partition_hash(1::regclass);
SELECT _timescaledb_internal.get_partition_hash(int4range(10, 20));
SELECT _timescaledb_internal.get_partition_hash(int8range(10, 20));
SELECT _timescaledb_internal.get_partition_hash(numrange(10, 20));
SELECT _timescaledb_internal.get_partition_hash(tsrange('2017-03-22T09:18:23', '2017-03-23T09:18:23'));
SELECT _timescaledb_internal.get_partition_hash(tstzrange('2017-03-22T09:18:23+01', '2017-03-23T09:18:23+00'));

-- Test hashing Var values
CREATE TABLE hash_test(id int, value text);
INSERT INTO hash_test VALUES (1, 'test');

-- Test Vars
SELECT _timescaledb_internal.get_partition_hash(id) FROM hash_test;
SELECT _timescaledb_internal.get_partition_hash(value) FROM hash_test;
-- Test coerced value
SELECT _timescaledb_internal.get_partition_hash(id::text) FROM hash_test;

-- Test legacy function that converts values to text first
SELECT _timescaledb_internal.get_partition_for_key('4b6a5eec-b344-11e7-abc4-cec278b6b50a'::text);
SELECT _timescaledb_internal.get_partition_for_key('4b6a5eec-b344-11e7-abc4-cec278b6b50a'::varchar);
SELECT _timescaledb_internal.get_partition_for_key(187);
SELECT _timescaledb_internal.get_partition_for_key(187::bigint);
SELECT _timescaledb_internal.get_partition_for_key(187::numeric);
SELECT _timescaledb_internal.get_partition_for_key(187::double precision);
SELECT _timescaledb_internal.get_partition_for_key(int4range(10, 20));
SELECT _timescaledb_internal.get_partition_hash('08002b:010203'::macaddr);
