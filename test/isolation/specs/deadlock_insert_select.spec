##test case for github issue 865. avoid deadlocks between select and insert

setup
{
 CREATE TABLE tssensor_type ( sensortypeid integer PRIMARY KEY, type text NOT NULL) ;

 CREATE TABLE tssensor_location ( sensorlocationid integer PRIMARY KEY, location text NOT NULL) ;

 CREATE TABLE hourly_tssensor_data (
        sensortypeid integer NOT NULL REFERENCES tssensor_type(sensortypeid),
        sensorlocationid integer NOT NULL REFERENCES tssensor_location(sensorlocationid),
        measurementtime timestamp with time zone NOT NULL,
        measurementvalues jsonb
)
;

 SELECT create_hypertable('hourly_tssensor_data', 'measurementtime', chunk_time_interval => interval '1 day');

 INSERT INTO tssensor_location  VALUES (1, 'Location A');
 INSERT INTO tssensor_type  VALUES (1, 'Type 1');
 INSERT INTO hourly_tssensor_data (sensortypeid,sensorlocationid,measurementtime,measurementvalues)
 SELECT 1,1,
    generate_series( '2018-12-01 00:00'::timestamp, '2018-12-31 12:00','1 minute'), NULL
;

 select count(*) from _timescaledb_catalog.chunk  
 where hypertable_id =  (select id from _timescaledb_catalog.hypertable where table_name like 'hourly_tssensor_data')
 group by hypertable_id;

}

teardown 
{ 
 select min(measurementtime) from hourly_tssensor_data;
 select count(*) from _timescaledb_catalog.chunk  
 where hypertable_id =  (select id from _timescaledb_catalog.hypertable where table_name like 'hourly_tssensor_data')
 group by hypertable_id;
 DROP TABLE hourly_tssensor_data;
 DROP table tssensor_type;
 DROP table tssensor_location; 
}

####setup	{ BEGIN; SET LOCAL lock_timeout = '300s'; SET LOCAL deadlock_timeout = '300s'; }
###setup	{ BEGIN; SET LOCAL lock_timeout = '300s'; SET LOCAL deadlock_timeout = '300s'; }
session "s1"
setup	{ BEGIN;  }
step "s1a"	{ select drop_chunks( '2018-12-25 00:00'::timestamptz, 'hourly_tssensor_data'); }
step "s1b"	{ COMMIT; }

session "s2"
setup	{ BEGIN;  }
step "s2a"	{  SELECT st.type, sl.location, hsd.measurementtime, hsd.measurementvalues
                FROM hourly_tssensor_data hsd
                JOIN tssensor_location sl on sl.sensorlocationid = hsd.sensorlocationid
                JOIN tssensor_type st on st.sensortypeid = hsd.sensortypeid
                WHERE hsd.measurementtime >= '2018-12-01 03:00:00+00'
                AND hsd.measurementtime <= '2018-12-01 04:00:00+00'
                AND st.type = 'Type 1'
                order by st.type, sl.location, hsd.measurementtime ; }
step "s2b"	{ COMMIT; }

