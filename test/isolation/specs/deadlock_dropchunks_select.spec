##github issue 865 deadlock between select and drop chunks

setup
{
 CREATE TABLE ST ( sid int PRIMARY KEY, typ text) ;
 CREATE TABLE SL ( lid int PRIMARY KEY, loc text) ;
 CREATE TABLE DT ( sid int REFERENCES ST(sid), lid int REFERENCES SL(lid), mtim timestamp with time zone ) ;
 SELECT create_hypertable('DT', 'mtim', chunk_time_interval => interval '1 day');
 INSERT INTO SL  VALUES (1, 'LA');
 INSERT INTO ST  VALUES (1, 'T1');
 INSERT INTO DT (sid,lid,mtim)
 SELECT 1,1, generate_series( '2018-12-01 00:00'::timestamp, '2018-12-31 12:00','1 minute') ;
}

teardown
{ DROP TABLE DT; DROP table ST; DROP table SL; }

session "s1"
setup	{ BEGIN; SET TRANSACTION ISOLATION LEVEL READ COMMITTED; SET LOCAL lock_timeout = '50ms'; SET LOCAL deadlock_timeout = '10ms'; }
step "s1a"	{ SELECT count (*) FROM drop_chunks( '2018-12-25 00:00'::timestamptz, 'dt'); }
step "s1b"	{ COMMIT; }

session "s2"
setup	{ BEGIN; SET TRANSACTION ISOLATION LEVEL READ COMMITTED; SET LOCAL lock_timeout = '50ms'; SET LOCAL deadlock_timeout = '10ms'; }
step "s2a"	{  SELECT typ, loc, mtim FROM DT , SL , ST WHERE SL.lid = DT.lid AND ST.sid = DT.sid AND mtim >= '2018-12-01 03:00:00+00' AND mtim <= '2018-12-01 04:00:00+00' AND typ = 'T1' ; }
step "s2b"	{ COMMIT; }

