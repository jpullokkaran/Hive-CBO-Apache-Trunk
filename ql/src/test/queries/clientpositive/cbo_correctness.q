drop table if exists t1;

create table t1(key string, value string, c_int int, c_float float, c_boolean boolean)  row format delimited fields terminated by ',' STORED AS TEXTFILE;

load data local inpath '../../data/files/cbo_t1.txt' into table t1;

set hive.stats.dbclass=jdbc:derby;
analyze table t1 compute statistics;
analyze table t1 compute statistics for columns key, value, c_int, c_float, c_boolean;

set hive.stats.fetch.column.stats=true;

-- 1. Test Select + TS
select * from t1;
select * from t1 as t1;
select * from t1 as t2;

select t1.key as x, c_int as c_int, (((c_int+c_float)*10)+5) as y from t1; 

-- 2. Test Select + TS + FIL
select * from t1 where t1.c_int >= 0;
select * from t1 as t1  where t1.c_int >= 0 and c_float+c_int >= 0 or c_float <= 100;
select * from t1 as t2 where t2.c_int >= 0 and c_float+c_int >= 0 or c_float <= 100;

select t2.key as x, c_int as c_int, (((c_int+c_float)*10)+5) as y from t1 as t2  where t2.c_int >= 0 and c_float+c_int >= 0 or c_float <= 100;

-- 3 Test Select + Select + TS + FIL
select * from (select * from t1 where t1.c_int >= 0) as t1;
select * from (select * from t1 as t1  where t1.c_int >= 0 and c_float+c_int >= 0 or c_float <= 100) as t1;
select * from (select * from t1 as t2 where t2.c_int >= 0 and c_float+c_int >= 0 or c_float <= 100) as t1;
select * from (select t2.key as x, c_int as c_int, (((c_int+c_float)*10)+5) as y from t1 as t2  where t2.c_int >= 0 and c_float+c_int >= 0 or c_float <= 100) as t1;

select * from (select * from t1 where t1.c_int >= 0) as t1 where t1.c_int >= 0;
select * from (select * from t1 as t1  where t1.c_int >= 0 and c_float+c_int >= 0 or c_float <= 100) as t1  where t1.c_int >= 0 and c_float+c_int >= 0 or c_float <= 100;
select * from (select * from t1 as t2 where t2.c_int >= 0 and c_float+c_int >= 0 or c_float <= 100) as t2 where t2.c_int >= 0 and c_float+c_int >= 0 or c_float <= 100;
select * from (select t2.key as x, c_int as c_int, (((c_int+c_float)*10)+5) as y from t1 as t2  where t2.c_int >= 0 and c_float+c_int >= 0 or c_float <= 100) as t1 where t1.c_int >= 0 and y+c_int >= 0 or x <= 100;

select t1.c_int+c_float as x , c_int as c_int, (((c_int+c_float)*10)+5) as y from (select * from t1 where t1.c_int >= 0) as t1 where t1.c_int >= 0;
select t2.c_int+c_float as x , c_int as c_int, (((c_int+c_float)*10)+5) as y from (select * from t1 where t1.c_int >= 0) as t2 where t2.c_int >= 0;

