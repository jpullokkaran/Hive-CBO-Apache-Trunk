drop table if exists t1;
drop table if exists t2;
drop table if exists t3;
drop table if exists t4;

create table t1(key string, value string, c_int int, c_float float, c_boolean boolean)  row format delimited fields terminated by ',' STORED AS TEXTFILE;
create table t2(key string, value string, c_int int, c_float float, c_boolean boolean)  row format delimited fields terminated by ',' STORED AS TEXTFILE;
create table t3(key string, value string, c_int int, c_float float, c_boolean boolean)  row format delimited fields terminated by ',' STORED AS TEXTFILE;
create table t4(key string, value string, c_int int, c_float float, c_boolean boolean)  row format delimited fields terminated by ',' STORED AS TEXTFILE;

load data local inpath '../../data/files/cbo_t1.txt' into table t1;
load data local inpath '../../data/files/cbo_t2.txt' into table t2;
load data local inpath '../../data/files/cbo_t3.txt' into table t3;
load data local inpath '../../data/files/cbo_t4.txt' into table t4;

set hive.stats.dbclass=jdbc:derby;
analyze table t1 compute statistics;
analyze table t1 compute statistics for columns key, value, c_int, c_float, c_boolean;
analyze table t2 compute statistics;
analyze table t2 compute statistics for columns key, value, c_int, c_float, c_boolean;
analyze table t3 compute statistics;
analyze table t3 compute statistics for columns key, value, c_int, c_float, c_boolean;
analyze table t4 compute statistics;
analyze table t4 compute statistics for columns key, value, c_int, c_float, c_boolean;

set hive.stats.fetch.column.stats=true;
set hive.auto.convert.join=false;

select c, b, a from (select key as a, c_int as b, t1.c_float as c from t1) t1 left semi join (select t2.key as p, t2.c_int as q, c_float as r from t2) t2 on t1.a=p left semi join t3 on t1.a=t3.key;
