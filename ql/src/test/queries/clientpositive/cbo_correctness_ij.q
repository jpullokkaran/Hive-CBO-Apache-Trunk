drop table if exists t1;
drop table if exists t2;
drop table if exists t3;
drop table if exists t4;
drop table if exists t5;
drop table if exists t6;

create table t1(key string, value string, c_int int, c_float float, c_boolean boolean)  row format delimited fields terminated by ',' STORED AS TEXTFILE;
create table t2(key string, value string, c_int int, c_float float, c_boolean boolean)  row format delimited fields terminated by ',' STORED AS TEXTFILE;
create table t3(key string, value string, c_int int, c_float float, c_boolean boolean)  row format delimited fields terminated by ',' STORED AS TEXTFILE;
create table t4(key string, value string, c_int int, c_float float, c_boolean boolean)  row format delimited fields terminated by ',' STORED AS TEXTFILE;
create table t5(key string, value string, c_int int, c_float float, c_boolean boolean)  row format delimited fields terminated by ',' STORED AS TEXTFILE;
create table t6(key string, value string, c_int int, c_float float, c_boolean boolean)  row format delimited fields terminated by ',' STORED AS TEXTFILE;

load data local inpath '../../data/files/cbo_t1.txt' into table t1;
load data local inpath '../../data/files/cbo_t2.txt' into table t2;
load data local inpath '../../data/files/cbo_t3.txt' into table t3;
load data local inpath '../../data/files/cbo_t4.txt' into table t4;
load data local inpath '../../data/files/cbo_t5.txt' into table t5;
load data local inpath '../../data/files/cbo_t6.txt' into table t6;

set hive.stats.dbclass=jdbc:derby;
analyze table t1 compute statistics;
analyze table t1 compute statistics for columns key, value, c_int, c_int, c_float, c_boolean;
analyze table t2 compute statistics;
analyze table t2 compute statistics for columns key, value, c_int, c_int, c_float, c_boolean;
analyze table t3 compute statistics;
analyze table t3 compute statistics for columns key, value, c_int, c_int, c_float, c_boolean;
analyze table t4 compute statistics;
analyze table t4 compute statistics for columns key, value, c_int, c_int, c_float, c_boolean;
analyze table t5 compute statistics;
analyze table t5 compute statistics for columns key, value, c_int, c_int, c_float, c_boolean;
analyze table t6 compute statistics;
analyze table t6 compute statistics for columns key, value, c_int, c_int, c_float, c_boolean;

set hive.stats.fetch.column.stats=true;

-- Test Basic Inner Joins
select t1.key, t2.value from t1 join t2 on t1.key=t2.key;
select t1.key, t2.value from t1 join t2 on t1.key=t2.key where t1.key > 0;
select t1.key, t2.value from t1 join t2 on t1.key=t2.key where t2.key > 0;
select t1.key, t2.value from t1 join t2 on t1.key=t2.key where t1.key > 0 or t2.key > 0;
select t1.key, t2.value from t1 join t2 on t1.key=t2.key where (t1.key > 0 or t2.key > 0) and (t1.c_float + t2.c_float > 1);
select t1.key, t2.value from t1 join t2 on t1.key=t2.key where (t1.key > 0 or t2.key > 0) or (t1.c_float + t2.c_float > 1);

-- Test Basic Inner Joins with Subqueries
select t1.key, t2.value from (select key, value, c_float from t1)t1 join (select key, value, c_float from t2)t2 on t1.key=t2.key;
select t1.key, t2.value from (select key, value, c_float from t1)t1 join (select key, value, c_float from t2)t2 on t1.key=t2.key where t1.key > 0;
select t1.key, t2.value from (select key, value, c_float from t1)t1 join (select key, value, c_float from t2)t2 on t1.key=t2.key where t2.key > 0;
select t1.key, t2.value from (select key, value, c_float from t1)t1 join (select key, value, c_float from t2)t2 on t1.key=t2.key where t1.key > 0 or t2.key > 0;
select t1.key, t2.value from (select key, value, c_float from t1)t1 join (select key, value, c_float from t2)t2 on t1.key=t2.key where (t1.key > 0 or t2.key > 0) and (t1.c_float + t2.c_float > 1);
select t1.key, t2.value from (select key, value, c_float from t1)t1 join (select key, value, c_float from t2)t2 on t1.key=t2.key where (t1.key > 0 or t2.key > 0) or (t1.c_float + t2.c_float > 1);

-- Test  Basic Inner Joins with Subqueries using predicates, GB, OB, LIMIT
select t1.key, t2.value from (select key, value, c_float from t1 where t1.c_float*10 > c_int group by key, value, c_float order by key limit 10)t1 join (select key, value, c_float from t2 where t2.c_float*10 > c_int  group by key, value, c_float order by key limit 10)t2 on t1.key=t2.key;
select t1.key, t2.value from (select key, value, c_float from t1 where t1.c_float*10 > c_int group by key, value, c_float order by key limit 10)t1 join (select key, value, c_float from t2 where t2.c_float*10 > c_int group by key, value, c_float order by key limit 10)t2 on t1.key=t2.key where t1.key > 0;
select t1.key, t2.value from (select key, value, c_float from t1 where t1.c_float*10 > c_int group by key, value, c_float order by key limit 10)t1 join (select key, value, c_float from t2 where t2.c_float*10 > c_int group by key, value, c_float order by key limit 10)t2 on t1.key=t2.key where t2.key > 0;
select t1.key, t2.value from (select key, value, c_float from t1 where t1.c_float*10 > c_int group by key, value, c_float order by key limit 10)t1 join (select key, value, c_float from t2 where t2.c_float*10 > c_int group by key, value, c_float order by key limit 10)t2 on t1.key=t2.key where t1.key > 0 or t2.key > 0;
select t1.key, t2.value from (select key, value, c_float from t1 where t1.c_float*10 > c_int group by key, value, c_float order by key limit 10)t1 join (select key, value, c_float from t2 where t2.c_float*10 > c_int group by key, value, c_float order by key limit 10)t2 on t1.key=t2.key where (t1.key > 0 or t2.key > 0) and (t1.c_float + t2.c_float > 1);
select t1.key, t2.value from (select key, value, c_float from t1 where t1.c_float*10 > c_int group by key, value, c_float order by key limit 10)t1 join (select key, value, c_float from t2 where t2.c_float*10 > c_int group by key, value, c_float order by key limit 10)t2 on t1.key=t2.key where (t1.key > 0 or t2.key > 0) or (t1.c_float + t2.c_float > 1);

-- Test  Basic Inner Joins with Subqueries + outer queries predicates, GB, OB, LIMIT
select t1.key, t2.value from (select key, value, c_float from t1 where t1.c_float*10 > c_int)t1 join (select key, value, c_float from t2 where t2.c_float*10 > c_int)t2 on t1.key=t2.key group by t1.key, t2.value, t2.c_float order by t1.key limit 10;
select t1.key, t2.value from (select key, value, c_float from t1 where t1.c_float*10 > c_int)t1 join (select key, value, c_float from t2 where t2.c_float*10 > c_int)t2 on t1.key=t2.key where t1.key > 0 group by t1.key, t2.value, t2.c_float order by t1.key limit 10;
select t1.key, t2.value from (select key, value, c_float from t1 where t1.c_float*10 > c_int)t1 join (select key, value, c_float from t2 where t2.c_float*10 > c_int)t2 on t1.key=t2.key where t2.key > 0 group by t1.key, t2.value, t2.c_float order by t1.key limit 10;
select t1.key, t2.value from (select key, value, c_float from t1 where t1.c_float*10 > c_int)t1 join (select key, value, c_float from t2 where t2.c_float*10 > c_int)t2 on t1.key=t2.key where t1.key > 0 or t2.key > 0 group by t1.key, t2.value, t2.c_float order by t1.key limit 10;
select t1.key, t2.value from (select key, value, c_float from t1 where t1.c_float*10 > c_int)t1 join (select key, value, c_float from t2 where t2.c_float*10 > c_int)t2 on t1.key=t2.key where (t1.key > 0 or t2.key > 0) and (t1.c_float + t2.c_float > 1) group by t1.key, t2.value, t2.c_float order by t1.key limit 10;
select t1.key, t2.value from (select key, value, c_float from t1 where t1.c_float*10 > c_int)t1 join (select key, value, c_float from t2 where t2.c_float*10 > c_int)t2 on t1.key=t2.key where (t1.key > 0 or t2.key > 0) or (t1.c_float + t2.c_float > 1) group by t1.key, t2.value, t2.c_float order by t1.key limit 10;

-- Test  Basic Inner Joins with Subqueries using predicates, GB, OB, LIMIT  + outer queries predicates, GB, OB, LIMIT
select t1.key, t2.value from (select key, value, c_float from t1 where t1.c_float*10 > c_int group by key, value, c_float order by key limit 10)t1 join (select key, value, c_float from t2 where t2.c_float*10 > c_int  group by key, value, c_float order by key limit 10)t2 on t1.key=t2.key group by t1.key, t2.value, t2.c_float order by t1.key limit 10;
select t1.key, t2.value from (select key, value, c_float from t1 where t1.c_float*10 > c_int group by key, value, c_float order by key limit 10)t1 join (select key, value, c_float from t2 where t2.c_float*10 > c_int group by key, value, c_float order by key limit 10)t2 on t1.key=t2.key where t1.key > 0 group by t1.key, t2.value, t2.c_float order by t1.key limit 10;
select t1.key, t2.value from (select key, value, c_float from t1 where t1.c_float*10 > c_int group by key, value, c_float order by key limit 10)t1 join (select key, value, c_float from t2 where t2.c_float*10 > c_int group by key, value, c_float order by key limit 10)t2 on t1.key=t2.key where t2.key > 0 group by t1.key, t2.value, t2.c_float order by t1.key limit 10;
select t1.key, t2.value from (select key, value, c_float from t1 where t1.c_float*10 > c_int group by key, value, c_float order by key limit 10)t1 join (select key, value, c_float from t2 where t2.c_float*10 > c_int group by key, value, c_float order by key limit 10)t2 on t1.key=t2.key where t1.key > 0 or t2.key > 0 group by t1.key, t2.value, t2.c_float order by t1.key limit 10;
select t1.key, t2.value from (select key, value, c_float from t1 where t1.c_float*10 > c_int group by key, value, c_float order by key limit 10)t1 join (select key, value, c_float from t2 where t2.c_float*10 > c_int group by key, value, c_float order by key limit 10)t2 on t1.key=t2.key where (t1.key > 0 or t2.key > 0) and (t1.c_float + t2.c_float > 1) group by t1.key, t2.value, t2.c_float order by t1.key limit 10;
select t1.key, t2.value from (select key, value, c_float from t1 where t1.c_float*10 > c_int group by key, value, c_float order by key limit 10)t1 join (select key, value, c_float from t2 where t2.c_float*10 > c_int group by key, value, c_float order by key limit 10)t2 on t1.key=t2.key where (t1.key > 0 or t2.key > 0) or (t1.c_float + t2.c_float > 1) group by t1.key, t2.value, t2.c_float order by t1.key limit 10;


