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

-- 4. Test Select + Join + TS
select t1.c_int, t2.c_int from t1 join             t2 on t1.key=t2.key;
select t1.c_int           from t1 left semi join   t2 on t1.key=t2.key;
select t1.c_int, t2.c_int from t1 left outer join  t2 on t1.key=t2.key;
select t1.c_int, t2.c_int from t1 right outer join t2 on t1.key=t2.key;
select t1.c_int, t2.c_int from t1 full outer join  t2 on t1.key=t2.key;

select b, t1.c, t2.p, q, t3.c_int from (select key as a, c_int as b, t1.c_float as c from t1) t1 join (select t2.key as p, t2.c_int as q, c_float as r from t2) t2 on t1.a=p join t3 on t1.a=key;
select key, t1.c_int, t2.p, q from t1 join (select t2.key as p, t2.c_int as q, c_float as r from t2) t2 on t1.key=p join (select key as a, c_int as b, t3.c_float as c from t3)t3 on t1.key=a;
select a, t1.b, key, t2.c_int, t3.p from (select key as a, c_int as b, t1.c_float as c from t1) t1 join t2  on t1.a=key join (select key as p, c_int as q, t3.c_float as r from t3)t3 on t1.a=t3.p;
select b, t1.c, t2.c_int, t3.c_int from (select key as a, c_int as b, t1.c_float as c from t1) t1 join t2 on t1.a=t2.key join t3 on t1.a=t3.key;
select t3.c_int, b, t2.c_int, t1.c from (select key as a, c_int as b, t1.c_float as c from t1) t1 join t2 on t1.a=t2.key join t3 on t1.a=t3.key;

select b, t1.c, t2.p, q, t3.c_int from (select key as a, c_int as b, t1.c_float as c from t1) t1 left outer join (select t2.key as p, t2.c_int as q, c_float as r from t2) t2 on t1.a=p join t3 on t1.a=key;
select key, t1.c_int, t2.p, q from t1 join (select t2.key as p, t2.c_int as q, c_float as r from t2) t2 on t1.key=p left outer join (select key as a, c_int as b, t3.c_float as c from t3)t3 on t1.key=a;

select b, t1.c, t2.p, q, t3.c_int from (select key as a, c_int as b, t1.c_float as c from t1) t1 right outer join (select t2.key as p, t2.c_int as q, c_float as r from t2) t2 on t1.a=p join t3 on t1.a=key;
select key, t1.c_int, t2.p, q from t1 join (select t2.key as p, t2.c_int as q, c_float as r from t2) t2 on t1.key=p right outer join (select key as a, c_int as b, t3.c_float as c from t3)t3 on t1.key=a;

select b, t1.c, t2.p, q, t3.c_int from (select key as a, c_int as b, t1.c_float as c from t1) t1 full outer join (select t2.key as p, t2.c_int as q, c_float as r from t2) t2 on t1.a=p join t3 on t1.a=key;
select key, t1.c_int, t2.p, q from t1 join (select t2.key as p, t2.c_int as q, c_float as r from t2) t2 on t1.key=p full outer join (select key as a, c_int as b, t3.c_float as c from t3)t3 on t1.key=a;

-- 5. Test Select + Join + FIL + TS
select t1.c_int, t2.c_int from t1 join             t2 on t1.key=t2.key where (t1.c_int + t2.c_int == 2) and (t1.c_int > 0 or t2.c_float >= 0);
select t1.c_int           from t1 left semi join   t2 on t1.key=t2.key where (t1.c_int + 1 == 2) and (t1.c_int > 0 or t1.c_float >= 0);
select t1.c_int, t2.c_int from t1 left outer join  t2 on t1.key=t2.key where (t1.c_int + t2.c_int == 2) and (t1.c_int > 0 or t2.c_float >= 0);
select t1.c_int, t2.c_int from t1 right outer join t2 on t1.key=t2.key where (t1.c_int + t2.c_int == 2) and (t1.c_int > 0 or t2.c_float >= 0);
select t1.c_int, t2.c_int from t1 full outer join  t2 on t1.key=t2.key where (t1.c_int + t2.c_int == 2) and (t1.c_int > 0 or t2.c_float >= 0);

select b, t1.c, t2.p, q, t3.c_int from (select key as a, c_int as b, t1.c_float as c from t1  where (t1.c_int + 1 == 2) and (t1.c_int > 0 or t1.c_float >= 0)) t1 join (select t2.key as p, t2.c_int as q, c_float as r from t2  where (t2.c_int + 1 == 2) and (t2.c_int > 0 or t2.c_float >= 0)) t2 on t1.a=p join t3 on t1.a=key where (b + t2.q == 2) and (b > 0 or t2.q >= 0);

select q, b, t2.p, t1.c, t3.c_int from (select key as a, c_int as b, t1.c_float as c from t1  where (t1.c_int + 1 == 2) and (t1.c_int > 0 or t1.c_float >= 0)) t1 left outer join (select t2.key as p, t2.c_int as q, c_float as r from t2  where (t2.c_int + 1 == 2) and (t2.c_int > 0 or t2.c_float >= 0)) t2 on t1.a=p join t3 on t1.a=key where (b + t2.q == 2) and (b > 0 or c_int >= 0);



select q, b, t2.p, t1.c, t3.c_int from (select key as a, c_int as b, t1.c_float as c from t1  where (t1.c_int + 1 == 2) and (t1.c_int > 0 or t1.c_float >= 0)) t1 right outer join (select t2.key as p, t2.c_int as q, c_float as r from t2  where (t2.c_int + 1 == 2) and (t2.c_int > 0 or t2.c_float >= 0)) t2 on t1.a=p join t3 on t1.a=key where (b + t2.q == 2) and (b > 0 or c_int >= 0);

select q, b, t2.p, t1.c, t3.c_int from (select key as a, c_int as b, t1.c_float as c from t1  where (t1.c_int + 1 == 2) and (t1.c_int > 0 or t1.c_float >= 0)) t1 full outer join (select t2.key as p, t2.c_int as q, c_float as r from t2  where (t2.c_int + 1 == 2) and (t2.c_int > 0 or t2.c_float >= 0)) t2 on t1.a=p join t3 on t1.a=key where (b + t2.q == 2) and (b > 0 or c_int >= 0);

select * from (select q, b, t2.p, t1.c, t3.c_int from (select key as a, c_int as b, t1.c_float as c from t1  where (t1.c_int + 1 == 2) and (t1.c_int > 0 or t1.c_float >= 0)) t1 full outer join (select t2.key as p, t2.c_int as q, c_float as r from t2  where (t2.c_int + 1 == 2) and (t2.c_int > 0 or t2.c_float >= 0)) t2 on t1.a=p join t3 on t1.a=key where (b + t2.q == 2) and (b > 0 or c_int >= 0)) R where  (q + 1 = 2) and (R.b > 0 or c_int >= 0);



select * from (select c, b, a from (select key as a, c_int as b, t1.c_float as c from t1  where (t1.c_int + 1 == 2) and (t1.c_int > 0 or t1.c_float >= 0)) t1 left semi join (select t2.key as p, t2.c_int as q, c_float as r from t2  where (t2.c_int + 1 == 2) and (t2.c_int > 0 or t2.c_float >= 0)) t2 on t1.a=p left semi join t3 on t1.a=key where (b + 1 == 2) and (b > 0 or c >= 0)) R where  (b + 1 = 2) and (R.b > 0 or c >= 0);

select * from (select t3.c_int, t1.c, b from (select key as a, c_int as b, t1.c_float as c from t1  where (t1.c_int + 1 = 2) and (t1.c_int > 0 or t1.c_float >= 0)) t1 left semi join (select t2.key as p, t2.c_int as q, c_float as r from t2  where (t2.c_int + 1 == 2) and (t2.c_int > 0 or t2.c_float >= 0)) t2 on t1.a=p left outer join t3 on t1.a=key where (b + t3.c_int  == 2) and (b > 0 or c_int >= 0)) R where  (R.c_int + 1 = 2) and (R.b > 0 or c_int >= 0);

select * from (select c_int, b, t1.c from (select key as a, c_int as b, t1.c_float as c from t1  where (t1.c_int + 1 == 2) and (t1.c_int > 0 or t1.c_float >= 0)) t1 left semi join (select t2.key as p, t2.c_int as q, c_float as r from t2  where (t2.c_int + 1 == 2) and (t2.c_int > 0 or t2.c_float >= 0)) t2 on t1.a=p right outer join t3 on t1.a=key where (b + 1 == 2) and (b > 0 or c_int >= 0)) R where  (c + 1 = 2) and (R.b > 0 or c_int >= 0);

select * from (select c_int, b, t1.c from (select key as a, c_int as b, t1.c_float as c from t1  where (t1.c_int + 1 == 2) and (t1.c_int > 0 or t1.c_float >= 0)) t1 left semi join (select t2.key as p, t2.c_int as q, c_float as r from t2  where (t2.c_int + 1 == 2) and (t2.c_int > 0 or t2.c_float >= 0)) t2 on t1.a=p full outer join t3 on t1.a=key where (b + 1 == 2) and (b > 0 or c_int >= 0)) R where  (c + 1 = 2) and (R.b > 0 or c_int >= 0);



select * from (select q, b, t2.p, t1.c, t3.c_int from (select key as a, c_int as b, t1.c_float as c from t1  where (t1.c_int + 1 == 2) and (t1.c_int > 0 or t1.c_float >= 0)) t1 left outer join (select t2.key as p, t2.c_int as q, c_float as r from t2  where (t2.c_int + 1 == 2) and (t2.c_int > 0 or t2.c_float >= 0)) t2 on t1.a=p left outer join t3 on t1.a=key where (b + t2.q == 2) and (b > 0 or c_int >= 0)) R where  (q + 1 = 2) and (R.b > 0 or c_int >= 0);

select * from (select q, b, t2.p, t1.c, t3.c_int from (select key as a, c_int as b, t1.c_float as c from t1  where (t1.c_int + 1 == 2) and (t1.c_int > 0 or t1.c_float >= 0)) t1 left outer join (select t2.key as p, t2.c_int as q, c_float as r from t2  where (t2.c_int + 1 == 2) and (t2.c_int > 0 or t2.c_float >= 0)) t2 on t1.a=p right outer join t3 on t1.a=key where (b + t2.q == 2) and (b > 0 or c_int >= 0)) R where  (q + 1 = 2) and (R.b > 0 or c_int >= 0);

select * from (select q, b, t2.p, t1.c, t3.c_int from (select key as a, c_int as b, t1.c_float as c from t1  where (t1.c_int + 1 == 2) and (t1.c_int > 0 or t1.c_float >= 0)) t1 left outer join (select t2.key as p, t2.c_int as q, c_float as r from t2  where (t2.c_int + 1 == 2) and (t2.c_int > 0 or t2.c_float >= 0)) t2 on t1.a=p full outer join t3 on t1.a=key where (b + t2.q == 2) and (b > 0 or c_int >= 0)) R where  (q + 1 = 2) and (R.b > 0 or c_int >= 0);



select * from (select q, b, t2.p, t1.c, t3.c_int from (select key as a, c_int as b, t1.c_float as c from t1  where (t1.c_int + 1 == 2) and (t1.c_int > 0 or t1.c_float >= 0)) t1 right outer join (select t2.key as p, t2.c_int as q, c_float as r from t2  where (t2.c_int + 1 == 2) and (t2.c_int > 0 or t2.c_float >= 0)) t2 on t1.a=p right outer join t3 on t1.a=key where (b + t2.q == 2) and (b > 0 or c_int >= 0)) R where  (q + 1 = 2) and (R.b > 0 or c_int >= 0);

select * from (select q, b, t2.p, t1.c, t3.c_int from (select key as a, c_int as b, t1.c_float as c from t1  where (t1.c_int + 1 == 2) and (t1.c_int > 0 or t1.c_float >= 0)) t1 right outer join (select t2.key as p, t2.c_int as q, c_float as r from t2  where (t2.c_int + 1 == 2) and (t2.c_int > 0 or t2.c_float >= 0)) t2 on t1.a=p left outer join t3 on t1.a=key where (b + t2.q == 2) and (b > 0 or c_int >= 0)) R where  (q + 1 = 2) and (R.b > 0 or c_int >= 0);

select * from (select q, b, t2.p, t1.c, t3.c_int from (select key as a, c_int as b, t1.c_float as c from t1  where (t1.c_int + 1 == 2) and (t1.c_int > 0 or t1.c_float >= 0)) t1 right outer join (select t2.key as p, t2.c_int as q, c_float as r from t2  where (t2.c_int + 1 == 2) and (t2.c_int > 0 or t2.c_float >= 0)) t2 on t1.a=p full outer join t3 on t1.a=key where (b + t2.q == 2) and (b > 0 or c_int >= 0)) R where  (q + 1 = 2) and (R.b > 0 or c_int >= 0);



select * from (select q, b, t2.p, t1.c, t3.c_int from (select key as a, c_int as b, t1.c_float as c from t1  where (t1.c_int + 1 == 2) and (t1.c_int > 0 or t1.c_float >= 0)) t1 full outer join (select t2.key as p, t2.c_int as q, c_float as r from t2  where (t2.c_int + 1 == 2) and (t2.c_int > 0 or t2.c_float >= 0)) t2 on t1.a=p full outer join t3 on t1.a=key where (b + t2.q == 2) and (b > 0 or c_int >= 0)) R where  (q + 1 = 2) and (R.b > 0 or c_int >= 0);

select * from (select q, b, t2.p, t1.c, t3.c_int from (select key as a, c_int as b, t1.c_float as c from t1  where (t1.c_int + 1 == 2) and (t1.c_int > 0 or t1.c_float >= 0)) t1 full outer join (select t2.key as p, t2.c_int as q, c_float as r from t2  where (t2.c_int + 1 == 2) and (t2.c_int > 0 or t2.c_float >= 0)) t2 on t1.a=p left outer join t3 on t1.a=key where (b + t2.q == 2) and (b > 0 or c_int >= 0)) R where  (q + 1 = 2) and (R.b > 0 or c_int >= 0);

select * from (select q, b, t2.p, t1.c, t3.c_int from (select key as a, c_int as b, t1.c_float as c from t1  where (t1.c_int + 1 == 2) and (t1.c_int > 0 or t1.c_float >= 0)) t1 full outer join (select t2.key as p, t2.c_int as q, c_float as r from t2  where (t2.c_int + 1 == 2) and (t2.c_int > 0 or t2.c_float >= 0)) t2 on t1.a=p right outer join t3 on t1.a=key where (b + t2.q == 2) and (b > 0 or c_int >= 0)) R where  (q + 1 = 2) and (R.b > 0 or c_int >= 0);


-- 6. Test Select + TS + Join + Fil + GB + GB Having
select * from t1 group by c_int;
select key, (c_int+1)+2 as x, sum(c_int) from t1 group by c_float, t1.c_int, key;
select * from (select key, (c_int+c_float+1+2) as x, sum(c_int) as y from t1 group by c_float, t1.c_int, key) R group by y, x;

select * from (select key as a, c_int+1 as b, sum(c_int) as c from t1 where (t1.c_int + 1 >= 0) and (t1.c_int > 0 or t1.c_float >= 0) group by c_float, t1.c_int, key order by a) t1 join (select key as p, c_int+1 as q, sum(c_int) as r from t2 where (t2.c_int + 1 >= 0) and (t2.c_int > 0 or t2.c_float >= 0)  group by c_float, t2.c_int, key order by q/10 desc, r asc) t2 on t1.a=p join t3 on t1.a=key where (b + t2.q >= 0) and (b > 0 or c_int >= 0) group by t3.c_int, c order by t3.c_int+c desc, c;

select * from (select key as a, c_int+1 as b, sum(c_int) as c from t1 where (t1.c_int + 1 >= 0) and (t1.c_int > 0 or t1.c_float >= 0)  group by c_float, t1.c_int, key having t1.c_float > 0 and (c_int >=1 or c_float >= 1) and (c_int + c_float) >= 0 order by a+b desc, c asc) t1 left semi join (select key as p, c_int+1 as q, sum(c_int) as r from t2 where (t2.c_int + 1 >= 0) and (t2.c_int > 0 or t2.c_float >= 0)  group by c_float, t2.c_int, key having t2.c_float > 0 and (c_int >=1 or c_float >= 1) and (c_int + c_float) >= 0 order by q+r/10 desc, p) t2 on t1.a=p left semi join t3 on t1.a=key where (b + 1  >= 0) and (b > 0 or a >= 0) group by a, c  having a > 0 and (a >=1 or c >= 1) and (a + c) >= 0 order by c, a;

select * from (select key as a, c_int+1 as b, sum(c_int) as c from t1 where (t1.c_int + 1 >= 0) and (t1.c_int > 0 or t1.c_float >= 0)  group by c_float, t1.c_int, key having t1.c_float > 0 and (c_int >=1 or c_float >= 1) and (c_int + c_float) >= 0 order by b % c asc, b desc) t1 left outer join (select key as p, c_int+1 as q, sum(c_int) as r from t2 where (t2.c_int + 1 >= 0) and (t2.c_int > 0 or t2.c_float >= 0)  group by c_float, t2.c_int, key  having t2.c_float > 0 and (c_int >=1 or c_float >= 1) and (c_int + c_float) >= 0) t2 on t1.a=p left outer join t3 on t1.a=key where (b + t2.q >= 0) and (b > 0 or c_int >= 0) group by t3.c_int, c  having t3.c_int > 0 and (c_int >=1 or c >= 1) and (c_int + c) >= 0  order by t3.c_int % c asc, t3.c_int desc;

select * from (select key as a, c_int+1 as b, sum(c_int) as c from t1 where (t1.c_int + 1 >= 0) and (t1.c_int > 0 or t1.c_float >= 0)  group by c_float, t1.c_int, key having t1.c_float > 0 and (c_int >=1 or c_float >= 1) and (c_int + c_float) >= 0 order by b+c, a desc) t1 right outer join (select key as p, c_int+1 as q, sum(c_int) as r from t2 where (t2.c_int + 1 >= 0) and (t2.c_int > 0 or t2.c_float >= 0)  group by c_float, t2.c_int, key having t2.c_float > 0 and (c_int >=1 or c_float >= 1) and (c_int + c_float) >= 0) t2 on t1.a=p right outer join t3 on t1.a=key where (b + t2.q >= 2) and (b > 0 or c_int >= 0) group by t3.c_int, c;

select * from (select key as a, c_int+1 as b, sum(c_int) as c from t1 where (t1.c_int + 1 >= 0) and (t1.c_int > 0 or t1.c_float >= 0)  group by c_float, t1.c_int, key having t1.c_float > 0 and (c_int >=1 or c_float >= 1) and (c_int + c_float) >= 0 order by c+a desc) t1 full outer join (select key as p, c_int+1 as q, sum(c_int) as r from t2 where (t2.c_int + 1 >= 0) and (t2.c_int > 0 or t2.c_float >= 0)  group by c_float, t2.c_int, key having t2.c_float > 0 and (c_int >=1 or c_float >= 1) and (c_int + c_float) >= 0 order by p+q desc, r asc) t2 on t1.a=p full outer join t3 on t1.a=key where (b + t2.q >= 0) and (b > 0 or c_int >= 0) group by t3.c_int, c having t3.c_int > 0 and (c_int >=1 or c >= 1) and (c_int + c) >= 0 order by t3.c_int;

select * from (select key as a, c_int+1 as b, sum(c_int) as c from t1 where (t1.c_int + 1 >= 0) and (t1.c_int > 0 or t1.c_float >= 0)  group by c_float, t1.c_int, key having t1.c_float > 0 and (c_int >=1 or c_float >= 1) and (c_int + c_float) >= 0) t1 join (select key as p, c_int+1 as q, sum(c_int) as r from t2 where (t2.c_int + 1 >= 0) and (t2.c_int > 0 or t2.c_float >= 0)  group by c_float, t2.c_int, key having t2.c_float > 0 and (c_int >=1 or c_float >= 1) and (c_int + c_float) >= 0) t2 on t1.a=p join t3 on t1.a=key where (b + t2.q >= 0) and (b > 0 or c_int >= 0) group by t3.c_int, c;

-- 7. Test Select + TS + Join + Fil + GB + GB Having + Limit
select * from t1 group by c_int limit 1;
select key, (c_int+1)+2 as x, sum(c_int) from t1 group by c_float, t1.c_int, key order by x limit 1;
select * from (select key, (c_int+c_float+1+2) as x, sum(c_int) as y from t1 group by c_float, t1.c_int, key) R group by y, x order by x,y limit 1;
select key from(select key from (select key from t1 limit 5)t2  limit 5)t3  limit 5;
select key, c_int from(select key, c_int from (select key, c_int from t1 order by c_int limit 5)t1  order by c_int limit 5)t2  order by c_int limit 5;

select * from (select key as a, c_int+1 as b, sum(c_int) as c from t1 where (t1.c_int + 1 >= 0) and (t1.c_int > 0 or t1.c_float >= 0) group by c_float, t1.c_int, key order by a limit 5) t1 join (select key as p, c_int+1 as q, sum(c_int) as r from t2 where (t2.c_int + 1 >= 0) and (t2.c_int > 0 or t2.c_float >= 0)  group by c_float, t2.c_int, key order by q/10 desc, r asc limit 5) t2 on t1.a=p join t3 on t1.a=key where (b + t2.q >= 0) and (b > 0 or c_int >= 0) group by t3.c_int, c order by t3.c_int+c desc, c limit 5;

select * from (select key as a, c_int+1 as b, sum(c_int) as c from t1 where (t1.c_int + 1 >= 0) and (t1.c_int > 0 or t1.c_float >= 0)  group by c_float, t1.c_int, key having t1.c_float > 0 and (c_int >=1 or c_float >= 1) and (c_int + c_float) >= 0 order by a+b desc, c asc limit 5) t1 left semi join (select key as p, c_int+1 as q, sum(c_int) as r from t2 where (t2.c_int + 1 >= 0) and (t2.c_int > 0 or t2.c_float >= 0)  group by c_float, t2.c_int, key having t2.c_float > 0 and (c_int >=1 or c_float >= 1) and (c_int + c_float) >= 0 order by q+r/10 desc, p limit 5) t2 on t1.a=p left semi join t3 on t1.a=key where (b + 1  >= 0) and (b > 0 or a >= 0) group by a, c  having a > 0 and (a >=1 or c >= 1) and (a + c) >= 0 order by c, a;

select * from (select key as a, c_int+1 as b, sum(c_int) as c from t1 where (t1.c_int + 1 >= 0) and (t1.c_int > 0 or t1.c_float >= 0)  group by c_float, t1.c_int, key having t1.c_float > 0 and (c_int >=1 or c_float >= 1) and (c_int + c_float) >= 0 order by b % c asc, b desc limit 5) t1 left outer join (select key as p, c_int+1 as q, sum(c_int) as r from t2 where (t2.c_int + 1 >= 0) and (t2.c_int > 0 or t2.c_float >= 0)  group by c_float, t2.c_int, key  having t2.c_float > 0 and (c_int >=1 or c_float >= 1) and (c_int + c_float) >= 0 limit 5) t2 on t1.a=p left outer join t3 on t1.a=key where (b + t2.q >= 0) and (b > 0 or c_int >= 0) group by t3.c_int, c  having t3.c_int > 0 and (c_int >=1 or c >= 1) and (c_int + c) >= 0  order by t3.c_int % c asc, t3.c_int desc limit 5;

