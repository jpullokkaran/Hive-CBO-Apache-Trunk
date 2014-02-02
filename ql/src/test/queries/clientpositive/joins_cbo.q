create table over10k(
           t tinyint,
           si smallint,
           i int,
           b bigint,
           f float,
           d double,
           bo boolean,
           s string,
	   ts timestamp, 
           dec decimal(4,2),  
           bin binary)
       row format delimited
       fields terminated by '|';

load data local inpath '../../data/files/over10k' into table over10k;

-- i ranges from 65536 to (65536 + 30)
-- with around 30-40 rows per value.

create table over1k as select * from over10k where i < 65536 + 30;
create table over500 as select * from over10k where i < 65536 + 15;
create table over2k as select * from over10k where i < 65536 + 60;
create table over5k as select * from over10k where i < 65536 + 125;

set hive.stats.dbclass=jdbc:derby;
analyze table over10k compute statistics;
analyze table over10k compute statistics for columns t,si,i,b,f,d,bo,s,ts,bin;
analyze table over1k compute statistics;
analyze table over1k compute statistics for columns t,si,i,b,f,d,bo,s,ts,bin;
analyze table over500 compute statistics;
analyze table over500 compute statistics for columns t,si,i,b,f,d,bo,s,ts,bin;
analyze table over2k compute statistics;
analyze table over2k compute statistics for columns t,si,i,b,f,d,bo,s,ts,bin;
analyze table over5k compute statistics;
analyze table over5k compute statistics for columns t,si,i,b,f,d,bo,s,ts,bin;

set hive.stats.fetch.column.stats=true;
set hive.cbo.enable=true

-- 3 way join on d,f,b,i
explain
select  r1.i, r2.i, r3.i
from over10k r1 join over5k r2 join over500 r3 
on r1.d = r2.d and r1.d = r3.d and
   r1.f = r2.f and r1.f = r3.f and
   r1.bo = r2.bo and r1.bo = r3.bo and
   r1.i = r2.i and r1.i = r3.i
where  r1.i + r2.i + r3.i = (65536) * 3
;

-- 3way join, left is subquery
Explain
SELECT s1.i1,
       s1.i2,
       r3.i
FROM   (SELECT r1.i i1,
               r2.i i2,
               r1.f,
               r1.bo,
               r1.d
        FROM   over10k r1
               JOIN over5k r2
                 ON r1.d = r2.d
                    AND r1.f = r2.f
                    AND r1.bo = r2.bo
                    AND r1.i = r2.i) s1
       JOIN over500 r3
         ON s1.d + 1 = r3.d + 1
            AND s1.f = r3.f
            AND s1.bo = r3.bo
            AND s1.i1 = r3.i
WHERE  s1.i1 + s1.i2 + r3.i = ( 65536 ) * 3 ;

-- 3way join, left and right are subquery
Explain
SELECT s1.i1,
       s1.i2,
       r3.i
FROM   (SELECT r1.i i1,
               r2.i i2,
               r1.f,
               r1.bo,
               r1.d
        FROM   over10k r1
               JOIN over5k r2
                 ON r1.d = r2.d
                    AND r1.f = r2.f
                    AND r1.bo = r2.bo
                    AND r1.i = r2.i) s1
       JOIN (SELECT i,
                    f,
                    d,
                    bo
             FROM   over500) r3
         ON s1.d + 1 = r3.d + 1
            AND s1.f = r3.f
            AND s1.bo = r3.bo
            AND s1.i1 = r3.i
WHERE  s1.i1 + s1.i2 + r3.i = ( 65536 ) * 3 