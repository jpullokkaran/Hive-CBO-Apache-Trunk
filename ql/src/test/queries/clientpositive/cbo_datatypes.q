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

-- set hive.stats.dbclass=jdbc:derby;
analyze table over10k compute statistics;
analyze table over10k compute statistics for columns t,si,i,b,f,d,bo,s;
analyze table over1k compute statistics;
analyze table over1k compute statistics for columns t,si,i,b,f,d,bo,s;
analyze table over500 compute statistics;
analyze table over500 compute statistics for columns t,si,i,b,f,d,bo,s;
analyze table over2k compute statistics;
analyze table over2k compute statistics for columns t,si,i,b,f,d,bo,s;
analyze table over5k compute statistics;
analyze table over5k compute statistics for columns t,si,i,b,f,d,bo,s;

set hive.stats.fetch.column.stats=true;

-- 3 way join on d,f,b,i
select  r1.i, r2.i, r3.i
from over10k r1 join over5k r2 join over500 r3 
on r1.d = r2.d and r1.d = r3.d and
   r1.f = r2.f and r1.f = r3.f and
   r1.bo = r2.bo and r1.bo = r3.bo and
   r1.i = r2.i and r1.i = r3.i
where  r1.i + r2.i + r3.i = (65536) * 3
;

-- 4 way join on compound, typecasting
select r1.i, r2.i, r3.i, r4.i
from over10k r1 join over5k r2 join over500 r3 join over2k r4 
on r1.f = r2.f and r1.d = r3.d and r2.s = r4.s and round(r1.f) = round(r4.f)
where r1.i + r2.i + r3.i + r4.i = (65536) * 4
;

-- mixed types in join cond, filter
select  r1.i, r2.i, r2.d + 65659 
from over10k r1 join over5k r2  on r1.i = round(r2.d + 65600) 
where r2.i > r2.d + 65659
;

-- group by
select r1.i, round(f/2), count(*), sum(d), avg(t), sum (d + f) 
from over10k r1   
where i = 65726 group by i, round(f/2)
;

-- compound join condition
select t1.i, t1.d, t2.i, t2.d
from over1k t1 join over1k t2 on ((t1.i+t1.d)/10)=((t2.i+t2.d)/10)
where t1.i + t2.i = (65536) * 2 and t1.d + t2.d < 3
;