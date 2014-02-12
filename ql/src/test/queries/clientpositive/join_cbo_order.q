drop table if exists tbl1;
drop table if exists tbl2;
drop table if exists tbl3;
drop table if exists tbl4;

create table tbl1(a_int int, b_int int, c_float float, d_float float)  STORED AS TEXTFILE;
create table tbl2(a_int int, b_int int, c_float float, d_float float)  STORED AS TEXTFILE;
create table tbl3(a_int int, b_int int, c_float float, d_float float)  STORED AS TEXTFILE;
create table tbl4(a_int int, b_int int, c_float float, d_float float)  STORED AS TEXTFILE;

INSERT INTO TABLE tbl1  SELECT 1,1,1,1 FROM src limit 10;

INSERT INTO TABLE tbl2  SELECT 1,1,1,1 FROM src limit 5;
INSERT INTO TABLE tbl2  SELECT 2,2,2,2 FROM src limit 5;

INSERT INTO TABLE tbl3  SELECT 1,1,1,1 FROM src limit 4;
INSERT INTO TABLE tbl3  SELECT 2,2,2,2 FROM src limit 3;
INSERT INTO TABLE tbl3  SELECT 3,3,3,3 FROM src limit 3;

INSERT INTO TABLE tbl4  SELECT 1,1,1,1 FROM src limit 3;
INSERT INTO TABLE tbl4  SELECT 2,2,2,2 FROM src limit 3;
INSERT INTO TABLE tbl4  SELECT 3,3,3,3 FROM src limit 2;
INSERT INTO TABLE tbl4  SELECT 4,4,4,4 FROM src limit 2;

set hive.stats.dbclass=jdbc:derby;
analyze table tbl1 compute statistics;
analyze table tbl1 compute statistics for columns a_int, b_int, c_float, d_float;
analyze table tbl2 compute statistics;
analyze table tbl2 compute statistics for columns a_int, b_int, c_float, d_float;
analyze table tbl3 compute statistics;
analyze table tbl3 compute statistics for columns a_int, b_int, c_float, d_float;
analyze table tbl4 compute statistics;
analyze table tbl4 compute statistics for columns a_int, b_int, c_float, d_float;

set hive.cbo.enable=true;
set hive.auto.convert.join=false;
set hive.auto.convert.join.noconditionaltask.size=0;
set hive.stats.fetch.column.stats=true;


-- Simple 3-way join
explain select * from tbl1 join tbl2 on tbl1.a_int = tbl2.a_int join tbl3 on tbl1.a_int=tbl3.a_int;

-- Simple 4-way join
explain select * from tbl1 join tbl2 on tbl1.a_int = tbl2.a_int join tbl3 on tbl1.a_int=tbl3.a_int join tbl4 on tbl1.a_int=tbl4.a_int;

-- 3-way join + filter(<)
explain select * from tbl1 join (select * from tbl2 where  tbl2.b_int < 100 ) tbl2 on tbl1.a_int = tbl2.a_int join tbl3 on tbl1.a_int=tbl3.a_int;

-- 3-way join + GB
explain select * from tbl1 join (select a_int, b_int from tbl2 group by a_int, b_int) tbl2 on tbl1.a_int = tbl2.a_int join tbl3 on tbl1.a_int=tbl3.a_int;

