create table src_cbo as select * from src;

set hive.stats.dbclass=jdbc:derby;
analyze table src_cbo compute statistics;
analyze table src_cbo compute statistics for columns key,value;

set hive.cbo.enable=true;
set hive.stats.fetch.column.stats=true;

EXPLAIN
SELECT src1.key as k1, src1.value as v1, 
       src2.key as k2, src2.value as v2 FROM 
  (SELECT * FROM src_cbo WHERE src_cbo.key < 10) src1 
    JOIN 
  (SELECT * FROM src_cbo WHERE src_cbo.key < 10) src2
  SORT BY k1, v1, k2, v2;

EXPLAIN FORMATTED
SELECT src1.key as k1, src1.value as v1, 
       src2.key as k2, src2.value as v2 FROM 
  (SELECT * FROM src_cbo WHERE src_cbo.key < 10) src1 
    JOIN 
  (SELECT * FROM src_cbo WHERE src_cbo.key < 10) src2
  SORT BY k1, v1, k2, v2;

SELECT src1.key as k1, src1.value as v1, 
       src2.key as k2, src2.value as v2 FROM 
  (SELECT * FROM src_cbo WHERE src_cbo.key < 10) src1 
    JOIN 
  (SELECT * FROM src_cbo WHERE src_cbo.key < 10) src2
  SORT BY k1, v1, k2, v2;
