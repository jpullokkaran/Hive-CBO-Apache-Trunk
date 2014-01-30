create table src_cbo as select * from src;

set hive.stats.dbclass=jdbc:derby;
analyze table src_cbo compute statistics;
analyze table src_cbo compute statistics for columns key,value;

set hive.cbo.enable=true;
set hive.stats.fetch.column.stats=true;


EXPLAIN
SELECT src1.c1, src2.c4 
FROM
(SELECT src_cbo.key as c1, src_cbo.value as c2 from src_cbo) src1
JOIN
(SELECT src_cbo.key as c3, src_cbo.value as c4 from src_cbo) src2
ON src1.c1 = src2.c3 AND src1.c1 < 100
JOIN
(SELECT src_cbo.key as c5, src_cbo.value as c6 from src_cbo) src3
ON src1.c1 = src3.c5 AND src3.c5 < 80;

SELECT src1.c1, src2.c4 
FROM
(SELECT src_cbo.key as c1, src_cbo.value as c2 from src_cbo) src1
JOIN
(SELECT src_cbo.key as c3, src_cbo.value as c4 from src_cbo) src2
ON src1.c1 = src2.c3 AND src1.c1 < 100
JOIN
(SELECT src_cbo.key as c5, src_cbo.value as c6 from src_cbo) src3
ON src1.c1 = src3.c5 AND src3.c5 < 80;
