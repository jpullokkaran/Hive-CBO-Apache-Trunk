create table src_cbo as select * from src;

set hive.stats.dbclass=jdbc:derby;
analyze table src_cbo compute statistics;
analyze table src_cbo compute statistics for columns key,value;

set hive.cbo.enable=true;
set hive.stats.fetch.column.stats=true;



explain
SELECT src_cbo5.src_cbo1_value FROM (SELECT src_cbo3.*, src_cbo4.value as src_cbo4_value, src_cbo4.key as src_cbo4_key FROM src_cbo src_cbo4 JOIN (SELECT src_cbo2.*, src_cbo1.key as src_cbo1_key, src_cbo1.value as src_cbo1_value FROM src_cbo src_cbo1 JOIN src_cbo src_cbo2 ON src_cbo1.key = src_cbo2.key) src_cbo3 ON src_cbo3.src_cbo1_key = src_cbo4.key) src_cbo5;
