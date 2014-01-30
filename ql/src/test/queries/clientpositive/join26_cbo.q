CREATE TABLE dest_j1(key STRING, value STRING, val2 STRING) STORED AS TEXTFILE;

create table src_cbo as select * from src;
create table src_cbo1 as select * from src1;
create table src_cbopart as select * from srcpart;

set hive.stats.dbclass=jdbc:derby;
analyze table src_cbo compute statistics;
analyze table src_cbo compute statistics for columns key,value;
analyze table src_cbo1 compute statistics;
analyze table src_cbo1 compute statistics for columns key,value;
analyze table src_cbopart compute statistics;
analyze table src_cbopart compute statistics for columns key,value,ds,hr;

set hive.cbo.enable=true;
set hive.stats.fetch.column.stats=true;


EXPLAIN EXTENDED
INSERT OVERWRITE TABLE dest_j1
SELECT /*+ MAPJOIN(x,y) */ x.key, z.value, y.value
FROM src_cbo1 x JOIN src_cbo y ON (x.key = y.key) 
JOIN src_cbopart z ON (x.key = z.key and z.ds='2008-04-08' and z.hr=11);

INSERT OVERWRITE TABLE dest_j1
SELECT /*+ MAPJOIN(x,y) */ x.key, z.value, y.value
FROM src_cbo1 x JOIN src_cbo y ON (x.key = y.key) 
JOIN src_cbopart z ON (x.key = z.key and z.ds='2008-04-08' and z.hr=11);

select * from dest_j1 x order by x.key;

