--Medical
drop table opt_20210916.vis;
create table opt_20210916.vis (
PTID varchar,VISITID varchar,VISIT_TYPE varchar,VISIT_START_DATE date,VISIT_START_TIME time,VISIT_END_DATE date,VISIT_END_TIME time,
DISCHARGE_DISPOSITION varchar,ADMISSION_SOURCE varchar,DRG varchar,SOURCEID varchar
) 
WITH (appendonly=true, orientation=column, compresstype=zlib)
distributed by (PTID);

-- NOTE: Load each year 1 by one updating the gpfdist string and hard coded YEAR value in visert statement.

drop external table ext_covid_vis;
CREATE EXTERNAL TABLE ext_covid_vis (
PTID varchar,VISITID varchar,VISIT_TYPE varchar,VISIT_START_DATE date,VISIT_START_TIME time,VISIT_END_DATE date,VISIT_END_TIME time,
DISCHARGE_DISPOSITION varchar,ADMISSION_SOURCE varchar,DRG varchar,SOURCEID varchar
) 
LOCATION ( 
'gpfdist://greenplum01:8081/covid/20210916/*vis*.txt.gz'
)
FORMAT 'text' ( HEADER DELIMITER '|' null as '' escape 'OFF');

-- Test
/*
select *
from ext_covid_vis
limit 1000;
*/
-- visert: 7s, Updated Rows	2902137
insert into opt_20210916.vis
select * from ext_covid_vis;

--Scratch
select count(*)
from opt_20210916.vis
group by 1
order by 1;