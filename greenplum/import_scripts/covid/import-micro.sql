--Medical
drop table opt_20210401.micro;
create table opt_20210401.micro (
PTID varchar,ENCID varchar,ORDER_DATE varchar,ORDER_TIME varchar,
COLLECT_DATE date,COLLECT_TIME time,RECEIVE_DATE date,RECEIVE_TIME time,RESULT_DATE date,RESULT_TIME time,RESULT_STATUS varchar,
SPECIMEN_SOURCE varchar,ORGANISM varchar,MAPPED_ORGANISM_FOUND varchar,MAPPED_ORGANISM_EXCLUDED varchar,CULTURE_GROWTH varchar,CULTURE_VALUE varchar,CULTURE_UNIT varchar,
ANTIBIOTIC varchar,MAPPED_ANTIBIOTIC varchar,SENSITIVITY varchar,SOURCEID varchar

) 
WITH (appendonly=true, orientation=column, compresstype=zlib)
distributed by (ptid);

-- NOTE: Load each year 1 by one updating the gpfdist string and hard coded YEAR value in microert statement.

drop external table ext_covid_micro;
CREATE EXTERNAL TABLE ext_covid_micro (
PTID varchar,ENCID varchar,ORDER_DATE varchar,ORDER_TIME varchar,
COLLECT_DATE date,COLLECT_TIME time,RECEIVE_DATE date,RECEIVE_TIME time,RESULT_DATE date,RESULT_TIME time,RESULT_STATUS varchar,
SPECIMEN_SOURCE varchar,ORGANISM varchar,MAPPED_ORGANISM_FOUND varchar,MAPPED_ORGANISM_EXCLUDED varchar,CULTURE_GROWTH varchar,CULTURE_VALUE varchar,CULTURE_UNIT varchar,
ANTIBIOTIC varchar,MAPPED_ANTIBIOTIC varchar,SENSITIVITY varchar,SOURCEID varchar
) 
LOCATION ( 
'gpfdist://greenplum01:8081/covid/20210401/*micro*.txt.gz'
)
FORMAT 'text' ( HEADER DELIMITER '|' null as '' escape 'OFF');

-- Test
/*
select *
from ext_covid_micro
limit 1000;
*/
-- microert: 88s, Updated Rows	151,742,681
insert into opt_20210401.micro
select * from ext_covid_micro;

--Scratch
select year, count(*), min(admit_date), max(admit_date)
from opt_20210401.micro
group by 1
order by 1;