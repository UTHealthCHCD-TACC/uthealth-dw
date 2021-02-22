--Medical
drop table opt_20210107.pt;
create table opt_20210107.pt (
PTID varchar,BIRTH_YR varchar,GENDER varchar,RACE varchar,ETHNICITY varchar,REGION varchar,DIVISION varchar,DECEASED_INDICATOR varchar,DATE_OF_DEATH varchar,
PROVID_PCP varchar,IDN_INDICATOR varchar, FIRST_MONTH_ACTIVE varchar,LAST_MONTH_ACTIVE varchar,NOTES_ELIGIBLE varchar,HAS_NOTES varchar,
SOURCEID varchar,SOURCE_DATA_THROUGH varchar,INTEGRATED varchar
) 
WITH (appendonly=true, orientation=column, compresstype=zlib)
distributed by (ptid);

-- NOTE: Load each year 1 by one updating the gpfdist string and hard coded YEAR value in ptert statement.

drop external table ext_covid_pt;
CREATE EXTERNAL TABLE ext_covid_pt (
PTID varchar,BIRTH_YR varchar,GENDER varchar,RACE varchar,ETHNICITY varchar,REGION varchar,DIVISION varchar,DECEASED_INDICATOR varchar,DATE_OF_DEATH varchar,
PROVID_PCP varchar,IDN_INDICATOR varchar, FIRST_MONTH_ACTIVE varchar,LAST_MONTH_ACTIVE varchar,NOTES_ELIGIBLE varchar,HAS_NOTES varchar,
SOURCEID varchar,SOURCE_DATA_THROUGH varchar,INTEGRATED varchar
) 
LOCATION ( 
'gpfdist://greenplum01:8081/covid/20210107/*pt.txt.gz'
)
FORMAT 'text' ( HEADER DELIMITER '|' null as '' escape 'OFF');

-- Test
/*
select *
from ext_covid_pt
limit 1000;
*/
-- ptert: 2s, Updated Rows	548665
insert into opt_20210107.pt
select * from ext_covid_pt;

--Scratch
select year, count(*), min(admit_date), max(admit_date)
from opt_20210107.pt
group by 1
order by 1;