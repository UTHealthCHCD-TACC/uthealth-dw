--Medical
drop table covid_20200525.pt;
create table covid_20200525.pt (
PTID varchar,BIRTH_YR int,GENDER varchar,RACE varchar,ETHNICITY varchar,REGION varchar,DIVISION varchar,DECEASED_INDICATOR varchar,DATE_OF_DEATH varchar,
PROVID_PCP varchar,IDN_INDICATOR varchar, FIRST_MONTH_ACTIVE varchar,LAST_MONTH_ACTIVE varchar,NOTES_ELIGIBLE varchar,HAS_NOTES varchar,
SOURCEID varchar,SOURCE_DATA_THROUGH varchar,INTEGRATED varchar
) 
WITH (appendonly=true, orientation=column, compresstype=zlib)
distributed by (ptid);

-- NOTE: Load each year 1 by one updating the gpfdist string and hard coded YEAR value in ptert statement.

drop external table ext_covid_pt;
CREATE EXTERNAL TABLE ext_covid_pt (
PTID varchar,BIRTH_YR int,GENDER varchar,RACE varchar,ETHNICITY varchar,REGION varchar,DIVISION varchar,DECEASED_INDICATOR varchar,DATE_OF_DEATH varchar,
PROVID_PCP varchar,IDN_INDICATOR varchar, FIRST_MONTH_ACTIVE varchar,LAST_MONTH_ACTIVE varchar,NOTES_ELIGIBLE varchar,HAS_NOTES varchar,
SOURCEID varchar,SOURCE_DATA_THROUGH varchar,INTEGRATED varchar
) 
LOCATION ( 
'gpfdist://192.168.58.179:8081/covid/*pt.*'
)
FORMAT 'text' ( HEADER DELIMITER '|' null as '' escape 'OFF');

-- Test
/*
select *
from ext_covid_pt
limit 1000;
*/
-- ptert: 2s, Updated Rows	548665
insert into covid_20200525.pt
select * from ext_covid_pt;

--Scratch
select year, count(*), min(admit_date), max(admit_date)
from covid_20200525.pt
group by 1
order by 1;