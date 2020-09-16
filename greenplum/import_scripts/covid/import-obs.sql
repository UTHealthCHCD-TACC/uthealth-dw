--Medical
drop table covid_20200525.obs;
create table covid_20200525.obs (
PTID varchar,ENCID varchar,OBS_TYPE varchar,NLP varchar,OBS_DATE date,OBS_TIME time,OBS_RESULT varchar,OBS_UNIT varchar,
EVALUATED_FOR_RANGE varchar,VALUE_WITHIN_RANGE varchar,RESULT_DATE varchar,SOURCEID varchar
) 
WITH (appendonly=true, orientation=column, compresstype=zlib)
distributed by (ptid);

-- NOTE: Load each year 1 by one updating the gpfdist string and hard coded YEAR value in obsert statement.

drop external table ext_covid_obs;
CREATE EXTERNAL TABLE ext_covid_obs (
PTID varchar,ENCID varchar,OBS_TYPE varchar,NLP varchar,OBS_DATE date,OBS_TIME time,OBS_RESULT varchar,OBS_UNIT varchar,
EVALUATED_FOR_RANGE varchar,VALUE_WITHIN_RANGE varchar,RESULT_DATE varchar,SOURCEID varchar
) 
LOCATION ( 
'gpfdist://192.168.58.179:8081/covid/*obs.*'
)
FORMAT 'text' ( HEADER DELIMITER '|' null as '' escape 'OFF');

-- Test
/*
select *
from ext_covid_obs
limit 1000;
*/
-- obsert: 272s, Updated Rows	516,354,697
insert into covid_20200525.obs
select * from ext_covid_obs;

--Scratch
select year, count(*), min(admit_date), max(admit_date)
from covid_20200525.obs
group by 1
order by 1;