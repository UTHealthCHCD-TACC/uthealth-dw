--Medical
drop table covid_20200525.enc;
create table covid_20200525.enc (
PTID varchar, VISITID varchar,ENCID varchar,INTERACTION_TYPE varchar,INTERACTION_DATE date,INTERACTION_TIME time,ACADEMIC_COMMUNITY_FLAG varchar,SOURCEID varchar
) 
WITH (appendonly=true, orientation=column, compresstype=zlib)
distributed randomly;

-- NOTE: Load each year 1 by one updating the gpfdist string and hard coded YEAR value in insert statement.

drop external table ext_covid_enc;
CREATE EXTERNAL TABLE ext_covid_enc (
PTID varchar, VISITID varchar,ENCID varchar,INTERACTION_TYPE varchar,INTERACTION_DATE date,INTERACTION_TIME time,ACADEMIC_COMMUNITY_FLAG varchar,SOURCEID varchar
) 
LOCATION ( 
'gpfdist://192.168.58.179:8081/covid/*enc.*'
)
FORMAT 'text' ( HEADER DELIMITER '|' null as '' escape 'OFF');

-- Test
/*
select *
from ext_covid_enc
limit 1000;
*/
-- Insert: 88s, Updated Rows	151,742,681
insert into covid_20200525.enc
select * from ext_covid_enc;

--Scratch
select year, count(*), min(admit_date), max(admit_date)
from covid_20200525.enc
group by 1
order by 1;