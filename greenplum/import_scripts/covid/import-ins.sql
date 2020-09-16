--Medical
drop table covid_20200525.ins;
create table covid_20200525.ins (
PTID varchar, ENCID varchar, INSURANCE_DATE date,INSURANCE_TIME time, INS_TYPE varchar,SOURCEID varchar
) 
WITH (appendonly=true, orientation=column, compresstype=zlib)
distributed by(ptid);

-- NOTE: Load each year 1 by one updating the gpfdist string and hard coded YEAR value in insert statement.

drop external table ext_covid_ins;
CREATE EXTERNAL TABLE ext_covid_ins (
PTID varchar, ENCID varchar, INSURANCE_DATE date,INSURANCE_TIME time, INS_TYPE varchar,SOURCEID varchar
) 
LOCATION ( 
'gpfdist://192.168.58.179:8081/covid/*ins.*'
)
FORMAT 'text' ( HEADER DELIMITER '|' null as '' escape 'OFF');

-- Test
/*
select *
from ext_covid_ins
limit 1000;
*/
-- Insert: 14s, Updated Rows	68,071,486
insert into covid_20200525.ins
select * from ext_covid_ins;

--Scratch
select year, count(*), min(admit_date), max(admit_date)
from covid_20200525.ins
group by 1
order by 1;