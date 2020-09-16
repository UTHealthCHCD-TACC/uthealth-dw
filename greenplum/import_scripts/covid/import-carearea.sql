--Medical
drop table covid_20200525.carearea;
create table covid_20200525.carearea (
PTID varchar,ENCID varchar,CAREAREA varchar,CAREAREA_DATE date,CAREAREA_TIME time,SOURCEID varchar
) 
WITH (appendonly=true, orientation=column, compresstype=zlib)
distributed randomly;

-- NOTE: Load each year 1 by one updating the gpfdist string and hard coded YEAR value in insert statement.

drop external table ext_covid_carearea;
CREATE EXTERNAL TABLE ext_covid_carearea (
PTID varchar,ENCID varchar,CAREAREA varchar,CAREAREA_DATE date,CAREAREA_TIME time,SOURCEID varchar
) 
LOCATION ( 
'gpfdist://192.168.58.179:8081/covid/*carearea.*'
)
FORMAT 'text' ( HEADER DELIMITER '|' null as '' escape 'OFF');

-- Test
/*
select *
from ext_covid_carearea
limit 1000;
*/
-- Insert: 14s, Updated Rows	26,567,167
insert into covid_20200525.carearea
select * from ext_covid_carearea;

--Scratch
select year, count(*), min(admit_date), max(admit_date)
from optum_zip_refresh.confinement
group by 1
order by 1;