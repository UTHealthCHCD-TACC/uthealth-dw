--Medical
drop table covid_20200525.diag;
create table covid_20200525.diag (
PTID varchar, ENCID varchar, DIAG_DATE date, DIAG_TIME time, DIAGNOSIS_CD varchar, DIAGNOSIS_CD_TYPE varchar, 
DIAGNOSIS_STATUS varchar, POA varchar, ADMITTING_DIAGNOSIS varchar, DISCHARGE_DIAGNOSIS varchar, PRIMARY_DIAGNOSIS varchar,
PROBLEM_LIST varchar, SOURCEID varchar
) 
WITH (appendonly=true, orientation=column, compresstype=zlib)
distributed randomly;

-- NOTE: Load each year 1 by one updating the gpfdist string and hard coded YEAR value in insert statement.

drop external table ext_covid_diag;
CREATE EXTERNAL TABLE ext_covid_diag (
PTID varchar, ENCID varchar, DIAG_DATE date, DIAG_TIME time, DIAGNOSIS_CD varchar, DIAGNOSIS_CD_TYPE varchar, 
DIAGNOSIS_STATUS varchar, POA varchar, ADMITTING_DIAGNOSIS varchar, DISCHARGE_DIAGNOSIS varchar, PRIMARY_DIAGNOSIS varchar,
PROBLEM_LIST varchar, SOURCEID varchar
) 
LOCATION ( 
'gpfdist://192.168.58.179:8081/covid/*diag.*'
)
FORMAT 'text' ( HEADER DELIMITER '|' null as '' escape 'OFF');

-- Test
/*
select *
from ext_covid_diag
limit 1000;
*/
-- Insert: 3m20s, Updated Rows	352,349,506
insert into covid_20200525.diag
select * from ext_covid_diag;

--Scratch
select year, count(*), min(admit_date), max(admit_date)
from covid_20200525.diag
group by 1
order by 1;