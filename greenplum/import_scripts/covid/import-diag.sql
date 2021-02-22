--Medical
drop table opt_20210107.diag;
create table opt_20210107.diag (
PTID varchar, ENCID varchar, DIAG_DATE date, DIAG_TIME time, DIAGNOSIS_CD varchar, DIAGNOSIS_CD_TYPE varchar, 
DIAGNOSIS_STATUS varchar, POA varchar, ADMITTING_DIAGNOSIS varchar, DISCHARGE_DIAGNOSIS varchar, PRIMARY_DIAGNOSIS varchar,
PROBLEM_LIST varchar, SOURCEID varchar
) 
WITH (appendonly=true, orientation=column, compresstype=zlib)
distributed by (ptid);

-- NOTE: Load each year 1 by one updating the gpfdist string and hard coded YEAR value in insert statement.

drop external table ext_covid_diag;
CREATE EXTERNAL TABLE ext_covid_diag (
PTID varchar, ENCID varchar, DIAG_DATE date, DIAG_TIME time, DIAGNOSIS_CD varchar, DIAGNOSIS_CD_TYPE varchar, 
DIAGNOSIS_STATUS varchar, POA varchar, ADMITTING_DIAGNOSIS varchar, DISCHARGE_DIAGNOSIS varchar, PRIMARY_DIAGNOSIS varchar,
PROBLEM_LIST varchar, SOURCEID varchar
) 
LOCATION ( 
'gpfdist://greenplum01:8081/covid/20210107/*diag.*'
)
FORMAT 'text' ( HEADER DELIMITER '|' null as '' escape 'OFF');

-- Test
/*
select *
from ext_covid_diag
limit 1000;
*/
-- Insert: 3m20s, Updated Rows	352,349,506
insert into opt_20210107.diag
select * from ext_covid_diag;

analyze opt_20210107.diag;

--Scratch
select DIAGNOSIS_CD_TYPE, count(*), min(DIAG_DATE), max(DIAG_DATE), count(distinct ptid) as ptid_cnt
from opt_20210107.diag
group by 1;

select diagnosis_cd, count(*)
from opt_20210107.diag
group by 1
order by 2 desc;

select count(*)
from opt_20210107.diag;