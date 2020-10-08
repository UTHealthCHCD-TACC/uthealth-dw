--Medical
drop table covid_20200525.proc;
create table covid_20200525.proc (
PTID varchar,ENCID varchar,PROC_DATE date,PROC_TIME time,PROC_CODE varchar,PROC_DESC varchar,PROC_CODE_TYPE varchar,PROVID_PERFORM varchar,PROVID_ORDER varchar,
BETOS_CODE varchar,BETOS_DESC varchar,SOURCEID varchar
) 
WITH (appendonly=true, orientation=column, compresstype=zlib)
distributed by (ptid);

-- NOTE: Load each year 1 by one updating the gpfdist string and hard coded YEAR value in procert statement.

drop external table ext_covid_proc;
CREATE EXTERNAL TABLE ext_covid_proc (
PTID varchar,ENCID varchar,PROC_DATE date,PROC_TIME time,PROC_CODE varchar,PROC_DESC varchar,PROC_CODE_TYPE varchar,PROVID_PERFORM varchar,PROVID_ORDER varchar,
BETOS_CODE varchar,BETOS_DESC varchar,SOURCEID varchar) 
LOCATION ( 
'gpfdist://192.168.58.179:8081/covid/*proc.*'
)
FORMAT 'text' ( HEADER DELIMITER '|' null as '' escape 'OFF');

-- Test
/*
select *
from ext_covid_proc
limit 1000;
*/
-- procert: 89s, Updated Rows	87,668,224
insert into covid_20200525.proc
select * from ext_covid_proc;

--Scratch
select count(*)
from covid_20200525.proc
group by 1
order by 1;