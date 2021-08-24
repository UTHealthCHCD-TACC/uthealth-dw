--Medical
drop table opt_20210624.proc;
create table opt_20210624.proc (
PTID varchar,ENCID varchar,PROC_DATE date,PROC_TIME time,PROC_CODE varchar,PROC_DESC varchar,PROC_CODE_TYPE varchar,PROVID_PERFORM varchar,PROVID_ORDER varchar,
BETOS_CODE varchar,BETOS_DESC varchar,SOURCEID varchar
) 
WITH (appendonly=true, orientation=column, compresstype=zlib)
distributed by (ptid);

drop external table ext_covid_proc;
CREATE EXTERNAL TABLE ext_covid_proc (
PTID varchar,ENCID varchar,PROC_DATE date,PROC_TIME time,PROC_CODE varchar,PROC_DESC varchar,PROC_CODE_TYPE varchar,PROVID_PERFORM varchar,PROVID_ORDER varchar,
BETOS_CODE varchar,BETOS_DESC varchar,SOURCEID varchar) 
LOCATION ( 
'gpfdist://greenplum01:8081/covid/20210624/*proc*.txt.gz'
)
FORMAT 'text' ( HEADER DELIMITER '|' null as '' escape 'OFF');

-- Test
/*
select *
from ext_covid_proc
limit 1000;
*/
-- procert: 89s, Updated Rows	87,668,224
insert into opt_20210624.proc
select * from ext_covid_proc;

--Scratch
select count(*)
from opt_20210624.proc
group by 1
order by 1;