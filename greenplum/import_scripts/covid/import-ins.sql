--Medical
drop table opt_20210624.ins;
create table opt_20210624.ins (
PTID varchar, ENCID varchar, INSURANCE_DATE date,INSURANCE_TIME time, INS_TYPE varchar,SOURCEID varchar
) 
WITH (appendonly=true, orientation=column, compresstype=zlib)
distributed by(ptid);

drop external table ext_covid_ins;
CREATE EXTERNAL TABLE ext_covid_ins (
PTID varchar, ENCID varchar, INSURANCE_DATE date,INSURANCE_TIME time, INS_TYPE varchar,SOURCEID varchar
) 
LOCATION ( 
'gpfdist://greenplum01:8081/covid/20210624/*ins*.txt.gz'
)
FORMAT 'text' ( HEADER DELIMITER '|' null as '' escape 'OFF');

-- Test
/*
select *
from ext_covid_ins
limit 1000;
*/
-- Insert: 14s, Updated Rows	68,071,486
insert into opt_20210624.ins
select * from ext_covid_ins;

--Scratch
select count(*)
from opt_20210624.ins
group by 1
order by 1;