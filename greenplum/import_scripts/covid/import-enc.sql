--Medical
drop table opt_20210916.enc;
create table opt_20210916.enc (
PTID varchar, VISITID varchar,ENCID varchar,INTERACTION_TYPE varchar,INTERACTION_DATE date,INTERACTION_TIME time,ACADEMIC_COMMUNITY_FLAG varchar,SOURCEID varchar
) 
WITH (appendonly=true, orientation=column, compresstype=zlib)
distributed BY (PTID);

drop external table ext_covid_enc;
CREATE EXTERNAL TABLE ext_covid_enc (
PTID varchar, VISITID varchar,ENCID varchar,INTERACTION_TYPE varchar,INTERACTION_DATE date,INTERACTION_TIME time,ACADEMIC_COMMUNITY_FLAG varchar,SOURCEID varchar
) 
LOCATION ( 
'gpfdist://greenplum01:8081/covid/20210916/*enc_[0-9]*.txt.gz'
)
FORMAT 'text' ( HEADER DELIMITER '|' null as '' escape 'OFF');

-- Test
/*
select *
from ext_covid_enc
limit 1000;
*/

insert into opt_20210916.enc
select * from ext_covid_enc;

--Scratch
select year, count(*), min(admit_date), max(admit_date)
from opt_20210916.enc
group by 1
order by 1;