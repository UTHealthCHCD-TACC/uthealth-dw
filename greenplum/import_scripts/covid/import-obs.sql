--Medical
drop table opt_20210916.obs;
create table opt_20210916.obs (
PTID varchar,ENCID varchar,OBS_TYPE varchar,NLP varchar,OBS_DATE date,OBS_TIME time,OBS_RESULT varchar,OBS_UNIT varchar,
EVALUATED_FOR_RANGE varchar,VALUE_WITHIN_RANGE varchar,RESULT_DATE varchar,SOURCEID varchar
) 
WITH (appendonly=true, orientation=column, compresstype=zlib)
distributed by (ptid);


drop external table ext_covid_obs;
CREATE EXTERNAL TABLE ext_covid_obs (
PTID varchar,ENCID varchar,OBS_TYPE varchar,NLP varchar,OBS_DATE date,OBS_TIME time,OBS_RESULT varchar,OBS_UNIT varchar,
EVALUATED_FOR_RANGE varchar,VALUE_WITHIN_RANGE varchar,RESULT_DATE varchar,SOURCEID varchar
) 
LOCATION ( 
'gpfdist://greenplum01:8081/covid/20210916/*obs*.txt.gz'
)
FORMAT 'text' ( HEADER DELIMITER '|' null as '' escape 'OFF');

-- Test
/*
select *
from ext_covid_obs
limit 1000;
*/
-- obsert: 272s, Updated Rows	516,354,697
insert into opt_20210916.obs
select * from ext_covid_obs;

--Scratch
select count(*)
from opt_20210916.obs
group by 1
order by 1;