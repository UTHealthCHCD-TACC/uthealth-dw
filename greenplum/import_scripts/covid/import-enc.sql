--Medical
drop table opt_20210107.enc;
create table opt_20210107.enc (
PTID varchar, VISITID varchar,ENCID varchar,INTERACTION_TYPE varchar,INTERACTION_DATE date,INTERACTION_TIME time,ACADEMIC_COMMUNITY_FLAG varchar,SOURCEID varchar
) 
WITH (appendonly=true, orientation=column, compresstype=zlib)
distributed BY (PTID);

-- NOTE: Load each year 1 by one updating the gpfdist string and hard coded YEAR value in insert statement.

drop external table ext_covid_enc;
CREATE EXTERNAL TABLE ext_covid_enc (
PTID varchar, VISITID varchar,ENCID varchar,INTERACTION_TYPE varchar,INTERACTION_DATE date,INTERACTION_TIME time,ACADEMIC_COMMUNITY_FLAG varchar,SOURCEID varchar
) 
LOCATION ( 
'gpfdist://greenplum01:8081/covid/20210107/*enc.*'
)
FORMAT 'text' ( HEADER DELIMITER '|' null as '' escape 'OFF');

-- Test
/*
select *
from ext_covid_enc
limit 1000;
*/
-- Insert: 88s, Updated Rows	151,742,681
insert into opt_20210107.enc
select * from ext_covid_enc;

--Scratch
select year, count(*), min(admit_date), max(admit_date)
from opt_20210107.enc
group by 1
order by 1;