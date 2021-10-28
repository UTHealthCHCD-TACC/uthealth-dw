--NOTE: Dropped betos_code and betos_desc in 20210916
--nlp_sds
drop table opt_20210916.nlp_sds;
create table opt_20210916.nlp_sds (
PTID varchar,ENCID varchar,NOTE_DATE date,SDS_TERM varchar,SDS_LOCATION varchar,SDS_ATTRIBUTE varchar,SDS_SENTIMENT varchar,
OCCURRENCE_DATE varchar,NOTE_SECTION varchar,SDS_CONCEPT varchar,SDS_TIMING varchar,SDS_SEVERITY varchar,SDS_EXTENT varchar,
SDS_DURATION varchar,SDS_CHANGE varchar,SDS_QUALITY varchar,SOURCEID varchar
) 
WITH (appendonly=true, orientation=column, compresstype=zlib)
distributed by (ptid);

drop external table ext_covid_nlp_sds;
CREATE EXTERNAL TABLE ext_covid_nlp_sds (
PTID varchar,ENCID varchar,NOTE_DATE date,SDS_TERM varchar,SDS_LOCATION varchar,SDS_ATTRIBUTE varchar,SDS_SENTIMENT varchar,
OCCURRENCE_DATE varchar,NOTE_SECTION varchar,SDS_CONCEPT varchar,SDS_TIMING varchar,SDS_SEVERITY varchar,SDS_EXTENT varchar,
SDS_DURATION varchar,SDS_CHANGE varchar,SDS_QUALITY varchar,SOURCEID varchar) 
LOCATION ( 
'gpfdist://greenplum01:8081/covid/20210916/*nlp_sds*.txt.gz'
)
FORMAT 'text' ( HEADER DELIMITER '|' null as '' escape 'OFF');


-- Test
/*
select *
from ext_covid_nlp_sds
limit 1000;
*/
-- nlp_sdsert: 89s, Updated Rows	87,668,224
insert into opt_20210916.nlp_sds
select * from ext_covid_nlp_sds;

--Scratch
select count(*)
from opt_20210916.nlp_sds
group by 1
order by 1;