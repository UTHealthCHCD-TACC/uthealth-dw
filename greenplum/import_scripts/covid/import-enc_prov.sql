--Medical
drop table opt_20210916.enc_prov;
create table opt_20210916.enc_prov (
PTID varchar, ENCID varchar, PROVIDERROLE varchar, SOURCEID varchar
) 
WITH (appendonly=true, orientation=column, compresstype=zlib)
distributed BY (ptid);

drop external table ext_covid_enc_prov;
CREATE EXTERNAL TABLE ext_covid_enc_prov (
PTID varchar, ENCID varchar, PROVIDERROLE varchar, SOURCEID varchar
) 
LOCATION ( 
'gpfdist://greenplum01:8081/covid/20210916/*enc_prov*.txt.gz'
)
FORMAT 'text' ( HEADER DELIMITER '|' null as '' escape 'OFF');

-- Test
/*
select *
from ext_covid_enc_prov
limit 1000;
*/
-- Insert: 78s, Updated Rows	178,532,862
insert into opt_20210916.enc_prov
select * from ext_covid_enc_prov;

--Scratch
select sourceid, count(*)
from opt_20210916.enc_prov
group by 1
order by 1;