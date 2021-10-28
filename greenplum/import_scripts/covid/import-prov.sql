--Medical
drop table opt_20210916.prov;
create table opt_20210916.prov (
PROVID varchar, SPECIALTY varchar, PRIM_SPEC_IND varchar, SOURCEID varchar
) 
WITH (appendonly=true, orientation=column, compresstype=zlib)
distributed by (PROVID);

-- NOTE: Load each year 1 by one updating the gpfdist string and hard coded YEAR value in provert statement.

drop external table ext_covid_prov;
CREATE EXTERNAL TABLE ext_covid_prov (
PROVID varchar, SPECIALTY varchar, PRIM_SPEC_IND varchar, SOURCEID varchar
) 
LOCATION ( 
'gpfdist://greenplum01:8081/covid/20210916/*prov*.txt.gz'
)
FORMAT 'text' ( HEADER DELIMITER '|' null as '' escape 'OFF');

-- Test
/*
select *
from ext_covid_prov
limit 1000;
*/
-- provert: 88s, Updated Rows	187303978
insert into opt_20210916.prov
select * from ext_covid_prov;

--Scratch
select count(*)
from opt_20210916.prov
group by 1
order by 1;