--Medical
drop table covid_20200525.prov;
create table covid_20200525.prov (
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
'gpfdist://192.168.58.179:8081/covid/*prov.*'
)
FORMAT 'text' ( HEADER DELIMITER '|' null as '' escape 'OFF');

-- Test
/*
select *
from ext_covid_prov
limit 1000;
*/
-- provert: 88s, Updated Rows	187303978
insert into covid_20200525.prov
select * from ext_covid_prov;

--Scratch
select count(*)
from covid_20200525.prov
group by 1
order by 1;