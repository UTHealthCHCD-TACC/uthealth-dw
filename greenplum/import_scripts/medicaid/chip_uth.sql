--Medical
drop table medicaid.chip_uth;
create table medicaid.chip_uth (
year_fy smallint, file varchar, 
CLIENT_NBR varchar, DATE_OF_BIRTH varchar, ETHNICITY varchar, MAILING_ZIP varchar, ELIG_MONTH varchar, PLAN_CD varchar,
COUNTY_CD varchar, GENDER_CD varchar, AGE numeric, PLAN_ENR_START_DT varchar, PLAN_ENR_END_DT varchar, MCO_ID varchar, PURE_RATE varchar
) 
WITH (appendonly=true, orientation=column, compresstype=zlib)
distributed by (CLIENT_NBR);

drop external table ext_chip_uth;
CREATE EXTERNAL TABLE ext_chip_uth (
year_fy smallint, filename varchar,
CLIENT_NBR varchar, DATE_OF_BIRTH varchar, ETHNICITY varchar, MAILING_ZIP varchar, ELIG_MONTH varchar, PLAN_CD varchar,
COUNTY_CD varchar, GENDER_CD varchar, AGE numeric, PLAN_ENR_START_DT varchar, PLAN_ENR_END_DT varchar, MCO_ID varchar, PURE_RATE varchar
) 
LOCATION ( 
'gpfdist://greenplum01:8081/uthealth/medicaid/2020/CHIP_UTH_*.csv#transform=add_parentname_filename_comma'
)
FORMAT 'CSV' ( HEADER DELIMITER ',' );

-- Test
/*
select *
from ext_chip_uth
limit 10;
*/
-- Insert
insert into medicaid.chip_uth
select * from ext_chip_uth;

-- 318 secs
update medicaid.chip_uth set year_fy=date_part('year_fy', FST_DT) where year_fy=0;


-- Analyze
analyze medicaid.chip_uth;
 
-- Verify
select count(*), min(year_fy), max(year_fy), count(distinct year_fy) from medicaid.chip_uth;


select year_fy, date_part('quarter', FST_DT) as quarter, count(*), min(FST_DT), max(FST_DT)
from medicaid.chip_uth
group by 1, 2
order by 1, 2;
