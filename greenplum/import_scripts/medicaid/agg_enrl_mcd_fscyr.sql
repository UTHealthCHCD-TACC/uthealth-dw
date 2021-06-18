--Medical
drop table medicaid.agg_enrl_mcd_fscyr;
create table medicaid.agg_enrl_mcd_fscyr (
CLIENT_NBR varchar, MCO_PROGRAM_NM varchar, SEX char, AGE int,ZIP int, SMIB varchar,ENRL_MONTHS int, ENRL_FY int, AgeGrp int
) 
WITH (appendonly=true, orientation=column, compresstype=zlib)
distributed by (CLIENT_NBR);

drop external table ext_agg_enrl_mcd_fscyr;
CREATE EXTERNAL TABLE ext_agg_enrl_mcd_fscyr (
CLIENT_NBR varchar, MCO_PROGRAM_NM varchar, SEX char, AGE int,ZIP int, SMIB varchar,ENRL_MONTHS int, ENRL_FY int, AgeGrp int
) 
LOCATION ( 
'gpfdist://greenplum01:8081/uthealth/medicaid/AGG_ENRL_MCD_FSCYR.csv'
)
FORMAT 'CSV' ( HEADER DELIMITER ',' );

-- Test
/*
select *
from ext_agg_enrl_mcd_fscyr
limit 10;
*/
-- Insert
insert into medicaid.agg_enrl_mcd_fscyr
select * from ext_agg_enrl_mcd_fscyr;


-- Analyze
analyze medicaid.agg_enrl_mcd_fscyr;
 
-- Verify
select count(*), min(year_fy), max(year_fy), count(distinct year_fy) from medicaid.chip_prov;


select year_fy, date_part('quarter', FST_DT) as quarter, count(*), min(FST_DT), max(FST_DT)
from medicaid.chip_prov
group by 1, 2
order by 1, 2;
