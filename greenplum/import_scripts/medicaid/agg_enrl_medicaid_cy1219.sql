--Medical
drop table medicaid.agg_enrl_medicaid_cy1219;
create table medicaid.agg_enrl_medicaid_cy1219 (
CLIENT_NBR varchar, SEX char, AGE varchar, ZIP3 int, ENRL_MONTHS int, ENRL_CY int
) 
WITH (appendonly=true, orientation=column, compresstype=zlib)
distributed by (CLIENT_NBR);

drop external table ext_agg_enrl_medicaid_cy1219;
CREATE EXTERNAL TABLE ext_agg_enrl_medicaid_cy1219 (
CLIENT_NBR varchar, SEX char, AGE varchar, ZIP3 int, ENRL_MONTHS int, ENRL_CY int
) 
LOCATION ( 
'gpfdist://greenplum01:8081/uthealth/medicaid/AGG_ENRL_Medicaid_CY1219.csv'
)
FORMAT 'CSV' ( HEADER DELIMITER ',' );

-- Test
/*
select *
from ext_agg_enrl_medicaid_cy1219
limit 10;
*/
-- Insert
insert into medicaid.agg_enrl_medicaid_cy1219
select * from ext_agg_enrl_medicaid_cy1219;


-- Analyze
analyze medicaid.agg_enrl_mcd_fscyr;
 
-- Verify
select count(*), min(year_fy), max(year_fy), count(distinct year_fy) from medicaid.chip_prov;


select year_fy, date_part('quarter', FST_DT) as quarter, count(*), min(FST_DT), max(FST_DT)
from medicaid.chip_prov
group by 1, 2
order by 1, 2;
