--Medical
drop table medicaid.htw_clm_dx;
create table medicaid.htw_clm_dx ( 
ICN varchar,PRIM_DX_QAL varchar,PRIM_DX_CD varchar,DRG varchar,ADM_DX_CD varchar,DX_CD_QUAL_1 varchar,DX_CD_1 varchar,DX_CD_QUAL_2 varchar,DX_CD_2 varchar,DX_CD_QUAL_3 varchar,DX_CD_3 varchar,DX_CD_QUAL_4 varchar,DX_CD_4 varchar,DX_CD_QUAL_5 varchar,DX_CD_5 varchar,DX_CD_QUAL_6 varchar,DX_CD_6 varchar,DX_CD_QUAL_7 varchar,DX_CD_7 varchar,
DX_CD_QUAL_8 varchar,DX_CD_8 varchar,DX_CD_QUAL_9 varchar,DX_CD_9 varchar,DX_CD_QUAL_10 varchar,DX_CD_10 varchar,DX_CD_QUAL_11 varchar,DX_CD_11 varchar,DX_CD_QUAL_12 varchar,DX_CD_12 varchar,DX_CD_QUAL_13 varchar,DX_CD_13 varchar,DX_CD_QUAL_14 varchar,DX_CD_14 varchar,
DX_CD_QUAL_15 varchar,DX_CD_15 varchar,DX_CD_QUAL_16 varchar,DX_CD_16 varchar,DX_CD_QUAL_17 varchar,DX_CD_17 varchar,DX_CD_QUAL_18 varchar,DX_CD_18 varchar,DX_CD_QUAL_19 varchar,DX_CD_19 varchar,DX_CD_QUAL_20 varchar,DX_CD_20 varchar,DX_CD_QUAL_21 varchar,DX_CD_21 varchar,
DX_CD_QUAL_22 varchar,DX_CD_22 varchar,DX_CD_QUAL_23 varchar,DX_CD_23 varchar,DX_CD_QUAL_24 varchar,DX_CD_24 varchar,DX_CD_QUAL_25 varchar,DX_CD_25 varchar,
PRM_DX_POA varchar,DX_POA_1 varchar,DX_POA_2 varchar,DX_POA_3 varchar,DX_POA_4 varchar,DX_POA_5 varchar,DX_POA_6 varchar,DX_POA_7 varchar,
DX_POA_8 varchar,DX_POA_9 varchar,DX_POA_10 varchar,DX_POA_11 varchar,DX_POA_12 varchar,DX_POA_13 varchar,DX_POA_14 varchar,
DX_POA_15 varchar,DX_POA_16 varchar,DX_POA_17 varchar,DX_POA_18 varchar,DX_POA_19 varchar,DX_POA_20 varchar,DX_POA_21 varchar,
DX_POA_22 varchar,DX_POA_23 varchar,DX_POA_24 varchar, DX_POA_25 varchar
) 
WITH (appendonly=true, orientation=column, compresstype=zlib)
distributed by (ICN);

drop external table ext_htw_clm_dx;
CREATE EXTERNAL TABLE ext_htw_clm_dx (
ICN varchar,PRIM_DX_QAL varchar,PRIM_DX_CD varchar,DRG varchar,ADM_DX_CD varchar,DX_CD_QUAL_1 varchar,DX_CD_1 varchar,DX_CD_QUAL_2 varchar,DX_CD_2 varchar,DX_CD_QUAL_3 varchar,DX_CD_3 varchar,DX_CD_QUAL_4 varchar,DX_CD_4 varchar,DX_CD_QUAL_5 varchar,DX_CD_5 varchar,DX_CD_QUAL_6 varchar,DX_CD_6 varchar,DX_CD_QUAL_7 varchar,DX_CD_7 varchar,
DX_CD_QUAL_8 varchar,DX_CD_8 varchar,DX_CD_QUAL_9 varchar,DX_CD_9 varchar,DX_CD_QUAL_10 varchar,DX_CD_10 varchar,DX_CD_QUAL_11 varchar,DX_CD_11 varchar,DX_CD_QUAL_12 varchar,DX_CD_12 varchar,DX_CD_QUAL_13 varchar,DX_CD_13 varchar,DX_CD_QUAL_14 varchar,DX_CD_14 varchar,
DX_CD_QUAL_15 varchar,DX_CD_15 varchar,DX_CD_QUAL_16 varchar,DX_CD_16 varchar,DX_CD_QUAL_17 varchar,DX_CD_17 varchar,DX_CD_QUAL_18 varchar,DX_CD_18 varchar,DX_CD_QUAL_19 varchar,DX_CD_19 varchar,DX_CD_QUAL_20 varchar,DX_CD_20 varchar,DX_CD_QUAL_21 varchar,DX_CD_21 varchar,
DX_CD_QUAL_22 varchar,DX_CD_22 varchar,DX_CD_QUAL_23 varchar,DX_CD_23 varchar,DX_CD_QUAL_24 varchar,DX_CD_24 varchar,DX_CD_QUAL_25 varchar,DX_CD_25 varchar,
PRM_DX_POA varchar,DX_POA_1 varchar,DX_POA_2 varchar,DX_POA_3 varchar,DX_POA_4 varchar,DX_POA_5 varchar,DX_POA_6 varchar,DX_POA_7 varchar,
DX_POA_8 varchar,DX_POA_9 varchar,DX_POA_10 varchar,DX_POA_11 varchar,DX_POA_12 varchar,DX_POA_13 varchar,DX_POA_14 varchar,
DX_POA_15 varchar,DX_POA_16 varchar,DX_POA_17 varchar,DX_POA_18 varchar,DX_POA_19 varchar,DX_POA_20 varchar,DX_POA_21 varchar,
DX_POA_22 varchar,DX_POA_23 varchar,DX_POA_24 varchar, DX_POA_25 varchar
) 
LOCATION ( 
'gpfdist://greenplum01:8081/uthealth/medicaid/HealthyTexasWomen/CLM_DX_*.csv'
)
FORMAT 'CSV' ( HEADER DELIMITER ',' );

-- Test
/*
select *
from ext_htw_clm_dx
limit 10;
*/
-- Insert
insert into medicaid.htw_clm_dx
select * from ext_htw_clm_dx;

-- 318 secs
update medicaid.htw_clm_dx set year_fy=date_part('year_fy', FST_DT) where year_fy=0;


-- Analyze
analyze medicaid.htw_clm_dx;
 
-- Verify
select count(*), min(year_fy), max(year_fy), count(distinct year_fy) from medicaid.htw_clm_dx;


select year_fy, date_part('quarter', FST_DT) as quarter, count(*), min(FST_DT), max(FST_DT)
from medicaid.htw_clm_dx
group by 1, 2
order by 1, 2;
