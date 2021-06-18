--Medical
drop table medicaid.clm_detail;
create table medicaid.clm_detail (
year_fy smallint, file varchar, 
ICN varchar, CLM_DTL_NBR varchar, DTL_STAT_CD  varchar, FROM_DOS date, TO_DOS date,
PROC_CD varchar, SUB_PROC_CD varchar, DTL_BILL_AMT numeric, DTL_ALWD_AMT numeric, DTL_PD_AMT numeric, DTL_BL_QUANT_AMT numeric,
DTL_ALWD_QUANT_AMT numeric, DTL_MAN_QUANT_AMT numeric, DTL_DX_CD varchar, 
PROC_MOD_1 varchar, PROC_MOD_2 varchar, PROC_MOD_3 varchar, PROC_MOD_4 varchar, PROC_MOD_5 varchar,
POS varchar, TOS varchar, REV_CD varchar, REF_PROV_NPI varchar, PERF_PROV_NPI varchar, TXM_CD varchar, PERF_PROV_ID varchar, SUB_PERF_PROV_SFX varchar
) 
WITH (appendonly=true, orientation=column, compresstype=zlib)
distributed by (ICN);

drop external table ext_clm_detail;
CREATE EXTERNAL TABLE ext_clm_detail (
year_fy smallint, filename varchar,
ICN varchar, CLM_DTL_NBR varchar, DTL_STAT_CD  varchar, FROM_DOS date, TO_DOS date,
PROC_CD varchar, SUB_PROC_CD varchar, DTL_BILL_AMT numeric, DTL_ALWD_AMT numeric, DTL_PD_AMT numeric, DTL_BL_QUANT_AMT numeric,
DTL_ALWD_QUANT_AMT numeric, DTL_MAN_QUANT_AMT numeric, DTL_DX_CD varchar, 
PROC_MOD_1 varchar, PROC_MOD_2 varchar, PROC_MOD_3 varchar, PROC_MOD_4 varchar, PROC_MOD_5 varchar,
POS varchar, TOS varchar, REV_CD varchar, REF_PROV_NPI varchar, PERF_PROV_NPI varchar, TXM_CD varchar, PERF_PROV_ID varchar, SUB_PERF_PROV_SFX varchar
) 
LOCATION ( 
'gpfdist://greenplum01:8081/uthealth/medicaid/load/*/CLM_DETAIL_*.csv#transform=add_parentname_filename_comma'
)
FORMAT 'CSV' ( HEADER DELIMITER ',' );

-- Test
/*
select *
from ext_clm_detail
limit 10;
*/
-- Insert
insert into medicaid.clm_detail
select * from ext_clm_detail;

-- 318 secs
update medicaid.clm_detail set year_fy=date_part('year_fy', FST_DT) where year_fy=0;


-- Analyze
analyze medicaid.clm_detail;
 
-- Verify
select count(*), min(year_fy), max(year_fy), count(distinct year_fy) from medicaid.clm_detail;


select year_fy, date_part('quarter', FST_DT) as quarter, count(*), min(FST_DT), max(FST_DT)
from medicaid.clm_detail
group by 1, 2
order by 1, 2;
