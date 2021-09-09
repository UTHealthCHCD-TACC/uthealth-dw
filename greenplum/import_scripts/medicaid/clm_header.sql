--Medical
drop table medicaid.clm_header;
create table medicaid.clm_header ( 
year_fy smallint, filename varchar,
ICN varchar,CLM_TYP_CD varchar,CLM_STAT_DT varchar,CLM_PRG_CD varchar,CLM_CUR_STAT_CD varchar,HDR_PD_DT varchar,HDR_FRM_DOS varchar,HDR_TO_DOS varchar,
ADM_DT varchar,DIS_DT varchar,PAT_STAT_CD varchar,TOT_BILL_AMT varchar,TOT_ALWD_AMT varchar,HDR_PD_AMT varchar,BILL_PROV_NPI varchar,BILL_PROV_ID varchar,BILL_PROV_SFX varchar,
HDR_TXM_CD varchar,BILL_PROV_TY_CD varchar,BILL_PROV_SP_CD varchar,HDR_zip5_CD varchar,ATD_PROV_NPI varchar,FAC_PROV_TY_CD varchar,POA_CD varchar,PTA_CD varchar,
REAS_ICD_QAL_1 varchar,REAS_CD_1 varchar,REAS_ICD_QAL_2 varchar,REAS_CD_2 varchar,REAS_ICD_QAL_3 varchar,REAS_CD_3 varchar,
PROC_DT_1 varchar,PROC_DT_2 varchar,PROC_DT_3 varchar,PROC_DT_4 varchar,PROC_DT_5 varchar,PROC_DT_6 varchar,PROC_DT_7 varchar,
PROC_DT_8 varchar,PROC_DT_9 varchar,PROC_DT_10 varchar,PROC_DT_11 varchar,PROC_DT_12 varchar,PROC_DT_13 varchar,PROC_DT_14 varchar,
PROC_DT_15 varchar,PROC_DT_16 varchar,PROC_DT_17 varchar,PROC_DT_18 varchar,PROC_DT_19 varchar,PROC_DT_20 varchar,PROC_DT_21 varchar,
PROC_DT_22 varchar,PROC_DT_23 varchar,PROC_DT_24 varchar,
BILL_PROV_EXMPT_IND varchar
) 
WITH (appendonly=true, orientation=column, compresstype=zlib)
distributed by (ICN);

drop external table ext_clm_header;
CREATE EXTERNAL TABLE ext_clm_header (
year_fy smallint, filename varchar,
ICN varchar,CLM_TYP_CD varchar,CLM_STAT_DT varchar,CLM_PRG_CD varchar,CLM_CUR_STAT_CD varchar,HDR_PD_DT varchar,HDR_FRM_DOS varchar,HDR_TO_DOS varchar,
ADM_DT varchar,DIS_DT varchar,PAT_STAT_CD varchar,TOT_BILL_AMT varchar,TOT_ALWD_AMT varchar,HDR_PD_AMT varchar,BILL_PROV_NPI varchar,BILL_PROV_ID varchar,BILL_PROV_SFX varchar,
HDR_TXM_CD varchar,BILL_PROV_TY_CD varchar,BILL_PROV_SP_CD varchar,HDR_zip5_CD varchar,ATD_PROV_NPI varchar,FAC_PROV_TY_CD varchar,POA_CD varchar,PTA_CD varchar,
REAS_ICD_QAL_1 varchar,REAS_CD_1 varchar,REAS_ICD_QAL_2 varchar,REAS_CD_2 varchar,REAS_ICD_QAL_3 varchar,REAS_CD_3 varchar,
PROC_DT_1 varchar,PROC_DT_2 varchar,PROC_DT_3 varchar,PROC_DT_4 varchar,PROC_DT_5 varchar,PROC_DT_6 varchar,PROC_DT_7 varchar,
PROC_DT_8 varchar,PROC_DT_9 varchar,PROC_DT_10 varchar,PROC_DT_11 varchar,PROC_DT_12 varchar,PROC_DT_13 varchar,PROC_DT_14 varchar,
PROC_DT_15 varchar,PROC_DT_16 varchar,PROC_DT_17 varchar,PROC_DT_18 varchar,PROC_DT_19 varchar,PROC_DT_20 varchar,PROC_DT_21 varchar,
PROC_DT_22 varchar,PROC_DT_23 varchar,PROC_DT_24 varchar,
BILL_PROV_EXMPT_IND varchar
) 
LOCATION ( 
'gpfdist://greenplum01:8081/uthealth/medicaid/2020/CLM_HEADER_*.csv#transform=add_parentname_filename_comma'
)
FORMAT 'CSV' ( HEADER DELIMITER ',' );

-- Test
/*
select *
from ext_clm_header
limit 10;
*/
-- Insert
insert into medicaid.clm_header
select * from ext_clm_header;

-- 318 secs
update medicaid.htw_clm_header set year_fy=date_part('year_fy', FST_DT) where year_fy=0;


-- Analyze
analyze medicaid.htw_clm_header;
 
-- Verify
select count(*), min(year_fy), max(year_fy), count(distinct year_fy) from medicaid.htw_clm_header;


select year_fy, date_part('quarter', FST_DT) as quarter, count(*), min(FST_DT), max(FST_DT)
from medicaid.htw_clm_header
group by 1, 2
order by 1, 2;
