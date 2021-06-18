--Medical
drop table medicaid.enc_header;
create table medicaid.enc_header (
year_fy smallint, file varchar, 
MCO_ICN varchar,TX_CD varchar,SUB_MCO_PLN varchar,PRG varchar,ENC_STAT_CD varchar,ADJUD_DT varchar,FRM_DOS date,TO_DOS date,ADM_DT date,DIS_DT date,
PAT_STAT varchar,TOT_CHRG_AMT varchar,MCO_PD_AMT varchar,
BILL_PROV_NPI varchar,BILL_PROV_ID varchar,BILL_PROV_SFX varchar,BILL_PROV_TAX_CD varchar,BILL_PROV_NBR varchar,BILL_PROV_TYP_CD varchar,BILL_PROV_SPC_CD varchar,BILL_PROV_ZIP varchar,
ATTD_PHY_NPI varchar,FAC_TYP_CD varchar,POA_CD varchar,PTA_CD varchar,
REAS_ICD_QAL_1 varchar,REAS_ICD_1 varchar,REAS_ICD_QAL_2 varchar,REAS_ICD_2 varchar,REAS_ICD_QAL_3 varchar,REAS_ICD_3 varchar,
PRin_PROC_DT varchar,PROC_DT_1 varchar,PROC_DT_2 varchar,PROC_DT_3 varchar,PROC_DT_4 varchar,PROC_DT_5 varchar,PROC_DT_6 varchar,PROC_DT_7 varchar,
PROC_DT_8 varchar,PROC_DT_9 varchar,PROC_DT_10 varchar,PROC_DT_11 varchar,PROC_DT_12 varchar,PROC_DT_13 varchar,PROC_DT_14 varchar,
PROC_DT_15 varchar,PROC_DT_16 varchar,PROC_DT_17 varchar,PROC_DT_18 varchar,PROC_DT_19 varchar,PROC_DT_20 varchar,PROC_DT_21 varchar,
PROC_DT_22 varchar,PROC_DT_23 varchar,PROC_DT_24 varchar,MCO_SDA_NAME varchar,HDR_CAR_TYP_CD varchar,FIN_AGR_CD varchar,DERV_ENC varchar
) 
WITH (appendonly=true, orientation=column, compresstype=zlib)
distributed by (MCO_ICN);

drop external table ext_enc_header;
CREATE EXTERNAL TABLE ext_enc_header (
year_fy smallint, filename varchar,
MCO_ICN varchar,TX_CD varchar,SUB_MCO_PLN varchar,PRG varchar,ENC_STAT_CD varchar,ADJUD_DT varchar,FRM_DOS date,TO_DOS date,ADM_DT date,DIS_DT date,
PAT_STAT varchar,TOT_CHRG_AMT varchar,MCO_PD_AMT varchar,
BILL_PROV_NPI varchar,BILL_PROV_ID varchar,BILL_PROV_SFX varchar,BILL_PROV_TAX_CD varchar,BILL_PROV_NBR varchar,BILL_PROV_TYP_CD varchar,BILL_PROV_SPC_CD varchar,BILL_PROV_ZIP varchar,
ATTD_PHY_NPI varchar,FAC_TYP_CD varchar,POA_CD varchar,PTA_CD varchar,
REAS_ICD_QAL_1 varchar,REAS_ICD_1 varchar,REAS_ICD_QAL_2 varchar,REAS_ICD_2 varchar,REAS_ICD_QAL_3 varchar,REAS_ICD_3 varchar,
PRin_PROC_DT varchar,PROC_DT_1 varchar,PROC_DT_2 varchar,PROC_DT_3 varchar,PROC_DT_4 varchar,PROC_DT_5 varchar,PROC_DT_6 varchar,PROC_DT_7 varchar,
PROC_DT_8 varchar,PROC_DT_9 varchar,PROC_DT_10 varchar,PROC_DT_11 varchar,PROC_DT_12 varchar,PROC_DT_13 varchar,PROC_DT_14 varchar,
PROC_DT_15 varchar,PROC_DT_16 varchar,PROC_DT_17 varchar,PROC_DT_18 varchar,PROC_DT_19 varchar,PROC_DT_20 varchar,PROC_DT_21 varchar,
PROC_DT_22 varchar,PROC_DT_23 varchar,PROC_DT_24 varchar,MCO_SDA_NAME varchar,HDR_CAR_TYP_CD varchar,FIN_AGR_CD varchar,DERV_ENC varchar
) 
LOCATION ( 
'gpfdist://greenplum01:8081/uthealth/medicaid/load/*/ENC_HEADER_*.csv#transform=add_parentname_filename_comma'
)
FORMAT 'CSV' ( HEADER DELIMITER ',' );

-- Test
/*
select *
from ext_enc_header
limit 10;
*/
-- Insert
insert into medicaid.enc_header
select * from ext_enc_header;



-- Analyze
analyze medicaid.enc_header;
 
-- Verify
select count(*), min(year_fy), max(year_fy), count(distinct year_fy) from medicaid.enc_header;


select year_fy, file, count(*)
from medicaid.enc_header
group by 1, 2
order by 1, 2;
