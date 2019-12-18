drop external table ext_outpatient_revenue_center_k;

CREATE EXTERNAL TABLE ext_outpatient_revenue_center_k (
BENE_ID varchar, CLM_ID varchar, CLM_THRU_DT varchar, CLM_LINE_NUM varchar, NCH_CLM_TYPE_CD varchar, REV_CNTR varchar, REV_CNTR_DT varchar, 
REV_CNTR_1ST_ANSI_CD varchar, REV_CNTR_2ND_ANSI_CD varchar, REV_CNTR_3RD_ANSI_CD varchar, REV_CNTR_4TH_ANSI_CD varchar, 
REV_CNTR_APC_HIPPS_CD varchar, HCPCS_CD varchar, HCPCS_1ST_MDFR_CD varchar, HCPCS_2ND_MDFR_CD varchar, HCPCS_3RD_MDFR_CD varchar, 
HCPCS_4TH_MDFR_CD varchar, REV_CNTR_PMT_MTHD_IND_CD varchar, REV_CNTR_DSCNT_IND_CD varchar, REV_CNTR_PACKG_IND_CD varchar, 
REV_CNTR_OTAF_PMT_CD varchar, REV_CNTR_IDE_NDC_UPC_NUM varchar, REV_CNTR_UNIT_CNT varchar, REV_CNTR_RATE_AMT varchar, 
REV_CNTR_BLOOD_DDCTBL_AMT varchar, REV_CNTR_CASH_DDCTBL_AMT varchar, REV_CNTR_COINSRNC_WGE_ADJSTD_C varchar, 
REV_CNTR_RDCD_COINSRNC_AMT varchar, REV_CNTR_1ST_MSP_PD_AMT varchar, REV_CNTR_2ND_MSP_PD_AMT varchar, REV_CNTR_PRVDR_PMT_AMT varchar, 
REV_CNTR_BENE_PMT_AMT varchar, REV_CNTR_PTNT_RSPNSBLTY_PMT varchar, REV_CNTR_PMT_AMT_AMT varchar, REV_CNTR_TOT_CHRG_AMT varchar, 
REV_CNTR_NCVRD_CHRG_AMT varchar, REV_CNTR_STUS_IND_CD varchar, REV_CNTR_NDC_QTY varchar, REV_CNTR_NDC_QTY_QLFR_CD varchar, 
RNDRNG_PHYSN_UPIN varchar, RNDRNG_PHYSN_NPI varchar, RNDRNG_PHYSN_SPCLTY_CD varchar, REV_CNTR_DDCTBL_COINSRNC_CD varchar, 
REV_CNTR_PRCNG_IND_CD varchar, THRPY_CAP_IND_CD1 varchar, THRPY_CAP_IND_CD2 varchar, RC_PTNT_ADD_ON_PYMT_AMT varchar, 
TRNSTNL_DRUG_ADD_ON_PYMT_AMT varchar
) 
LOCATION ( 
'gpfdist://c252-140:8801/medicare/201*/outpatient_revenue_center_k.csv'
)
FORMAT 'CSV' ( HEADER DELIMITER ',' );

select *
from ext_outpatient_revenue_center_k
limit 1000;

create table medicare.outpatient_revenue_center_k
WITH (appendonly=true, orientation=column)
as
select * 
from ext_outpatient_revenue_center_k
distributed randomly;

select count(*)
from medicare.outpatient_revenue_center_k;