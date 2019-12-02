drop external table ext_snf_base_claims_k;

CREATE EXTERNAL TABLE ext_snf_base_claims_k (
BENE_ID varchar, CLM_ID varchar, NCH_NEAR_LINE_REC_IDENT_CD varchar, NCH_CLM_TYPE_CD varchar, CLM_FROM_DT varchar, CLM_THRU_DT varchar, 
NCH_WKLY_PROC_DT varchar, FI_CLM_PROC_DT varchar, CLAIM_QUERY_CODE varchar, PRVDR_NUM varchar, CLM_FAC_TYPE_CD varchar, 
CLM_SRVC_CLSFCTN_TYPE_CD varchar, CLM_FREQ_CD varchar, FI_NUM varchar, CLM_MDCR_NON_PMT_RSN_CD varchar, CLM_PMT_AMT varchar, 
NCH_PRMRY_PYR_CLM_PD_AMT varchar, NCH_PRMRY_PYR_CD varchar, FI_CLM_ACTN_CD varchar, PRVDR_STATE_CD varchar, ORG_NPI_NUM varchar, 
AT_PHYSN_UPIN varchar, AT_PHYSN_NPI varchar, AT_PHYSN_SPCLTY_CD varchar, OP_PHYSN_UPIN varchar, OP_PHYSN_NPI varchar, 
OP_PHYSN_SPCLTY_CD varchar, OT_PHYSN_UPIN varchar, OT_PHYSN_NPI varchar, OT_PHYSN_SPCLTY_CD varchar, RNDRNG_PHYSN_NPI varchar, 
RNDRNG_PHYSN_SPCLTY_CD varchar, CLM_MCO_PD_SW varchar, PTNT_DSCHRG_STUS_CD varchar, CLM_PPS_IND_CD varchar, CLM_TOT_CHRG_AMT varchar, 
CLM_ADMSN_DT varchar, CLM_IP_ADMSN_TYPE_CD varchar, CLM_SRC_IP_ADMSN_CD varchar, NCH_PTNT_STATUS_IND_CD varchar, 
NCH_BENE_IP_DDCTBL_AMT varchar, NCH_BENE_PTA_COINSRNC_LBLTY_AM varchar, NCH_BENE_BLOOD_DDCTBL_LBLTY_AM varchar, 
NCH_IP_NCVRD_CHRG_AMT varchar, NCH_IP_TOT_DDCTN_AMT varchar, CLM_PPS_CPTL_FSP_AMT varchar, CLM_PPS_CPTL_OUTLIER_AMT varchar, 
CLM_PPS_CPTL_DSPRPRTNT_SHR_AMT varchar, CLM_PPS_CPTL_IME_AMT varchar, CLM_PPS_CPTL_EXCPTN_AMT varchar, CLM_PPS_OLD_CPTL_HLD_HRMLS_AMT varchar, 
CLM_UTLZTN_DAY_CNT varchar, BENE_TOT_COINSRNC_DAYS_CNT varchar, CLM_NON_UTLZTN_DAYS_CNT varchar, NCH_BLOOD_PNTS_FRNSHD_QTY varchar, 
NCH_QLFYD_STAY_FROM_DT varchar, NCH_QLFYD_STAY_THRU_DT varchar, NCH_VRFD_NCVRD_STAY_FROM_DT varchar, NCH_VRFD_NCVRD_STAY_THRU_DT varchar, 
NCH_ACTV_OR_CVRD_LVL_CARE_THRU varchar, NCH_BENE_MDCR_BNFTS_EXHTD_DT_I varchar, NCH_BENE_DSCHRG_DT varchar, CLM_DRG_CD varchar, 
ADMTG_DGNS_CD varchar, PRNCPAL_DGNS_CD varchar, ICD_DGNS_CD1 varchar, ICD_DGNS_CD2 varchar, ICD_DGNS_CD3 varchar, ICD_DGNS_CD4 varchar, 
ICD_DGNS_CD5 varchar, ICD_DGNS_CD6 varchar, ICD_DGNS_CD7 varchar, ICD_DGNS_CD8 varchar, ICD_DGNS_CD9 varchar, ICD_DGNS_CD10 varchar, 
ICD_DGNS_CD11 varchar, ICD_DGNS_CD12 varchar, ICD_DGNS_CD13 varchar, ICD_DGNS_CD14 varchar, ICD_DGNS_CD15 varchar, ICD_DGNS_CD16 varchar, 
ICD_DGNS_CD17 varchar, ICD_DGNS_CD18 varchar, ICD_DGNS_CD19 varchar, ICD_DGNS_CD20 varchar, ICD_DGNS_CD21 varchar, ICD_DGNS_CD22 varchar, 
ICD_DGNS_CD23 varchar, ICD_DGNS_CD24 varchar, ICD_DGNS_CD25 varchar, FST_DGNS_E_CD varchar, ICD_DGNS_E_CD1 varchar, ICD_DGNS_E_CD2 varchar, 
ICD_DGNS_E_CD3 varchar, ICD_DGNS_E_CD4 varchar, ICD_DGNS_E_CD5 varchar, ICD_DGNS_E_CD6 varchar, ICD_DGNS_E_CD7 varchar, ICD_DGNS_E_CD8 varchar, 
ICD_DGNS_E_CD9 varchar, ICD_DGNS_E_CD10 varchar, ICD_DGNS_E_CD11 varchar, ICD_DGNS_E_CD12 varchar, ICD_PRCDR_CD1 varchar, PRCDR_DT1 varchar, 
ICD_PRCDR_CD2 varchar, PRCDR_DT2 varchar, ICD_PRCDR_CD3 varchar, PRCDR_DT3 varchar, ICD_PRCDR_CD4 varchar, PRCDR_DT4 varchar, 
ICD_PRCDR_CD5 varchar, PRCDR_DT5 varchar, ICD_PRCDR_CD6 varchar, PRCDR_DT6 varchar, ICD_PRCDR_CD7 varchar, PRCDR_DT7 varchar, 
ICD_PRCDR_CD8 varchar, PRCDR_DT8 varchar, ICD_PRCDR_CD9 varchar, PRCDR_DT9 varchar, ICD_PRCDR_CD10 varchar, PRCDR_DT10 varchar, 
ICD_PRCDR_CD11 varchar, PRCDR_DT11 varchar, ICD_PRCDR_CD12 varchar, PRCDR_DT12 varchar, ICD_PRCDR_CD13 varchar, PRCDR_DT13 varchar, 
ICD_PRCDR_CD14 varchar, PRCDR_DT14 varchar, ICD_PRCDR_CD15 varchar, PRCDR_DT15 varchar, ICD_PRCDR_CD16 varchar, PRCDR_DT16 varchar, 
ICD_PRCDR_CD17 varchar, PRCDR_DT17 varchar, ICD_PRCDR_CD18 varchar, PRCDR_DT18 varchar, ICD_PRCDR_CD19 varchar, PRCDR_DT19 varchar, 
ICD_PRCDR_CD20 varchar, PRCDR_DT20 varchar, ICD_PRCDR_CD21 varchar, PRCDR_DT21 varchar, ICD_PRCDR_CD22 varchar, PRCDR_DT22 varchar, 
ICD_PRCDR_CD23 varchar, PRCDR_DT23 varchar, ICD_PRCDR_CD24 varchar, PRCDR_DT24 varchar, ICD_PRCDR_CD25 varchar, PRCDR_DT25 varchar, 
DOB_DT varchar, GNDR_CD varchar, BENE_RACE_CD varchar, BENE_CNTY_CD varchar, BENE_STATE_CD varchar, BENE_MLG_CNTCT_ZIP_CD varchar, 
CLM_MDCL_REC varchar, CLM_TRTMT_AUTHRZTN_NUM varchar, CLM_PRCR_RTRN_CD varchar, CLM_SRVC_FAC_ZIP_CD varchar, 
NCH_PROFNL_CMPNT_CHRG_AMT varchar, CLM_NEXT_GNRTN_ACO_IND_CD1 varchar, CLM_NEXT_GNRTN_ACO_IND_CD2 varchar, CLM_NEXT_GNRTN_ACO_IND_CD3 varchar, 
CLM_NEXT_GNRTN_ACO_IND_CD4 varchar, CLM_NEXT_GNRTN_ACO_IND_CD5 varchar, ACO_ID_NUM varchar, CLM_BENE_ID_TYPE_CD varchar
) 
LOCATION ( 
'gpfdist://c252-140:8801/medicare/201*/snf_base_claims_k.csv'
)
FORMAT 'CSV' ( HEADER DELIMITER ',' );

select *
from ext_snf_base_claims_k
limit 1000;

create table medicare.snf_base_claims_k
WITH (appendonly=true, orientation=column)
as
select * 
from ext_snf_base_claims_k
distributed randomly;

select count(*)
from medicare.snf_base_claims_k;