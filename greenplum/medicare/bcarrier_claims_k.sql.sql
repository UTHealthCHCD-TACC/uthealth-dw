drop external table ext_bcarrier_claims_k;

CREATE EXTERNAL TABLE ext_bcarrier_claims_k (
BENE_ID varchar,CLM_ID varchar,NCH_NEAR_LINE_REC_IDENT_CD varchar,NCH_CLM_TYPE_CD varchar,CLM_FROM_DT varchar,CLM_THRU_DT varchar,
NCH_WKLY_PROC_DT varchar,CARR_CLM_ENTRY_CD varchar,CLM_DISP_CD varchar,CARR_NUM varchar,CARR_CLM_PMT_DNL_CD varchar,CLM_PMT_AMT varchar,
CARR_CLM_PRMRY_PYR_PD_AMT varchar,RFR_PHYSN_UPIN varchar,RFR_PHYSN_NPI varchar,CARR_CLM_PRVDR_ASGNMT_IND_SW varchar,
NCH_CLM_PRVDR_PMT_AMT varchar,NCH_CLM_BENE_PMT_AMT varchar,NCH_CARR_CLM_SBMTD_CHRG_AMT varchar,NCH_CARR_CLM_ALOWD_AMT varchar,
CARR_CLM_CASH_DDCTBL_APLD_AMT varchar,CARR_CLM_HCPCS_YR_CD varchar,CARR_CLM_RFRNG_PIN_NUM varchar,PRNCPAL_DGNS_CD varchar,
PRNCPAL_DGNS_VRSN_CD varchar,ICD_DGNS_CD1 varchar,ICD_DGNS_VRSN_CD1 varchar,ICD_DGNS_CD2 varchar,ICD_DGNS_VRSN_CD2 varchar,
ICD_DGNS_CD3 varchar,ICD_DGNS_VRSN_CD3 varchar,ICD_DGNS_CD4 varchar,ICD_DGNS_VRSN_CD4 varchar,ICD_DGNS_CD5 varchar,ICD_DGNS_VRSN_CD5 varchar,
ICD_DGNS_CD6 varchar,ICD_DGNS_VRSN_CD6 varchar,ICD_DGNS_CD7 varchar,ICD_DGNS_VRSN_CD7 varchar,ICD_DGNS_CD8 varchar,ICD_DGNS_VRSN_CD8 varchar,
ICD_DGNS_CD9 varchar,ICD_DGNS_VRSN_CD9 varchar,ICD_DGNS_CD10 varchar,ICD_DGNS_VRSN_CD10 varchar,ICD_DGNS_CD11 varchar,
ICD_DGNS_VRSN_CD11 varchar,ICD_DGNS_CD12 varchar,ICD_DGNS_VRSN_CD12 varchar,CLM_CLNCL_TRIL_NUM varchar,DOB_DT varchar,GNDR_CD varchar,
BENE_RACE_CD varchar,BENE_CNTY_CD varchar,BENE_STATE_CD varchar,BENE_MLG_CNTCT_ZIP_CD varchar,CLM_BENE_PD_AMT varchar,CPO_PRVDR_NUM varchar,
CPO_ORG_NPI_NUM varchar,CARR_CLM_BLG_NPI_NUM varchar,ACO_ID_NUM varchar,CARR_CLM_SOS_NPI_NUM varchar,CLM_BENE_ID_TYPE_CD varchar
)
location (
'gpfdist://c252-140:8801/medicare/2017/bcarrier_claims_k.csv'
)
FORMAT 'CSV' ( HEADER DELIMITER ',' );

select *
from ext_bcarrier_claims_k
limit 1000;