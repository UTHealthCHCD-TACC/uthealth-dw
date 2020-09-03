drop external table ext_hospice_base_claims_k;

CREATE EXTERNAL TABLE ext_hospice_base_claims_k (
year text,
BENE_ID varchar, CLM_ID varchar, NCH_NEAR_LINE_REC_IDENT_CD varchar, NCH_CLM_TYPE_CD varchar, CLM_FROM_DT varchar, CLM_THRU_DT varchar, 
NCH_WKLY_PROC_DT varchar, FI_CLM_PROC_DT varchar, PRVDR_NUM varchar, CLM_FAC_TYPE_CD varchar, CLM_SRVC_CLSFCTN_TYPE_CD varchar, 
CLM_FREQ_CD varchar, FI_NUM varchar, CLM_MDCR_NON_PMT_RSN_CD varchar, CLM_PMT_AMT varchar, NCH_PRMRY_PYR_CLM_PD_AMT varchar, 
NCH_PRMRY_PYR_CD varchar, PRVDR_STATE_CD varchar, ORG_NPI_NUM varchar, SRVC_LOC_NPI_NUM varchar, AT_PHYSN_UPIN varchar, 
AT_PHYSN_NPI varchar, AT_PHYSN_SPCLTY_CD varchar, OP_PHYSN_NPI varchar, OP_PHYSN_SPCLTY_CD varchar, OT_PHYSN_NPI varchar, 
OT_PHYSN_SPCLTY_CD varchar, RNDRNG_PHYSN_NPI varchar, RNDRNG_PHYSN_SPCLTY_CD varchar, RFR_PHYSN_NPI varchar, RFR_PHYSN_SPCLTY_CD varchar, 
PTNT_DSCHRG_STUS_CD varchar, CLM_TOT_CHRG_AMT varchar, NCH_PTNT_STATUS_IND_CD varchar, CLM_UTLZTN_DAY_CNT varchar, NCH_BENE_DSCHRG_DT varchar, 
PRNCPAL_DGNS_CD varchar, ICD_DGNS_CD1 varchar, ICD_DGNS_CD2 varchar, ICD_DGNS_CD3 varchar, ICD_DGNS_CD4 varchar, ICD_DGNS_CD5 varchar, 
ICD_DGNS_CD6 varchar, ICD_DGNS_CD7 varchar, ICD_DGNS_CD8 varchar, ICD_DGNS_CD9 varchar, ICD_DGNS_CD10 varchar, ICD_DGNS_CD11 varchar, 
ICD_DGNS_CD12 varchar, ICD_DGNS_CD13 varchar, ICD_DGNS_CD14 varchar, ICD_DGNS_CD15 varchar, ICD_DGNS_CD16 varchar, ICD_DGNS_CD17 varchar, 
ICD_DGNS_CD18 varchar, ICD_DGNS_CD19 varchar, ICD_DGNS_CD20 varchar, ICD_DGNS_CD21 varchar, ICD_DGNS_CD22 varchar, ICD_DGNS_CD23 varchar, 
ICD_DGNS_CD24 varchar, ICD_DGNS_CD25 varchar, FST_DGNS_E_CD varchar, ICD_DGNS_E_CD1 varchar, ICD_DGNS_E_CD2 varchar, ICD_DGNS_E_CD3 varchar, 
ICD_DGNS_E_CD4 varchar, ICD_DGNS_E_CD5 varchar, ICD_DGNS_E_CD6 varchar, ICD_DGNS_E_CD7 varchar, ICD_DGNS_E_CD8 varchar, ICD_DGNS_E_CD9 varchar, 
ICD_DGNS_E_CD10 varchar, ICD_DGNS_E_CD11 varchar, ICD_DGNS_E_CD12 varchar, CLM_HOSPC_START_DT_ID varchar, BENE_HOSPC_PRD_CNT varchar, 
DOB_DT varchar, GNDR_CD varchar, BENE_RACE_CD varchar, BENE_CNTY_CD varchar, BENE_STATE_CD varchar, BENE_MLG_CNTCT_ZIP_CD varchar, 
CLM_MDCL_REC varchar, CLAIM_QUERY_CODE varchar, FI_CLM_ACTN_CD varchar, CLM_TRTMT_AUTHRZTN_NUM varchar, CLM_PRCR_RTRN_CD varchar, 
CLM_SRVC_FAC_ZIP_CD varchar, CLM_NEXT_GNRTN_ACO_IND_CD1 varchar, CLM_NEXT_GNRTN_ACO_IND_CD2 varchar, CLM_NEXT_GNRTN_ACO_IND_CD3 varchar, 
CLM_NEXT_GNRTN_ACO_IND_CD4 varchar, CLM_NEXT_GNRTN_ACO_IND_CD5 varchar, ACO_ID_NUM varchar, CLM_BENE_ID_TYPE_CD varchar
) 
LOCATION ( 
'gpfdist://192.168.58.179:8081/medicare/*/*hospice_base_claims_k.csv.gz#transform=add_parentname'
)
FORMAT 'CSV' ( HEADER DELIMITER ',' );

select *
from ext_hospice_base_claims_k
limit 1000;

create table medicare.hospice_base_claims_k
WITH (appendonly=true, orientation=column, compresstype=zlib)
as

--insert into medicare.hospice_base_claims_k 
select * 
from ext_hospice_base_claims_k

distributed randomly;

select count(*)
from medicare.hospice_base_claims_k;