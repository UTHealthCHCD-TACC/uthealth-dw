drop external table ext_bcarrier_line_k;

CREATE EXTERNAL TABLE ext_bcarrier_line_k (
BENE_ID varchar, CLM_ID varchar, LINE_NUM varchar, NCH_CLM_TYPE_CD varchar, CLM_THRU_DT varchar, CARR_PRFRNG_PIN_NUM varchar, 
PRF_PHYSN_UPIN varchar, PRF_PHYSN_NPI varchar, ORG_NPI_NUM varchar, CARR_LINE_PRVDR_TYPE_CD varchar, TAX_NUM varchar, PRVDR_STATE_CD varchar, 
PRVDR_ZIP varchar, PRVDR_SPCLTY varchar, PRTCPTNG_IND_CD varchar, CARR_LINE_RDCD_PMT_PHYS_ASTN_C varchar, LINE_SRVC_CNT varchar, 
LINE_CMS_TYPE_SRVC_CD varchar, LINE_PLACE_OF_SRVC_CD varchar, CARR_LINE_PRCNG_LCLTY_CD varchar, LINE_1ST_EXPNS_DT varchar, 
LINE_LAST_EXPNS_DT varchar, HCPCS_CD varchar, HCPCS_1ST_MDFR_CD varchar, HCPCS_2ND_MDFR_CD varchar, BETOS_CD varchar, 
LINE_NCH_PMT_AMT varchar, LINE_BENE_PMT_AMT varchar, LINE_PRVDR_PMT_AMT varchar, LINE_BENE_PTB_DDCTBL_AMT varchar, 
LINE_BENE_PRMRY_PYR_CD varchar, LINE_BENE_PRMRY_PYR_PD_AMT varchar, LINE_COINSRNC_AMT varchar, LINE_SBMTD_CHRG_AMT varchar, 
LINE_ALOWD_CHRG_AMT varchar, LINE_PRCSG_IND_CD varchar, LINE_PMT_80_100_CD varchar, LINE_SERVICE_DEDUCTIBLE varchar, 
CARR_LINE_MTUS_CNT varchar, CARR_LINE_MTUS_CD varchar, LINE_ICD_DGNS_CD varchar, LINE_ICD_DGNS_VRSN_CD varchar, HPSA_SCRCTY_IND_CD varchar, 
CARR_LINE_RX_NUM varchar, LINE_HCT_HGB_RSLT_NUM varchar, LINE_HCT_HGB_TYPE_CD varchar, LINE_NDC_CD varchar, CARR_LINE_CLIA_LAB_NUM varchar, 
CARR_LINE_ANSTHSA_UNIT_CNT varchar, CARR_LINE_CL_CHRG_AMT varchar, PHYSN_ZIP_CD varchar, LINE_OTHR_APLD_IND_CD1 varchar, 
LINE_OTHR_APLD_IND_CD2 varchar, LINE_OTHR_APLD_IND_CD3 varchar, LINE_OTHR_APLD_IND_CD4 varchar, LINE_OTHR_APLD_IND_CD5 varchar, 
LINE_OTHR_APLD_IND_CD6 varchar, LINE_OTHR_APLD_IND_CD7 varchar, LINE_OTHR_APLD_AMT1 varchar, LINE_OTHR_APLD_AMT2 varchar, 
LINE_OTHR_APLD_AMT3 varchar, LINE_OTHR_APLD_AMT4 varchar, LINE_OTHR_APLD_AMT5 varchar, LINE_OTHR_APLD_AMT6 varchar, 
LINE_OTHR_APLD_AMT7 varchar, THRPY_CAP_IND_CD1 varchar, THRPY_CAP_IND_CD2 varchar, THRPY_CAP_IND_CD3 varchar, THRPY_CAP_IND_CD4 varchar, 
THRPY_CAP_IND_CD5 varchar, CLM_NEXT_GNRTN_ACO_IND_CD1 varchar, CLM_NEXT_GNRTN_ACO_IND_CD2 varchar, CLM_NEXT_GNRTN_ACO_IND_CD3 varchar, 
CLM_NEXT_GNRTN_ACO_IND_CD4 varchar, CLM_NEXT_GNRTN_ACO_IND_CD5 varchar,CARR_LINE_MDPP_NPI_NUM varchar
) 
LOCATION ( 
'gpfdist://c252-140:8801/medicare/201*/bcarrier_line_k.csv'
)
FORMAT 'CSV' ( HEADER DELIMITER ',' );

select *
from ext_bcarrier_line_k
limit 1000;

create table medicare.bcarrier_line_k
WITH (appendonly=true, orientation=column)
as
select * 
from ext_bcarrier_line_k
distributed randomly;

select count(*)
from medicare.bcarrier_line_k;