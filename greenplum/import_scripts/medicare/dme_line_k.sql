drop external table ext_dme_line_k;

CREATE EXTERNAL TABLE ext_dme_line_k (
year text,
BENE_ID varchar, CLM_ID varchar, LINE_NUM varchar, NCH_CLM_TYPE_CD varchar, CLM_THRU_DT varchar, TAX_NUM varchar, PRVDR_SPCLTY varchar, 
PRTCPTNG_IND_CD varchar, LINE_SRVC_CNT varchar, LINE_CMS_TYPE_SRVC_CD varchar, LINE_PLACE_OF_SRVC_CD varchar, LINE_1ST_EXPNS_DT varchar, 
LINE_LAST_EXPNS_DT varchar, HCPCS_CD varchar, HCPCS_1ST_MDFR_CD varchar, HCPCS_2ND_MDFR_CD varchar, BETOS_CD varchar, LINE_NCH_PMT_AMT varchar, 
LINE_BENE_PMT_AMT varchar, LINE_PRVDR_PMT_AMT varchar, LINE_BENE_PTB_DDCTBL_AMT varchar, LINE_BENE_PRMRY_PYR_CD varchar, 
LINE_BENE_PRMRY_PYR_PD_AMT varchar, LINE_COINSRNC_AMT varchar, LINE_PRMRY_ALOWD_CHRG_AMT varchar, LINE_SBMTD_CHRG_AMT varchar, 
LINE_ALOWD_CHRG_AMT varchar, LINE_PRCSG_IND_CD varchar, LINE_PMT_80_100_CD varchar, LINE_SERVICE_DEDUCTIBLE varchar, 
LINE_ICD_DGNS_CD varchar, LINE_ICD_DGNS_VRSN_CD varchar, LINE_DME_PRCHS_PRICE_AMT varchar, PRVDR_NUM varchar, PRVDR_NPI varchar, 
DMERC_LINE_PRCNG_STATE_CD varchar, PRVDR_STATE_CD varchar, DMERC_LINE_SUPPLR_TYPE_CD varchar, HCPCS_3RD_MDFR_CD varchar, 
HCPCS_4TH_MDFR_CD varchar, DMERC_LINE_SCRN_SVGS_AMT varchar, DMERC_LINE_MTUS_CNT varchar, DMERC_LINE_MTUS_CD varchar, 
LINE_HCT_HGB_RSLT_NUM varchar, LINE_HCT_HGB_TYPE_CD varchar, LINE_NDC_CD varchar, LINE_OTHR_APLD_IND_CD1 varchar, 
LINE_OTHR_APLD_IND_CD2 varchar, LINE_OTHR_APLD_IND_CD3 varchar, LINE_OTHR_APLD_IND_CD4 varchar, LINE_OTHR_APLD_IND_CD5 varchar, 
LINE_OTHR_APLD_IND_CD6 varchar, LINE_OTHR_APLD_IND_CD7 varchar, LINE_OTHR_APLD_AMT1 varchar, LINE_OTHR_APLD_AMT2 varchar, 
LINE_OTHR_APLD_AMT3 varchar, LINE_OTHR_APLD_AMT4 varchar, LINE_OTHR_APLD_AMT5 varchar, LINE_OTHR_APLD_AMT6 varchar, LINE_OTHR_APLD_AMT7 varchar
) 
LOCATION ( 
'gpfdist://192.168.58.179:8081/medicare_texas/*/*dme_line_k.csv.gz#transform=add_parentname'
)
FORMAT 'CSV' ( HEADER DELIMITER ',' );

select *
from ext_dme_line_k
limit 1000;

create table medicare_texas.dme_line_k
WITH (appendonly=true, orientation=column, compresstype=zlib)
as

--insert into medicare_texas.dme_line_k 
select * 
from ext_dme_line_k

distributed randomly;

select count(*)
from medicare_texas.dme_line_k;