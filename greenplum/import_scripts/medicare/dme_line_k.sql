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
'gpfdist://greenplum01:8081/uthealth/medicare_national/*/*dme_line_k.csv.gz#transform=add_parentname_filename_comma'
)
FORMAT 'CSV' ( HEADER DELIMITER ',' );

select *
from ext_dme_line_k
limit 1000;

create table uthealth/medicare_national.dme_line_k
WITH (appendonly=true, orientation=column, compresstype=zlib)
as

--insert into uthealth/medicare_national.dme_line_k 
select * 
from ext_dme_line_k

distributed randomly;

-- 2018 & 2019
/*
c252-136.wrangler(71)$ gzip -cd 2018/dme_line_k.csv.gz | head -n1                                                                                                                             
BENE_ID,CLM_ID,LINE_NUM,NCH_CLM_TYPE_CD,CLM_THRU_DT,TAX_NUM,PRVDR_SPCLTY,
PRTCPTNG_IND_CD,LINE_SRVC_CNT,LINE_CMS_TYPE_SRVC_CD,LINE_PLACE_OF_SRVC_CD,LINE_1ST_EXPNS_DT,
LINE_LAST_EXPNS_DT,HCPCS_CD,HCPCS_1ST_MDFR_CD,HCPCS_2ND_MDFR_CD,BETOS_CD,LINE_NCH_PMT_AMT,
LINE_BENE_PMT_AMT,LINE_PRVDR_PMT_AMT,LINE_BENE_PTB_DDCTBL_AMT,LINE_BENE_PRMRY_PYR_CD,
LINE_BENE_PRMRY_PYR_PD_AMT,LINE_COINSRNC_AMT,LINE_PRMRY_ALOWD_CHRG_AMT,LINE_SBMTD_CHRG_AMT,
LINE_ALOWD_CHRG_AMT,LINE_PRCSG_IND_CD,LINE_PMT_80_100_CD,LINE_SERVICE_DEDUCTIBLE,
LINE_ICD_DGNS_CD,LINE_ICD_DGNS_VRSN_CD,LINE_DME_PRCHS_PRICE_AMT,PRVDR_NUM,PRVDR_NPI,
DMERC_LINE_PRCNG_STATE_CD,PRVDR_STATE_CD,DMERC_LINE_SUPPLR_TYPE_CD,HCPCS_3RD_MDFR_CD,
HCPCS_4TH_MDFR_CD,DMERC_LINE_SCRN_SVGS_AMT,DMERC_LINE_MTUS_CNT,DMERC_LINE_MTUS_CD
,LINE_HCT_HGB_RSLT_NUM,LINE_HCT_HGB_TYPE_CD,LINE_NDC_CD,LINE_OTHR_APLD_IND_CD1,
LINE_OTHR_APLD_IND_CD2,LINE_OTHR_APLD_IND_CD3,LINE_OTHR_APLD_IND_CD4,LINE_OTHR_APLD_IND_CD5,
LINE_OTHR_APLD_IND_CD6,LINE_OTHR_APLD_IND_CD7,LINE_OTHR_APLD_AMT1,LINE_OTHR_APLD_AMT2,
LINE_OTHR_APLD_AMT3,LINE_OTHR_APLD_AMT4,LINE_OTHR_APLD_AMT5,LINE_OTHR_APLD_AMT6,LINE_OTHR_APLD_AMT7,
LINE_RSDL_PYMT_IND_CD,LINE_RP_IND_CD,DMERC_LINE_FRGN_ADR_IND,LINE_RR_BRD_EXCLSN_IND_SW
 */

alter table medicare_national.dme_line_k add column LINE_RSDL_PYMT_IND_CD varchar;
alter table medicare_national.dme_line_k add column LINE_RP_IND_CD varchar;
alter table medicare_national.dme_line_k add column DMERC_LINE_FRGN_ADR_IND varchar;
alter table medicare_national.dme_line_k add column LINE_RR_BRD_EXCLSN_IND_SW varchar;
alter table medicare_national.dme_line_k add column LINE_VLNTRY_SRVC_IND_CD varchar;

drop external table ext_dme_line_k;

CREATE EXTERNAL TABLE ext_dme_line_k (
year text, filename text,
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
LINE_OTHR_APLD_AMT3 varchar, LINE_OTHR_APLD_AMT4 varchar, LINE_OTHR_APLD_AMT5 varchar, LINE_OTHR_APLD_AMT6 varchar, LINE_OTHR_APLD_AMT7 varchar,
LINE_RSDL_PYMT_IND_CD varchar, LINE_RP_IND_CD varchar, DMERC_LINE_FRGN_ADR_IND varchar, LINE_RR_BRD_EXCLSN_IND_SW varchar,
LINE_VLNTRY_SRVC_IND_CD varchar
) 
LOCATION ( 
'gpfdist://greenplum01:8081/uthealth/medicare_national/*/DME_LINE.CSV#transform=add_parentname_filename_comma'
)
FORMAT 'CSV' ( HEADER DELIMITER ',' );

select *
from ext_dme_line_k
limit 1000;

insert into medicare_national.dme_line_k (year,
BENE_ID,CLM_ID,LINE_NUM,NCH_CLM_TYPE_CD,CLM_THRU_DT,TAX_NUM,PRVDR_SPCLTY,
PRTCPTNG_IND_CD,LINE_SRVC_CNT,LINE_CMS_TYPE_SRVC_CD,LINE_PLACE_OF_SRVC_CD,LINE_1ST_EXPNS_DT,
LINE_LAST_EXPNS_DT,HCPCS_CD,HCPCS_1ST_MDFR_CD,HCPCS_2ND_MDFR_CD,BETOS_CD,LINE_NCH_PMT_AMT,
LINE_BENE_PMT_AMT,LINE_PRVDR_PMT_AMT,LINE_BENE_PTB_DDCTBL_AMT,LINE_BENE_PRMRY_PYR_CD,
LINE_BENE_PRMRY_PYR_PD_AMT,LINE_COINSRNC_AMT,LINE_PRMRY_ALOWD_CHRG_AMT,LINE_SBMTD_CHRG_AMT,
LINE_ALOWD_CHRG_AMT,LINE_PRCSG_IND_CD,LINE_PMT_80_100_CD,LINE_SERVICE_DEDUCTIBLE,
LINE_ICD_DGNS_CD,LINE_ICD_DGNS_VRSN_CD,LINE_DME_PRCHS_PRICE_AMT,PRVDR_NUM,PRVDR_NPI,
DMERC_LINE_PRCNG_STATE_CD,PRVDR_STATE_CD,DMERC_LINE_SUPPLR_TYPE_CD,HCPCS_3RD_MDFR_CD,
HCPCS_4TH_MDFR_CD,DMERC_LINE_SCRN_SVGS_AMT,DMERC_LINE_MTUS_CNT,DMERC_LINE_MTUS_CD
,LINE_HCT_HGB_RSLT_NUM,LINE_HCT_HGB_TYPE_CD,LINE_NDC_CD,LINE_OTHR_APLD_IND_CD1,
LINE_OTHR_APLD_IND_CD2,LINE_OTHR_APLD_IND_CD3,LINE_OTHR_APLD_IND_CD4,LINE_OTHR_APLD_IND_CD5,
LINE_OTHR_APLD_IND_CD6,LINE_OTHR_APLD_IND_CD7,LINE_OTHR_APLD_AMT1,LINE_OTHR_APLD_AMT2,
LINE_OTHR_APLD_AMT3,LINE_OTHR_APLD_AMT4,LINE_OTHR_APLD_AMT5,LINE_OTHR_APLD_AMT6,LINE_OTHR_APLD_AMT7,
LINE_RSDL_PYMT_IND_CD,LINE_RP_IND_CD,DMERC_LINE_FRGN_ADR_IND,LINE_RR_BRD_EXCLSN_IND_SW,
LINE_VLNTRY_SRVC_IND_CD)
select year,
BENE_ID,CLM_ID,LINE_NUM,NCH_CLM_TYPE_CD,CLM_THRU_DT,TAX_NUM,PRVDR_SPCLTY,
PRTCPTNG_IND_CD,LINE_SRVC_CNT,LINE_CMS_TYPE_SRVC_CD,LINE_PLACE_OF_SRVC_CD,LINE_1ST_EXPNS_DT,
LINE_LAST_EXPNS_DT,HCPCS_CD,HCPCS_1ST_MDFR_CD,HCPCS_2ND_MDFR_CD,BETOS_CD,LINE_NCH_PMT_AMT,
LINE_BENE_PMT_AMT,LINE_PRVDR_PMT_AMT,LINE_BENE_PTB_DDCTBL_AMT,LINE_BENE_PRMRY_PYR_CD,
LINE_BENE_PRMRY_PYR_PD_AMT,LINE_COINSRNC_AMT,LINE_PRMRY_ALOWD_CHRG_AMT,LINE_SBMTD_CHRG_AMT,
LINE_ALOWD_CHRG_AMT,LINE_PRCSG_IND_CD,LINE_PMT_80_100_CD,LINE_SERVICE_DEDUCTIBLE,
LINE_ICD_DGNS_CD,LINE_ICD_DGNS_VRSN_CD,LINE_DME_PRCHS_PRICE_AMT,PRVDR_NUM,PRVDR_NPI,
DMERC_LINE_PRCNG_STATE_CD,PRVDR_STATE_CD,DMERC_LINE_SUPPLR_TYPE_CD,HCPCS_3RD_MDFR_CD,
HCPCS_4TH_MDFR_CD,DMERC_LINE_SCRN_SVGS_AMT,DMERC_LINE_MTUS_CNT,DMERC_LINE_MTUS_CD
,LINE_HCT_HGB_RSLT_NUM,LINE_HCT_HGB_TYPE_CD,LINE_NDC_CD,LINE_OTHR_APLD_IND_CD1,
LINE_OTHR_APLD_IND_CD2,LINE_OTHR_APLD_IND_CD3,LINE_OTHR_APLD_IND_CD4,LINE_OTHR_APLD_IND_CD5,
LINE_OTHR_APLD_IND_CD6,LINE_OTHR_APLD_IND_CD7,LINE_OTHR_APLD_AMT1,LINE_OTHR_APLD_AMT2,
LINE_OTHR_APLD_AMT3,LINE_OTHR_APLD_AMT4,LINE_OTHR_APLD_AMT5,LINE_OTHR_APLD_AMT6,LINE_OTHR_APLD_AMT7,
LINE_RSDL_PYMT_IND_CD,LINE_RP_IND_CD,DMERC_LINE_FRGN_ADR_IND,LINE_RR_BRD_EXCLSN_IND_SW,
LINE_VLNTRY_SRVC_IND_CD
from ext_dme_line_k;

-- Scratch
select year, count(*)
from uthealth/medicare_national.dme_line_k
group by 1
order by 1;