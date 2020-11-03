drop external table ext_snf_revenue_center_k;

CREATE EXTERNAL TABLE ext_snf_revenue_center_k (
year text,
BENE_ID varchar, CLM_ID varchar, CLM_THRU_DT varchar, CLM_LINE_NUM varchar, NCH_CLM_TYPE_CD varchar, REV_CNTR varchar, HCPCS_CD varchar, 
HCPCS_1ST_MDFR_CD varchar, HCPCS_2ND_MDFR_CD varchar, HCPCS_3RD_MDFR_CD varchar, REV_CNTR_UNIT_CNT varchar, REV_CNTR_RATE_AMT varchar, 
REV_CNTR_TOT_CHRG_AMT varchar, REV_CNTR_NCVRD_CHRG_AMT varchar, REV_CNTR_DDCTBL_COINSRNC_CD varchar, REV_CNTR_NDC_QTY varchar, 
REV_CNTR_NDC_QTY_QLFR_CD varchar, RNDRNG_PHYSN_UPIN varchar, RNDRNG_PHYSN_NPI varchar, RNDRNG_PHYSN_SPCLTY_CD varchar, 
REV_CNTR_IDE_NDC_UPC_NUM varchar, REV_CNTR_PRCNG_IND_CD varchar, THRPY_CAP_IND_CD1 varchar, THRPY_CAP_IND_CD2 varchar
) 
LOCATION ( 
'gpfdist://192.168.58.179:8081/medicare_national/*/*snf_revenue_center_k.csv.gz#transform=add_parentname'
)
FORMAT 'CSV' ( HEADER DELIMITER ',' );

select *
from ext_snf_revenue_center_k
limit 1000;

create table medicare_national.snf_revenue_center_k
WITH (appendonly=true, orientation=column, compresstype=zlib)
as
--insert into medicare_national.snf_revenue_center_k 
select * 
from ext_snf_revenue_center_k
distributed randomly;

-- 2018+
-- New Cols: REV_CNTR_RP_IND_CD,RC_MODEL_REIMBRSMT_AMT

alter table medicare_national.snf_revenue_center_k add column REV_CNTR_RP_IND_CD varchar;
alter table medicare_national.snf_revenue_center_k add column RC_MODEL_REIMBRSMT_AMT varchar;

drop external table ext_snf_revenue_center_k;

CREATE EXTERNAL TABLE ext_snf_revenue_center_k (
year text, filename text, 
BENE_ID varchar, CLM_ID varchar, CLM_THRU_DT varchar, CLM_LINE_NUM varchar, NCH_CLM_TYPE_CD varchar, REV_CNTR varchar, HCPCS_CD varchar, 
HCPCS_1ST_MDFR_CD varchar, HCPCS_2ND_MDFR_CD varchar, HCPCS_3RD_MDFR_CD varchar, REV_CNTR_UNIT_CNT varchar, REV_CNTR_RATE_AMT varchar, 
REV_CNTR_TOT_CHRG_AMT varchar, REV_CNTR_NCVRD_CHRG_AMT varchar, REV_CNTR_DDCTBL_COINSRNC_CD varchar, REV_CNTR_NDC_QTY varchar, 
REV_CNTR_NDC_QTY_QLFR_CD varchar, RNDRNG_PHYSN_UPIN varchar, RNDRNG_PHYSN_NPI varchar, RNDRNG_PHYSN_SPCLTY_CD varchar, 
REV_CNTR_IDE_NDC_UPC_NUM varchar, REV_CNTR_PRCNG_IND_CD varchar, THRPY_CAP_IND_CD1 varchar, THRPY_CAP_IND_CD2 varchar,
REV_CNTR_RP_IND_CD varchar, RC_MODEL_REIMBRSMT_AMT varchar
) 
LOCATION ( 
'gpfdist://192.168.58.179:8081/medicare_national/*/*snf_revenue_center_k.csv.gz#transform=add_parentname_filename_comma'
)
FORMAT 'CSV' ( HEADER DELIMITER ',' );

select *
from ext_snf_revenue_center_k
limit 1000;

insert into medicare_national.snf_revenue_center_k (year, 
BENE_ID, CLM_ID, CLM_THRU_DT, CLM_LINE_NUM, NCH_CLM_TYPE_CD, REV_CNTR, HCPCS_CD, 
HCPCS_1ST_MDFR_CD, HCPCS_2ND_MDFR_CD, HCPCS_3RD_MDFR_CD, REV_CNTR_UNIT_CNT, REV_CNTR_RATE_AMT, 
REV_CNTR_TOT_CHRG_AMT, REV_CNTR_NCVRD_CHRG_AMT, REV_CNTR_DDCTBL_COINSRNC_CD, REV_CNTR_NDC_QTY, 
REV_CNTR_NDC_QTY_QLFR_CD, RNDRNG_PHYSN_UPIN, RNDRNG_PHYSN_NPI, RNDRNG_PHYSN_SPCLTY_CD, 
REV_CNTR_IDE_NDC_UPC_NUM, REV_CNTR_PRCNG_IND_CD, THRPY_CAP_IND_CD1, THRPY_CAP_IND_CD2,
REV_CNTR_RP_IND_CD, RC_MODEL_REIMBRSMT_AMT
)
select year,
BENE_ID, CLM_ID, CLM_THRU_DT, CLM_LINE_NUM, NCH_CLM_TYPE_CD, REV_CNTR, HCPCS_CD, 
HCPCS_1ST_MDFR_CD, HCPCS_2ND_MDFR_CD, HCPCS_3RD_MDFR_CD, REV_CNTR_UNIT_CNT, REV_CNTR_RATE_AMT, 
REV_CNTR_TOT_CHRG_AMT, REV_CNTR_NCVRD_CHRG_AMT, REV_CNTR_DDCTBL_COINSRNC_CD, REV_CNTR_NDC_QTY, 
REV_CNTR_NDC_QTY_QLFR_CD, RNDRNG_PHYSN_UPIN, RNDRNG_PHYSN_NPI, RNDRNG_PHYSN_SPCLTY_CD, 
REV_CNTR_IDE_NDC_UPC_NUM, REV_CNTR_PRCNG_IND_CD, THRPY_CAP_IND_CD1, THRPY_CAP_IND_CD2,
REV_CNTR_RP_IND_CD, RC_MODEL_REIMBRSMT_AMT
from ext_snf_revenue_center_k;


-- Scratch
select year, count(*)
from medicare_national.snf_revenue_center_k
group by 1
order by 1;