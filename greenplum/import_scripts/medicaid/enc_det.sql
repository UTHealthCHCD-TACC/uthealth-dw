--Medical
drop table medicaid.enc_det;
create table medicaid.enc_det (
year_fy smallint, file varchar, 
MCO_ICN varchar,TX_CD varchar,SUB_MCO_PLN varchar,LN_NBR varchar,ENC_STAT_CD varchar,FDOS_DT varchar,TDOS_CSL varchar,
PROC_CD varchar,SUB_CHRG_AMT varchar,DT_PD_AMT varchar,DT_LN_UNT varchar,PD_UNT_SVC varchar,
DX_CD_1 varchar,DX_CD_2 varchar,DX_CD_3 varchar,DX_CD_4 varchar,
PROC_MOD_CD_1 varchar,PROC_MOD_CD_2 varchar,PROC_MOD_CD_3 varchar,PROC_MOD_CD_4 varchar,
POS varchar,TOS varchar,REV_CD varchar,SUB_OPT_PHY_NPI varchar,SUB_REF_PROV_NPI varchar,
SUB_REND_PROV_NPI varchar,SUB_REND_PRV_TAX_CD varchar,SUB_REND_PROV_NBR varchar,REND_PROV_ID varchar,REND_PROV_SFX varchar,
DT_CRE_TY_CD varchar,DT_LN_UNT_MS varchar,DT_FNC_CD varchar,DERV_ENC varchar
) 
WITH (appendonly=true, orientation=column, compresstype=zlib)
distributed by (MCO_ICN);

drop external table ext_enc_det;
CREATE EXTERNAL TABLE ext_enc_det (
year_fy smallint, filename varchar,
MCO_ICN varchar,TX_CD varchar,SUB_MCO_PLN varchar,LN_NBR varchar,ENC_STAT_CD varchar,FDOS_DT varchar,TDOS_CSL varchar,
PROC_CD varchar,SUB_CHRG_AMT varchar,DT_PD_AMT varchar,DT_LN_UNT varchar,PD_UNT_SVC varchar,
DX_CD_1 varchar,DX_CD_2 varchar,DX_CD_3 varchar,DX_CD_4 varchar,
PROC_MOD_CD_1 varchar,PROC_MOD_CD_2 varchar,PROC_MOD_CD_3 varchar,PROC_MOD_CD_4 varchar,
POS varchar,TOS varchar,REV_CD varchar,SUB_OPT_PHY_NPI varchar,SUB_REF_PROV_NPI varchar,
SUB_REND_PROV_NPI varchar,SUB_REND_PRV_TAX_CD varchar,SUB_REND_PROV_NBR varchar,REND_PROV_ID varchar,REND_PROV_SFX varchar,
DT_CRE_TY_CD varchar,DT_LN_UNT_MS varchar,DT_FNC_CD varchar,DERV_ENC varchar
) 
LOCATION ( 
'gpfdist://greenplum01:8081/uthealth/medicaid/*/ENC_DET_*.csv#transform=add_parentname_filename_comma'
)
FORMAT 'CSV' ( HEADER DELIMITER ',' );

-- Test
/*
select *
from ext_enc_det
limit 10;
*/
-- Insert
insert into medicaid.enc_det
select * from ext_enc_det;

-- 318 secs
update medicaid.enc_det set year_fy=date_part('year_fy', FST_DT) where year_fy=0;


-- Analyze
analyze medicaid.enc_det;
 
-- Verify
select count(*), min(year_fy), max(year_fy), count(distinct year_fy) from medicaid.enc_det;


select year_fy, date_part('quarter', FST_DT) as quarter, count(*), min(FST_DT), max(FST_DT)
from medicaid.enc_det
group by 1, 2
order by 1, 2;
