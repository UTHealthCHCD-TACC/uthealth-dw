--Medical
drop table medicaid.enc_dx;
create table medicaid.enc_dx (
year smallint, file varchar, 
MCO_ICN varchar,TX_CD varchar,SUB_MCO_PLN varchar,PRIM_DX_QAL varchar,PRIM_DX_CD varchar,
PRM_DX_CAT varchar,ADM_DX_CD varchar,DX_CD_QAL_1 varchar,
DX_CD_1 varchar,DX_CD_QAL_2 varchar,DX_CD_2 varchar,DX_CD_QAL_3 varchar,DX_CD_3 varchar,
DX_CD_QAL_4 varchar,DX_CD_4 varchar,DX_CD_QAL_5 varchar,DX_CD_5 varchar,DX_CD_QAL_6 varchar,DX_CD_6 varchar,
DX_CD_QAL_7 varchar,DX_CD_7 varchar,DX_CD_QAL_8 varchar,DX_CD_8 varchar,DX_CD_QAL_9 varchar,DX_CD_9 varchar,
DX_CD_QAL_10 varchar,DX_CD_10 varchar,DX_CD_QAL_11 varchar,DX_CD_11 varchar,DX_CD_QAL_12 varchar,DX_CD_12 varchar,
DX_CD_QAL_13 varchar,DX_CD_13 varchar,DX_CD_QAL_14 varchar,DX_CD_14 varchar,DX_CD_QAL_15 varchar,DX_CD_15 varchar,
DX_CD_QAL_16 varchar,DX_CD_16 varchar,DX_CD_QAL_17 varchar,DX_CD_17 varchar,DX_CD_QAL_18 varchar,DX_CD_18 varchar,
DX_CD_QAL_19 varchar,DX_CD_19 varchar,DX_CD_QAL_20 varchar,DX_CD_20 varchar,DX_CD_QAL_21 varchar,DX_CD_21 varchar,
DX_CD_QAL_22 varchar,DX_CD_22 varchar,DX_CD_QAL_23 varchar,DX_CD_23 varchar,DX_CD_QAL_24 varchar,DX_CD_24 varchar,
PRM_DX_POA varchar,DX_POA_1 varchar,DX_POA_2 varchar,DX_POA_3 varchar,DX_POA_4 varchar,DX_POA_5 varchar,DX_POA_6 varchar,
DX_POA_7 varchar,DX_POA_8 varchar,DX_POA_9 varchar,DX_POA_10 varchar,DX_POA_11 varchar,DX_POA_12 varchar,
DX_POA_13 varchar,DX_POA_14 varchar,DX_POA_15 varchar,DX_POA_16 varchar,DX_POA_17 varchar,DX_POA_18 varchar,
DX_POA_19 varchar,DX_POA_20 varchar,DX_POA_21 varchar,DX_POA_22 varchar,DX_POA_23 varchar,DX_POA_24 varchar,DERV_ENC varchar
) 
WITH (appendonly=true, orientation=column, compresstype=zlib)
distributed by (MCO_ICN);

drop external table ext_enc_dx;
CREATE EXTERNAL TABLE ext_enc_dx (
year smallint, filename varchar,
MCO_ICN varchar,TX_CD varchar,SUB_MCO_PLN varchar,PRIM_DX_QAL varchar,PRIM_DX_CD varchar,
PRM_DX_CAT varchar,ADM_DX_CD varchar,DX_CD_QAL_1 varchar,
DX_CD_1 varchar,DX_CD_QAL_2 varchar,DX_CD_2 varchar,DX_CD_QAL_3 varchar,DX_CD_3 varchar,
DX_CD_QAL_4 varchar,DX_CD_4 varchar,DX_CD_QAL_5 varchar,DX_CD_5 varchar,DX_CD_QAL_6 varchar,DX_CD_6 varchar,
DX_CD_QAL_7 varchar,DX_CD_7 varchar,DX_CD_QAL_8 varchar,DX_CD_8 varchar,DX_CD_QAL_9 varchar,DX_CD_9 varchar,
DX_CD_QAL_10 varchar,DX_CD_10 varchar,DX_CD_QAL_11 varchar,DX_CD_11 varchar,DX_CD_QAL_12 varchar,DX_CD_12 varchar,
DX_CD_QAL_13 varchar,DX_CD_13 varchar,DX_CD_QAL_14 varchar,DX_CD_14 varchar,DX_CD_QAL_15 varchar,DX_CD_15 varchar,
DX_CD_QAL_16 varchar,DX_CD_16 varchar,DX_CD_QAL_17 varchar,DX_CD_17 varchar,DX_CD_QAL_18 varchar,DX_CD_18 varchar,
DX_CD_QAL_19 varchar,DX_CD_19 varchar,DX_CD_QAL_20 varchar,DX_CD_20 varchar,DX_CD_QAL_21 varchar,DX_CD_21 varchar,
DX_CD_QAL_22 varchar,DX_CD_22 varchar,DX_CD_QAL_23 varchar,DX_CD_23 varchar,DX_CD_QAL_24 varchar,DX_CD_24 varchar,
PRM_DX_POA varchar,DX_POA_1 varchar,DX_POA_2 varchar,DX_POA_3 varchar,DX_POA_4 varchar,DX_POA_5 varchar,DX_POA_6 varchar,
DX_POA_7 varchar,DX_POA_8 varchar,DX_POA_9 varchar,DX_POA_10 varchar,DX_POA_11 varchar,DX_POA_12 varchar,
DX_POA_13 varchar,DX_POA_14 varchar,DX_POA_15 varchar,DX_POA_16 varchar,DX_POA_17 varchar,DX_POA_18 varchar,
DX_POA_19 varchar,DX_POA_20 varchar,DX_POA_21 varchar,DX_POA_22 varchar,DX_POA_23 varchar,DX_POA_24 varchar,DERV_ENC varchar
) 
LOCATION ( 
'gpfdist://greenplum01:8081/uthealth/medicaid/*/ENC_DX_*.csv#transform=add_parentname_filename_comma'
)
FORMAT 'CSV' ( HEADER DELIMITER ',' );

-- Test
/*
select *
from ext_enc_dx
limit 10;
*/
-- Insert
insert into medicaid.enc_dx
select * from ext_enc_dx;

-- 318 secs
update medicaid.enc_dx set year=date_part('year', FST_DT) where year=0;


-- Analyze
analyze medicaid.enc_dx;
 
-- Verify
select count(*), min(year), max(year), count(distinct year) from medicaid.enc_dx;


select year, date_part('quarter', FST_DT) as quarter, count(*), min(FST_DT), max(FST_DT)
from medicaid.enc_dx
group by 1, 2
order by 1, 2;
