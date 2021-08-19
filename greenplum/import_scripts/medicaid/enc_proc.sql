--Medical
drop table medicaid.enc_proc;
create table medicaid.enc_proc (
year_fy smallint, file varchar, 
MCO_ICN varchar,TX_CD varchar,SUB_MCO_PLN varchar,PRIM_PROC_QAL varchar,PRIM_PROC_CD varchar,
PROC_ICD_QAL_1 varchar,PROC_ICD_CD_1 varchar,PROC_ICD_QAL_2 varchar,PROC_ICD_CD_2 varchar,PROC_ICD_QAL_3 varchar,PROC_ICD_CD_3 varchar,
PROC_ICD_QAL_4 varchar,PROC_ICD_CD_4 varchar,PROC_ICD_QAL_5 varchar,PROC_ICD_CD_5 varchar,PROC_ICD_QAL_6 varchar,PROC_ICD_CD_6 varchar,
PROC_ICD_QAL_7 varchar,PROC_ICD_CD_7 varchar,PROC_ICD_QAL_8 varchar,PROC_ICD_CD_8 varchar,PROC_ICD_QAL_9 varchar,PROC_ICD_CD_9 varchar,
PROC_ICD_QAL_10 varchar,PROC_ICD_CD_10 varchar,PROC_ICD_QAL_11 varchar,PROC_ICD_CD_11 varchar,PROC_ICD_QAL_12 varchar,PROC_ICD_CD_12 varchar,
PROC_ICD_QAL_13 varchar,PROC_ICD_CD_13 varchar,PROC_ICD_QAL_14 varchar,PROC_ICD_CD_14 varchar,PROC_ICD_QAL_15 varchar,PROC_ICD_CD_15 varchar,
PROC_ICD_QAL_16 varchar,PROC_ICD_CD_16 varchar,PROC_ICD_QAL_17 varchar,PROC_ICD_CD_17 varchar,PROC_ICD_QAL_18 varchar,PROC_ICD_CD_18 varchar,
PROC_ICD_QAL_19 varchar,PROC_ICD_CD_19 varchar,PROC_ICD_QAL_20 varchar,PROC_ICD_CD_20 varchar,PROC_ICD_QAL_21 varchar,PROC_ICD_CD_21 varchar,
PROC_ICD_QAL_22 varchar,PROC_ICD_CD_22 varchar,PROC_ICD_QAL_23 varchar,PROC_ICD_CD_23 varchar,PROC_ICD_QAL_24 varchar,PROC_ICD_CD_24 varchar,
MEM_ID varchar,GEN varchar,AGE varchar,DOB varchar,ZIP varchar,DRG varchar,BILL varchar,DERV_ENC varchar
) 
WITH (appendonly=true, orientation=column, compresstype=zlib)
distributed by (MCO_ICN);

drop external table ext_enc_proc;
CREATE EXTERNAL TABLE ext_enc_proc (
year_fy smallint, filename varchar,
MCO_ICN varchar,TX_CD varchar,SUB_MCO_PLN varchar,PRIM_PROC_QAL varchar,PRIM_PROC_CD varchar,
PROC_ICD_QAL_1 varchar,PROC_ICD_CD_1 varchar,PROC_ICD_QAL_2 varchar,PROC_ICD_CD_2 varchar,PROC_ICD_QAL_3 varchar,PROC_ICD_CD_3 varchar,
PROC_ICD_QAL_4 varchar,PROC_ICD_CD_4 varchar,PROC_ICD_QAL_5 varchar,PROC_ICD_CD_5 varchar,PROC_ICD_QAL_6 varchar,PROC_ICD_CD_6 varchar,
PROC_ICD_QAL_7 varchar,PROC_ICD_CD_7 varchar,PROC_ICD_QAL_8 varchar,PROC_ICD_CD_8 varchar,PROC_ICD_QAL_9 varchar,PROC_ICD_CD_9 varchar,
PROC_ICD_QAL_10 varchar,PROC_ICD_CD_10 varchar,PROC_ICD_QAL_11 varchar,PROC_ICD_CD_11 varchar,PROC_ICD_QAL_12 varchar,PROC_ICD_CD_12 varchar,
PROC_ICD_QAL_13 varchar,PROC_ICD_CD_13 varchar,PROC_ICD_QAL_14 varchar,PROC_ICD_CD_14 varchar,PROC_ICD_QAL_15 varchar,PROC_ICD_CD_15 varchar,
PROC_ICD_QAL_16 varchar,PROC_ICD_CD_16 varchar,PROC_ICD_QAL_17 varchar,PROC_ICD_CD_17 varchar,PROC_ICD_QAL_18 varchar,PROC_ICD_CD_18 varchar,
PROC_ICD_QAL_19 varchar,PROC_ICD_CD_19 varchar,PROC_ICD_QAL_20 varchar,PROC_ICD_CD_20 varchar,PROC_ICD_QAL_21 varchar,PROC_ICD_CD_21 varchar,
PROC_ICD_QAL_22 varchar,PROC_ICD_CD_22 varchar,PROC_ICD_QAL_23 varchar,PROC_ICD_CD_23 varchar,PROC_ICD_QAL_24 varchar,PROC_ICD_CD_24 varchar,
MEM_ID varchar,GEN varchar,AGE varchar,DOB varchar,ZIP varchar,DRG varchar,BILL varchar,DERV_ENC varchar
) 
LOCATION ( 
'gpfdist://greenplum01:8081/uthealth/medicaid/2020/ENC_PROC_*.csv#transform=add_parentname_filename_comma'
)
FORMAT 'CSV' ( HEADER DELIMITER ',' );

-- Test
/*
select *
from ext_enc_proc
limit 10;
*/
-- Insert
insert into medicaid.enc_proc
select * from ext_enc_proc;

-- Analyze
analyze medicaid.enc_proc;
 
-- Verify
select count(*), min(year_fy), max(year_fy), count(distinct year_fy) from medicaid.enc_proc;


select year_fy, file, count(*)
from medicaid.enc_proc
group by 1, 2
order by 1, 2;
