--Medical
drop table medicaid.clm_proc;
create table medicaid.clm_proc ( 
ICN varchar,PROC_ICD_QAL_1 varchar,PROC_ICD_CD_1 varchar,PROC_ICD_QAL_2 varchar,PROC_ICD_CD_2 varchar,PROC_ICD_QAL_3 varchar,PROC_ICD_CD_3 varchar,
PROC_ICD_QAL_4 varchar,PROC_ICD_CD_4 varchar,PROC_ICD_QAL_5 varchar,PROC_ICD_CD_5 varchar,PROC_ICD_QAL_6 varchar,PROC_ICD_CD_6 varchar,
PROC_ICD_QAL_7 varchar,PROC_ICD_CD_7 varchar,PROC_ICD_QAL_8 varchar,PROC_ICD_CD_8 varchar,PROC_ICD_QAL_9 varchar,PROC_ICD_CD_9 varchar,
PROC_ICD_QAL_10 varchar,PROC_ICD_CD_10 varchar,PROC_ICD_QAL_11 varchar,PROC_ICD_CD_11 varchar,PROC_ICD_QAL_12 varchar,PROC_ICD_CD_12 varchar,
PROC_ICD_QAL_13 varchar,PROC_ICD_CD_13 varchar,PROC_ICD_QAL_14 varchar,PROC_ICD_CD_14 varchar,PROC_ICD_QAL_15 varchar,PROC_ICD_CD_15 varchar,
PROC_ICD_QAL_16 varchar,PROC_ICD_CD_16 varchar,PROC_ICD_QAL_17 varchar,PROC_ICD_CD_17 varchar,PROC_ICD_QAL_18 varchar,PROC_ICD_CD_18 varchar,
PROC_ICD_QAL_19 varchar,PROC_ICD_CD_19 varchar,PROC_ICD_QAL_20 varchar,PROC_ICD_CD_20 varchar,PROC_ICD_QAL_21 varchar,PROC_ICD_CD_21 varchar,
PROC_ICD_QAL_22 varchar,PROC_ICD_CD_22 varchar,PROC_ICD_QAL_23 varchar,PROC_ICD_CD_23 varchar,PROC_ICD_QAL_24 varchar,PROC_ICD_CD_24 varchar,
PROC_ICD_QAL_25 varchar,PROC_ICD_CD_25 varchar,
PCN varchar,SEX varchar,AGE varchar,DOB varchar,DRG varchar,BILL varchar

) 
WITH (appendonly=true, orientation=column, compresstype=zlib)
distributed by (ICN);

drop external table ext_clm_proc;
CREATE EXTERNAL TABLE ext_clm_proc (
ICN varchar,PROC_ICD_QAL_1 varchar,PROC_ICD_CD_1 varchar,PROC_ICD_QAL_2 varchar,PROC_ICD_CD_2 varchar,PROC_ICD_QAL_3 varchar,PROC_ICD_CD_3 varchar,
PROC_ICD_QAL_4 varchar,PROC_ICD_CD_4 varchar,PROC_ICD_QAL_5 varchar,PROC_ICD_CD_5 varchar,PROC_ICD_QAL_6 varchar,PROC_ICD_CD_6 varchar,
PROC_ICD_QAL_7 varchar,PROC_ICD_CD_7 varchar,PROC_ICD_QAL_8 varchar,PROC_ICD_CD_8 varchar,PROC_ICD_QAL_9 varchar,PROC_ICD_CD_9 varchar,
PROC_ICD_QAL_10 varchar,PROC_ICD_CD_10 varchar,PROC_ICD_QAL_11 varchar,PROC_ICD_CD_11 varchar,PROC_ICD_QAL_12 varchar,PROC_ICD_CD_12 varchar,
PROC_ICD_QAL_13 varchar,PROC_ICD_CD_13 varchar,PROC_ICD_QAL_14 varchar,PROC_ICD_CD_14 varchar,PROC_ICD_QAL_15 varchar,PROC_ICD_CD_15 varchar,
PROC_ICD_QAL_16 varchar,PROC_ICD_CD_16 varchar,PROC_ICD_QAL_17 varchar,PROC_ICD_CD_17 varchar,PROC_ICD_QAL_18 varchar,PROC_ICD_CD_18 varchar,
PROC_ICD_QAL_19 varchar,PROC_ICD_CD_19 varchar,PROC_ICD_QAL_20 varchar,PROC_ICD_CD_20 varchar,PROC_ICD_QAL_21 varchar,PROC_ICD_CD_21 varchar,
PROC_ICD_QAL_22 varchar,PROC_ICD_CD_22 varchar,PROC_ICD_QAL_23 varchar,PROC_ICD_CD_23 varchar,PROC_ICD_QAL_24 varchar,PROC_ICD_CD_24 varchar,
PROC_ICD_QAL_25 varchar,PROC_ICD_CD_25 varchar,
PCN varchar,SEX varchar,AGE varchar,DOB varchar,DRG varchar,BILL varchar
) 
LOCATION ( 
'gpfdist://greenplum01:8081/uthealth/medicaid/HealthyTexasWomen/CLM_PROC_*.csv'
)
FORMAT 'CSV' ( HEADER DELIMITER ',' );

-- Test
/*
select *
from ext_clm_proc
limit 10;
*/
-- Insert
insert into medicaid.clm_proc
select * from ext_clm_proc;

-- 318 secs
update medicaid.clm_proc set year_fy=date_part('year_fy', FST_DT) where year_fy=0;


-- Analyze
analyze medicaid.clm_proc;
 
-- Verify
select count(*), min(year_fy), max(year_fy), count(distinct year_fy) from medicaid.clm_proc;


select year_fy, date_part('quarter', FST_DT) as quarter, count(*), min(FST_DT), max(FST_DT)
from medicaid.clm_proc
group by 1, 2
order by 1, 2;
