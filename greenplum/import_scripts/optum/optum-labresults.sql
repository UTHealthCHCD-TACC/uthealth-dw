--Medical
drop table optum_dod.lab_result;
create table optum_dod.lab_result (
year smallint, PATID bigint, PAT_PLANID bigint, ABNL_CD char(2), ANLYTSEQ char(2), FST_DT date, HI_NRML numeric, 
LABCLMID char(19), LOINC_CD char(7), LOW_NRML numeric, PROC_CD char(7), RSLT_NBR numeric, RSLT_TXT char(18), RSLT_UNIT_NM char(18), 
source char(2), TST_DESC char(30), TST_NBR char(10), EXTRACT_YM int, VERSION numeric
) 
WITH (appendonly=true, orientation=column)
distributed randomly;

drop external table ext_labresult;
CREATE EXTERNAL TABLE ext_labresult (
PATID bigint, PAT_PLANID bigint, ABNL_CD char(2), ANLYTSEQ char(2), FST_DT date, HI_NRML numeric, 
LABCLMID char(19), LOINC_CD char(7), LOW_NRML numeric, PROC_CD char(7), RSLT_NBR numeric, RSLT_TXT char(18), RSLT_UNIT_NM char(18), 
source char(2), TST_DESC char(30), TST_NBR char(10), EXTRACT_YM int, VERSION numeric
) 
LOCATION ( 
'gpfdist://c252-140:8801/*_lr2*'
)
FORMAT 'CSV' ( HEADER DELIMITER '|' );

-- Test
/*
select *
from ext_labresult
limit 1000;
*/
-- Insert
insert into optum_dod.lab_result
select 0, * from ext_labresult;

-- Analyze
analyze optum_dod.lab_result;

--Verify
select count(*), min(year), max(year), count(distinct year) from optum_dod.lab_result;
