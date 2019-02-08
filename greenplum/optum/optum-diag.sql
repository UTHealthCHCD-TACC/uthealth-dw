--Medical
drop table optum_dod.diagnostic;
create table optum_dod.diagnostic (
year smallint, PATID bigint, PAT_PLANID bigint, CLMID char(19), DIAG char(7), DIAG_POSITION smallint, ICD_FLAG char(2), LOC_CD char(1), POA char(50), EXTRACT_YM int, VERSION numeric, FST_DT date

) 
WITH (appendonly=true, orientation=column)
distributed randomly;

drop external table ext_diagnostic;
CREATE EXTERNAL TABLE ext_diagnostic (
PATID bigint, PAT_PLANID bigint, CLMID char(19), DIAG char(7), DIAG_POSITION smallint, ICD_FLAG char(2), LOC_CD char(1), POA char(50), EXTRACT_YM int, VERSION numeric, FST_DT date
) 
LOCATION ( 
'gpfdist://c252-140:8801/2018/*_diag2018*'
)
FORMAT 'CSV' ( HEADER DELIMITER '|' );

-- Test
/*
select *
from ext_diagnostic
limit 1000;
*/
-- Insert
insert into optum_dod.diagnostic
select 2018, * from ext_diagnostic;

-- Analyze
analyze optum.diagnostic;

-- Verify
select count(*), min(year), max(year), count(distinct year) from optum_dod.diagnostic;
