--Medical
drop table optum_dod.procedure;
create table optum_dod.procedure (
year smallint, PATID bigint, PAT_PLANID bigint, CLMID char(19), ICD_FLAG char(2), PROC char(7), PROC_POSITION smallint, EXTRACT_YM int, VERSION numeric, FST_DT date

) 
WITH (appendonly=true, orientation=column)
distributed randomly;

drop external table ext_procedure;
CREATE EXTERNAL TABLE ext_procedure (
PATID bigint, PAT_PLANID bigint, CLMID char(19), ICD_FLAG char(2), PROC char(7), PROC_POSITION smallint, EXTRACT_YM int, VERSION numeric, FST_DT date
) 
LOCATION ( 
'gpfdist://c252-140:8801/2018/*_proc2018*'
)
FORMAT 'CSV' ( HEADER DELIMITER '|' );

-- Test
/*
select *
from ext_procedure
limit 1000;
*/
-- Insert
insert into optum_dod.procedure
select 2018, * from ext_procedure;

-- Analyze
analyze optum.procedure;

-- Verify
select count(*), min(year), max(year), count(distinct year) from optum_dod.procedure;
