--Medical
drop table optum.procedure;
create table optum.procedure (
PATID bigint, PAT_PLANID bigint, CLMID char(19), ICD_FLAG char(2), PROC char(7), PROC_POSITION smallint, EXTRACT_YM int, VERSION numeric

) 
WITH (appendonly=true, orientation=column)
distributed randomly;

drop external table ext_procedure;
CREATE EXTERNAL TABLE ext_procedure (
PATID bigint, PAT_PLANID bigint, CLMID char(19), ICD_FLAG char(2), PROC char(7), PROC_POSITION smallint, EXTRACT_YM int, VERSION numeric
) 
LOCATION ( 
'gpfdist://c252-140:8801/*'
)
FORMAT 'CSV' ( HEADER DELIMITER '|' );

-- Test
select *
from ext_procedure
limit 1000;

-- Insert
insert into optum.procedure
select * from ext_procedure;

-- Analyze
analyze optum.procedure;

-- Verify
select count(*) from optum.procedure;
