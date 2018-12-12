--Medical
drop table optum.diagnostic;
create table optum.diagnostic (
PATID bigint, PAT_PLANID bigint, CLMID char(19), DIAG char(7), DIAG_POSITION smallint, ICD_FLAG char(2), LOC_CD char(1), POA char(50), EXTRACT_YM int, VERSION numeric

) 
WITH (appendonly=true, orientation=column)
distributed randomly;

drop external table ext_diagnostic;
CREATE EXTERNAL TABLE ext_diagnostic (
PATID bigint, PAT_PLANID bigint, CLMID char(19), DIAG char(7), DIAG_POSITION smallint, ICD_FLAG char(2), LOC_CD char(1), POA char(50), EXTRACT_YM int, VERSION numeric
) 
LOCATION ( 
'gpfdist://c252-140:8801/*'
)
FORMAT 'CSV' ( HEADER DELIMITER '|' );

-- Test
select *
from ext_diagnostic
limit 1000;

-- Insert
insert into optum.diagnostic
select * from ext_diagnostic;

-- Analyze
analyze optum.diagnostic;

-- Verify
select count(*) from optum.diagnostic;
