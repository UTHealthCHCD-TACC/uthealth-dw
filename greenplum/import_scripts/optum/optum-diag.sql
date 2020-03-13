--Medical
drop table optum_dod_refresh.diagnostic;
create table optum_dod_refresh.diagnostic (
year smallint, PATID bigint, PAT_PLANID bigint, CLMID char(19), DIAG char(7), DIAG_POSITION smallint, ICD_FLAG char(2), LOC_CD char(1), POA char(50), EXTRACT_YM int, VERSION numeric, FST_DT date

) 
WITH (appendonly=true, orientation=column, compresstype=zlib, compresslevel=5)
distributed randomly;

drop external table ext_diagnostic;
CREATE EXTERNAL TABLE ext_diagnostic (
PATID bigint, PAT_PLANID bigint, CLMID char(19), DIAG char(7), DIAG_POSITION smallint, ICD_FLAG char(2), LOC_CD char(1), POA char(50), EXTRACT_YM int, VERSION numeric, FST_DT date
) 
LOCATION ( 
'gpfdist://c252-136:8081/*_diag2*'
)
FORMAT 'CSV' ( HEADER DELIMITER '|' );

-- Test
/*
select *
from ext_diagnostic
limit 1000;
*/
-- Insert
insert into optum_dod_refresh.diagnostic
select 0, * from ext_diagnostic;

-- 318 secs
update optum_dod_refresh.diagnostic set year=date_part('year', FST_DT);


-- Analyze
analyze optum.diagnostic;

update 
-- Verify
select count(*), min(year), max(year), count(distinct year) from optum_dod_refresh.diagnostic;


select year, count(*), min(FST_DT), max(FST_DT)
from optum_dod_refresh.diagnostic
group by 1
order by 1;
