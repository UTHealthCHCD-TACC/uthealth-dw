--Medical
drop table optum_dod_refresh.diagnostic;
create table optum_dod_refresh.diagnostic2 (
year smallint, PATID bigint, PAT_PLANID bigint, CLMID char(19), DIAG char(7), DIAG_POSITION smallint, ICD_FLAG char(2), LOC_CD char(1), POA char(50), EXTRACT_YM int, VERSION numeric, FST_DT date

) 
WITH (appendonly=true, orientation=column, compresstype=zlib)
distributed randomly;

drop external table ext_diagnostic;
CREATE EXTERNAL TABLE ext_diagnostic (
PATID bigint, PAT_PLANID bigint, CLMID char(19), DIAG char(7), DIAG_POSITION smallint, ICD_FLAG char(2), LOC_CD char(1), POA char(50), EXTRACT_YM int, VERSION numeric, FST_DT date
) 
LOCATION ( 
'gpfdist://192.168.58.179:8081/*_diag2*'
)
FORMAT 'CSV' ( HEADER DELIMITER '|' );

-- Test
/*
select count(*)
from ext_diagnostic
limit 1000;
*/
-- Insert
insert into optum_dod_refresh.diagnostic2
select * from optum_dod_refresh.diagnostic;

-- 318 secs
update optum_dod_refresh.diagnostic set year=date_part('year', FST_DT) where year=0;


-- Analyze
analyze optum_dod_refresh.diagnostic;
 
-- Verify
select count(*), min(year), max(year), count(distinct year) from optum_dod_refresh.diagnostic;


select year, date_part('quarter', FST_DT) as quarter, count(*), min(FST_DT), max(FST_DT)
from optum_dod_refresh.diagnostic
group by 1, 2
order by 1, 2;
