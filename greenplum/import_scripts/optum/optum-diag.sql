--Medical
drop table optum_dod.diagnostic;
create table optum_dod.diagnostic (
year smallint, file varchar, 
PATID bigint, PAT_PLANID bigint, CLMID char(19), DIAG char(7), DIAG_POSITION smallint, ICD_FLAG char(2), LOC_CD char(1), POA char(50), EXTRACT_YM int, VERSION numeric, FST_DT date

) 
WITH (appendonly=true, orientation=column, compresstype=zlib)
distributed by (patid);

drop external table ext_diagnostic;
CREATE EXTERNAL TABLE ext_diagnostic (
year smallint, filename varchar,
PATID bigint, PAT_PLANID bigint, CLMID char(19), DIAG char(7), DIAG_POSITION smallint, ICD_FLAG char(2), LOC_CD char(1), POA char(50), EXTRACT_YM int, VERSION numeric, FST_DT date
) 
LOCATION ( 
'gpfdist://greenplum01.corral.tacc.utexas.edu:8081/uthealth/OPTUM_NEW/OPT_DOD_APril2021/\*/dod_diag2*.txt.gz#transform=add_parentname_filename_vertbar'
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
select * from ext_diagnostic;

-- 318 secs
update optum_dod.diagnostic set year=date_part('year', FST_DT) where year=0;


-- Analyze
analyze optum_dod.diagnostic;
 
-- Verify
select count(*), min(year), max(year), count(distinct year) from optum_dod.diagnostic;


select year, date_part('quarter', FST_DT) as quarter, count(*), min(FST_DT), max(FST_DT)
from optum_dod.diagnostic
group by 1, 2
order by 1, 2;

--Refresh
delete
from optum_dod.diagnostic 
where year > 2017 or file like '%2017q4%';
