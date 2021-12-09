/* 
******************************************************************************************************
 *  This script loads optum_dod/zip.diagnostic table
 *  refresh table is provided in most recent quarters 
 *  delete quarters provided in refresh, then load
 *
 * data should be staged in a parent folder with the year matching the filename year, a manual step
 * Examples: staging/optum_dod_refresh/2018/dod_proc2018q1.txt.gz, staging/optum_dod_refresh/2019/dod_proc2019q1.txt.gz
 * ******************************************************************************************************
 *  Author || Date      || Notes
 * ******************************************************************************************************
 *  wallingTACC  ||8/25/2021 || comments added
 * ******************************************************************************************************
 */
/* Original Create
drop table optum_dod.diagnostic;
create table optum_dod.diagnostic (
year smallint, file varchar, 
PATID bigint, PAT_PLANID bigint, CLMID char(19), DIAG char(7), DIAG_POSITION smallint, ICD_FLAG char(2), LOC_CD char(1), POA char(50), EXTRACT_YM int, VERSION numeric, FST_DT date

) 
WITH (appendonly=true, orientation=column, compresstype=zlib)
distributed by (patid);
*/

drop external table ext_diagnostic;
CREATE EXTERNAL TABLE ext_diagnostic (
year smallint, filename varchar,
PATID bigint, PAT_PLANID bigint, CLMID char(19), DIAG char(7), DIAG_POSITION smallint, ICD_FLAG char(2), LOC_CD char(1), POA char(50), EXTRACT_YM int, VERSION numeric, FST_DT date
) 
LOCATION ( 
'gpfdist://greenplum01.corral.tacc.utexas.edu:8081/uthealth/optum_dod/\*/dod_diag2*.txt.gz#transform=add_parentname_filename_vertbar'
)
FORMAT 'CSV' ( HEADER DELIMITER '|' );

-- Test
/*
select *
from ext_diagnostic
limit 1000;
*/
-- Insert
insert into optum_dod.diagnostic (PATID, PAT_PLANID, CLMID, DIAG, DIAG_POSITION, 
ICD_FLAG, LOC_CD, POA, EXTRACT_YM, VERSION, FST_DT, member_id_src)
select PATID, PAT_PLANID, trim(CLMID), DIAG, DIAG_POSITION, 
ICD_FLAG, LOC_CD, POA, EXTRACT_YM, VERSION, FST_DT, patid::text
from ext_diagnostic;

create table optum_dod.diagnostic_new
WITH (appendonly=true, orientation=column, compresstype=zlib)
as select PATID, PAT_PLANID, trim(CLMID), DIAG, DIAG_POSITION, 
ICD_FLAG, LOC_CD, POA, EXTRACT_YM, VERSION, FST_DT
from optum_dod.diagnostic;

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
select file, count(*)
from optum_dod.diagnostic 
where file >= 'dod_diag2018q1%'
group by 1
order by 1;

delete
from optum_dod.diagnostic 
where year is null;
where file >= 'dod_diag2018q2%';
