/* 
******************************************************************************************************
 *  This script loads optum_zip/zip.procedure table
 *  refresh table is provided in most recent quarters 
 *
 * data should be staged in a parent folder with the year matching the filename year, a manual step
 * Examples: staging/optum_zip_refresh/2018/zip5_proc2018q1.txt.gz, staging/optum_zip_refresh/2019/zip5_proc2019q1.txt.gz
 * ******************************************************************************************************
 *  Author || Date      || Notes
 * ******************************************************************************************************
 *  wallingTACC  ||8/25/2021 || comments added
 * ******************************************************************************************************
 */

/* Original Create
drop table optum_zip.procedure;
create table optum_zip.procedure (
year smallint, file varchar,
PATID bigint, PAT_PLANID bigint, CLMID char(19), ICD_FLAG char(2), PROC char(7), PROC_POSITION smallint, EXTRACT_YM int, VERSION numeric, FST_DT date

) 
WITH (appendonly=true, orientation=column, compresstype=zlib)
distributed BY (patid);
*/

drop external table ext_procedure_optum_zip;
CREATE EXTERNAL TABLE ext_procedure_optum_zip (
year smallint, file varchar,
PATID bigint, PAT_PLANID bigint, CLMID char(19), ICD_FLAG char(2), PROC char(7), PROC_POSITION smallint, EXTRACT_YM int, VERSION numeric, FST_DT date
) 
LOCATION ( 
'gpfdist://greenplum01.corral.tacc.utexas.edu:8081/uthealth/optum_zip/\*/zip5_proc2*.txt.gz#transform=add_parentname_filename_vertbar'
)
FORMAT 'CSV' ( HEADER DELIMITER '|' );

-- Test
/*
select *
from ext_procedure_optum_zip
limit 1000;
*/
-- Insert
insert into optum_zip.procedure (
year, file,
PATID, PAT_PLANID, CLMID, ICD_FLAG, PROC, PROC_POSITION, EXTRACT_YM, VERSION, FST_DT, member_id_src
)
select year, file,
PATID, PAT_PLANID, CLMID, ICD_FLAG, PROC, PROC_POSITION, EXTRACT_YM, VERSION, FST_DT, patid::text as member_id_src
from ext_procedure_optum_zip;

-- Fix distribution key
create table optum_zip.procedure_new
WITH (appendonly=true, orientation=column, compresstype=zlib)
as 
select year, file,
PATID, PAT_PLANID, CLMID, ICD_FLAG, PROC, PROC_POSITION, EXTRACT_YM, VERSION, FST_DT, patid::text as member_id_src
from optum_zip.procedure
distributed by (member_id_src);

drop table optum_zip.procedure;
ALTER TABLE optum_zip.procedure_new RENAME TO procedure;


-- Analyze
analyze optum_zip.procedure;

-- Year & Quarter
select distinct extract(quarter from FST_DT)
from ext_procedure;

-- Verify
select count(*), min(year), max(year), count(distinct year) from optum_zip.procedure;

--Refresh

select file, count(*)
from optum_zip."procedure"
where file >= 'zip5_proc2018q1%'
group by 1
order by 1;

delete 
from optum_zip."procedure"
where file >= 'zip5_proc2018q2%';
