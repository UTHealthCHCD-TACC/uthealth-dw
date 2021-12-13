/* 
******************************************************************************************************
 *  This script loads optum_zip/zip.lab_result table
 *  refresh table is provided in most recent quarters 
 *  delete quarters provided in refresh, then load
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
drop table optum_zip.lab_result;
create table optum_zip.lab_result (
year smallint, file varchar,
PATID bigint, PAT_PLANID bigint, ABNL_CD char(2), ANLYTSEQ char(2), FST_DT date, HI_NRML numeric, 
LABCLMID char(19), LOINC_CD char(7), LOW_NRML numeric, PROC_CD char(7), RSLT_NBR numeric, RSLT_TXT char(18), RSLT_UNIT_NM char(18), 
source char(2), TST_DESC char(30), TST_NBR char(10), EXTRACT_YM int, VERSION numeric
) 
WITH (appendonly=true, orientation=column, compresstype=zlib)
distributed by (patid);
*/

drop external table ext_labresult;
CREATE EXTERNAL TABLE ext_labresult (
year smallint, file varchar,
PATID bigint, PAT_PLANID bigint, ABNL_CD char(2), ANLYTSEQ char(2), FST_DT date, HI_NRML numeric, 
LABCLMID char(19), LOINC_CD char(7), LOW_NRML numeric, PROC_CD char(7), RSLT_NBR numeric, RSLT_TXT char(18), RSLT_UNIT_NM char(18), 
source char(2), TST_DESC char(30), TST_NBR char(10), EXTRACT_YM int, VERSION numeric
) 
LOCATION ( 
'gpfdist://greenplum01.corral.tacc.utexas.edu:8081/uthealth/optum_zip/\*/zip5_lr2*.txt.gz#transform=add_parentname_filename_vertbar'
)
FORMAT 'CSV' ( HEADER DELIMITER '|' );

-- Test
/*
select *
from ext_labresult
limit 1000;
*/
-- Insert
insert into optum_zip.lab_result (
year, file,
PATID, PAT_PLANID, ABNL_CD, ANLYTSEQ, FST_DT, HI_NRML, 
LABCLMID, LOINC_CD, LOW_NRML, PROC_CD, RSLT_NBR, RSLT_TXT, RSLT_UNIT_NM, 
source, TST_DESC, TST_NBR, EXTRACT_YM, version, member_id_src
)
select 
year, file, 
PATID, PAT_PLANID, ABNL_CD, ANLYTSEQ, FST_DT, HI_NRML, 
LABCLMID, LOINC_CD, LOW_NRML, PROC_CD, RSLT_NBR, RSLT_TXT, RSLT_UNIT_NM, 
source, TST_DESC, TST_NBR, EXTRACT_YM, version, patid::text
from ext_labresult;

-- Fix distribution
create table optum_zip.lab_result_new
WITH (appendonly=true, orientation=column, compresstype=zlib)
as
select 
year, file,
PATID, PAT_PLANID, ABNL_CD, ANLYTSEQ, FST_DT, HI_NRML, 
LABCLMID, LOINC_CD, LOW_NRML, PROC_CD, RSLT_NBR, RSLT_TXT, RSLT_UNIT_NM, 
source, TST_DESC, TST_NBR, EXTRACT_YM, version, patid::text as member_id_src
from optum_zip.lab_result
distributed by (member_id_src);

drop table optum_zip.lab_result;
alter table optum_zip.lab_result_new rename to lab_result;

-- Analyze
analyze optum_zip.lab_result;

--Verify
select count(*), min(year), max(year), count(distinct year) from optum_zip.lab_result;

select year, count(*), min(FST_DT), max(FST_DT)
from optum_zip.lab_result
group by 1
order by 1;

--Refresh
select file, count(*)
from optum_zip.lab_result
where file >= 'zip5_lr2020q3'
group by 1
order by 1;

delete
from optum_zip.lab_result
where file >= 'zip5_lr2020q3';
