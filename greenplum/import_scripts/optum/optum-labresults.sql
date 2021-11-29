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
'gpfdist://greenplum01.corral.tacc.utexas.edu:8081/uthealth/optum/\*/zip5_lr2*.txt.gz#transform=add_parentname_filename_vertbar'
)
FORMAT 'CSV' ( HEADER DELIMITER '|' );

-- Test
/*
select *
from ext_labresult
limit 1000;
*/
-- Insert
insert into optum_zip.lab_result
select * from ext_labresult;

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
where file >= 'zip5_lr2020q2'
group by 1
order by 1;

delete
from optum_zip.lab_result
where file >= 'zip5_lr2020q3';
