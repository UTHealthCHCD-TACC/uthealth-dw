/* ******************************************************************************************************
 *  This script loads optum_zip.mbr_enroll_r table, DOD only
 *  refresh table is provided as a full replacement
 * ******************************************************************************************************
 *  Author || Date      || Notes
 * ******************************************************************************************************
 *  wallingTACC  ||8/25/2021 || comments added
 * ******************************************************************************************************
 */

--DOD Only

/* Original Create
drop table optum_zip.mbr_enroll_r;
create table optum_zip.mbr_enroll_r (
PATID bigint, PAT_PLANID bigint, ASO char(1), BUS char(5), CDHP char(1), ELIGEFF date, ELIGEND date, FAMILY_ID numeric, GDR_CD char(1), GROUP_NBR char(20), 
HEALTH_EXCH char(1), LIS_DUAL char(1), PRODUCT char(5), RACE varchar, STATE char(2), YRDOB smallint, EXTRACT_YM int , VERSION numeric
) 
WITH (appendonly=true, orientation=column, compresstype=zlib)
distributed BY (patid);
*/
 
drop external table ext_mbr_enroll_r;
CREATE EXTERNAL TABLE ext_mbr_enroll_r (
PATID bigint, PAT_PLANID bigint, ASO char(1), BUS varchar, CDHP char(1), ELIGEFF date, ELIGEND date, FAMILY_ID numeric, 
GDR_CD char(1), GROUP_NBR varchar, 
HEALTH_EXCH char(1), LIS_DUAL char(1), PRODUCT varchar, RACE varchar, STATE char(2), YRDOB smallint, EXTRACT_YM int , VERSION numeric 
) 
LOCATION ( 
'gpfdist://greenplum01.corral.tacc.utexas.edu:8081/uthealth/OPTUM_NEW/ZIP_july212021/zip5_mbr_enroll_r.txt.gz'
)
FORMAT 'CSV' ( HEADER DELIMITER '|' );

-- Test
select *
from ext_mbr_enroll_r
limit 1000;

-- Insert
insert into optum_zip.mbr_enroll_r
select * from ext_mbr_enroll_r;

-- Analyze
analyze optum_zip.mbr_enroll;

--Verify
select min(eligeff), max(eligeff), count(*) from optum_zip.mbr_enroll;

--Refresh
truncate table optum_zip.mbr_enroll_r;