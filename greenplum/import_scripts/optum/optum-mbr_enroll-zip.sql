/* ******************************************************************************************************
 *  This script loads optum_zip.mbr_enroll table, ZIP Only
 *  refresh table is provided as a full replacement
 * ******************************************************************************************************
 *  Author || Date      || Notes
 * ******************************************************************************************************
 *  wallingTACC  ||8/25/2021 || comments added
 * ******************************************************************************************************
 */

/* Original Create
--- PATID|PAT_PLANID|ASO|BUS|CDHP|ELIGEFF|ELIGEND|GDR_CD|GROUP_NBR|HEALTH_EXCH|LIS_DUAL|PRODUCT|YRDOB|ZIPCODE_5|EXTRACT_YM|VERSION|FAMILY_ID
drop table optum_zip.mbr_enroll;
create table optum_zip.mbr_enroll (
PATID bigint, PAT_PLANID bigint, ASO char(1), BUS char(5), CDHP char(1), ELIGEFF date, ELIGEND date, GDR_CD char(1), GROUP_NBR char(20), 
HEALTH_EXCH char(1), LIS_DUAL char(1), PRODUCT char(5), YRDOB smallint, ZIPCODE_5 text, EXTRACT_YM int , VERSION numeric, FAMILY_ID numeric
) 
WITH (appendonly=true, orientation=column, compresstype=zlib)
distributed randomly;
*/

drop external table ext_mbr_enroll;
CREATE EXTERNAL TABLE ext_mbr_enroll (
PATID bigint, PAT_PLANID bigint, ASO char(1), BUS char(5), CDHP char(1), ELIGEFF date, ELIGEND date, 
GDR_CD char(1), GROUP_NBR char(20), 
HEALTH_EXCH char(1), LIS_DUAL char(1), PRODUCT char(5), YRDOB smallint, ZIPCODE_5 text, EXTRACT_YM int , VERSION numeric, FAMILY_ID numeric
) 
LOCATION ( 
'gpfdist://greenplum01.corral.tacc.utexas.edu:8081/uthealth/OPTUM_NEW/ZIP_july212021/zip5_mbr_enroll.txt.gz'
)
FORMAT 'CSV' ( HEADER DELIMITER '|' );

-- Test
select *
from ext_mbr_enroll
limit 1000;

-- Insert
insert into optum_zip.mbr_enroll
select * from ext_mbr_enroll;

-- Analyze
analyze optum_zip.mbr_enroll;

--Verify
select count(*) from optum_zip.mbr_enroll;

--Refresh
truncate optum_zip.mbr_enroll;