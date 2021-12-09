/* ******************************************************************************************************
 *  This script loads optum_dod.mbr_enroll_r table, DOD only
 *  refresh table is provided as a full replacement
 * ******************************************************************************************************
 *  Author || Date      || Notes
 * ******************************************************************************************************
 *  wallingTACC  ||8/25/2021 || comments added
 * ******************************************************************************************************
 */

--DOD Only

/* Original Create
drop table optum_dod.mbr_enroll_r;
create table optum_dod.mbr_enroll_r (
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
'gpfdist://greenplum01.corral.tacc.utexas.edu:8081/uthealth/optum_dod/dod_mbr_enroll_r.txt.gz'
)
FORMAT 'CSV' ( HEADER DELIMITER '|' );

-- Test
select *
from ext_mbr_enroll_r
limit 1000;

-- Insert
insert into optum_dod.mbr_enroll_r (
PATID, PAT_PLANID, ASO, BUS, CDHP, ELIGEFF, ELIGEND, FAMILY_ID, 
GDR_CD, GROUP_NBR, 
HEALTH_EXCH, LIS_DUAL, PRODUCT, RACE, STATE, YRDOB, EXTRACT_YM, VERSION, member_id_src
)
select PATID, PAT_PLANID, ASO, BUS, CDHP, ELIGEFF, ELIGEND, FAMILY_ID, 
GDR_CD, GROUP_NBR, 
HEALTH_EXCH, LIS_DUAL, PRODUCT, RACE, STATE, YRDOB, EXTRACT_YM, VERSION, patid::text as member_id_src
from ext_mbr_enroll_r;

-- Fix distribution key
create table optum_dod.mbr_enroll_r_new
WITH (appendonly=true, orientation=column, compresstype=zlib)
as 
select PATID, PAT_PLANID, ASO, BUS, CDHP, ELIGEFF, ELIGEND, FAMILY_ID, 
GDR_CD, GROUP_NBR, 
HEALTH_EXCH, LIS_DUAL, PRODUCT, RACE, STATE, YRDOB, EXTRACT_YM, VERSION, patid::text as member_id_src
from optum_dod.mbr_enroll_r
distributed by (member_id_src);

drop table optum_dod.mbr_enroll_r;
ALTER TABLE optum_dod.mbr_enroll_r_new RENAME TO mbr_enroll_r;


-- Analyze
analyze optum_dod.mbr_enroll;

--Verify
select min(eligeff), max(eligeff), count(*) from optum_dod.mbr_enroll;

--Refresh
truncate table optum_dod.mbr_enroll_r;