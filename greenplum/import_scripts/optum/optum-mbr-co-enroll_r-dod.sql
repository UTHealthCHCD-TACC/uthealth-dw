/* ******************************************************************************************************
 *  This script loads optum_zip.mbr_co_enroll_r table, DOD only
 *  refresh table is provided as a full replacement
 * ******************************************************************************************************
 *  Author || Date      || Notes
 * ******************************************************************************************************
 *  wallingTACC  ||8/25/2021 || comments added
 * ******************************************************************************************************
 */

/*
 /==> zip5_mbr_co_enroll.txt <==
PATID|ELIGEFF|ELIGEND|GDR_CD|YRDOB|EXTRACT_YM|VERSION
*/

/* Original Create
drop table optum_zip.mbr_co_enroll_r;
create table optum_zip.mbr_co_enroll_r (
PATID bigint, ELIGEFF date, ELIGEND date, GDR_CD char(1), RACE varchar, YRDOB smallint, EXTRACT_YM int , VERSION numeric
) 
WITH (appendonly=true, orientation=column, compresstype=zlib)
distributed by (patid);
*/

drop external table ext_mbr_co_enroll_r;
CREATE EXTERNAL TABLE ext_mbr_co_enroll_r (
PATID bigint, ELIGEFF date, ELIGEND date, GDR_CD char(1), RACE varchar, YRDOB smallint, EXTRACT_YM int , VERSION numeric
) 
LOCATION ( 
'gpfdist://greenplum01.corral.tacc.utexas.edu:8081/uthealth/optum_zip/zip5_mbr_co_enroll_r.txt.gz'
)
FORMAT 'CSV' ( HEADER DELIMITER '|' );

-- Test
select *
from ext_mbr_co_enroll_r
limit 1000;

-- Insert
insert into optum_zip.mbr_co_enroll_r (
PATID, ELIGEFF, ELIGEND, GDR_CD, RACE, YRDOB, EXTRACT_YM, version, member_id_src
)
select PATID, ELIGEFF, ELIGEND, GDR_CD, RACE, YRDOB, EXTRACT_YM, version, patid::text as member_id_src
from ext_mbr_co_enroll_r;


-- Fix distribution key
create table optum_zip.mbr_co_enroll_r_new
WITH (appendonly=true, orientation=column, compresstype=zlib)
as 
select PATID, ELIGEFF, ELIGEND, GDR_CD, RACE, YRDOB, EXTRACT_YM, version, patid::text as member_id_src
from optum_zip.mbr_co_enroll_r
distributed by (member_id_src);

drop table optum_zip.mbr_co_enroll_r
ALTER TABLE optum_zip.mbr_co_enroll_r_new RENAME TO mbr_co_enroll_r;

-- Analyze
analyze optum_zip.mbr_co_enroll_r;

--Verify
select min(eligeff), max(eligeff), count(*) from optum_zip.mbr_co_enroll_r;

--Refresh
truncate optum_zip.mbr_co_enroll_r;