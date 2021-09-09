/* ******************************************************************************************************
 *  This script loads optum_zip.mbr_co_enroll table, ZIP only
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
drop table optum_zip.mbr_co_enroll;
create table optum_zip.mbr_co_enroll (
PATID bigint, ELIGEFF date, ELIGEND date, GDR_CD char(1), YRDOB smallint, EXTRACT_YM int , VERSION numeric
) 
WITH (appendonly=true, orientation=column, compresstype=zlib)
distributed by (patid);
*/

drop external table ext_mbr_co_enroll;
CREATE EXTERNAL TABLE ext_mbr_co_enroll (
PATID bigint, ELIGEFF date, ELIGEND date, GDR_CD char(1), YRDOB smallint, EXTRACT_YM int , VERSION numeric
) 
LOCATION ( 
'gpfdist://greenplum01.corral.tacc.utexas.edu:8081/uthealth/OPTUM_NEW/ZIP_july212021/zip5_mbr_co_enroll.txt.gz'
)
FORMAT 'CSV' ( HEADER DELIMITER '|' );

-- Test
select *
from ext_mbr_co_enroll
limit 1000;

-- Insert
insert into optum_zip.mbr_co_enroll
select * from ext_mbr_co_enroll;

-- Analyze
analyze optum_zip.mbr_co_enroll;

--Verify
select min(eligeff), max(eligeff), count(*) from optum_zip.mbr_co_enroll;

--Refresh
truncate table optum_zip.mbr_co_enroll;