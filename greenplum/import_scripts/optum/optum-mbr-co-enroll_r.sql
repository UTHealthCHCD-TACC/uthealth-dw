/*
 /==> zip5_mbr_co_enroll.txt <==
PATID|ELIGEFF|ELIGEND|GDR_CD|YRDOB|EXTRACT_YM|VERSION
*/

--Medical
drop table optum_zip.mbr_co_enroll_r;
create table optum_zip.mbr_co_enroll_r (
PATID bigint, ELIGEFF date, ELIGEND date, GDR_CD char(1), RACE varchar, YRDOB smallint, EXTRACT_YM int , VERSION numeric
) 
WITH (appendonly=true, orientation=column, compresstype=zlib)
distributed by (patid);

drop external table ext_mbr_co_enroll_r;
CREATE EXTERNAL TABLE ext_mbr_co_enroll_r (
PATID bigint, ELIGEFF date, ELIGEND date, GDR_CD char(1), RACE varchar, YRDOB smallint, EXTRACT_YM int , VERSION numeric
) 
LOCATION ( 
'gpfdist://c252-136:8081/optum_zip/zip5_mbr_co_enroll_r.txt.gz'
)
FORMAT 'CSV' ( HEADER DELIMITER '|' );

-- Test
select *
from ext_mbr_co_enroll_r
limit 1000;

-- Insert
insert into optum_zip.mbr_co_enroll_r
select * from ext_mbr_co_enroll_r;

-- Analyze
analyze optum_zip.mbr_co_enroll_r;

--Verify
select min(eligeff), max(eligeff), count(*) from optum_zip.mbr_co_enroll_r;