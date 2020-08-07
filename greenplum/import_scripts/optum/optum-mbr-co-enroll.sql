/*
 /==> dod_mbr_co_enroll.txt <==
PATID|ELIGEFF|ELIGEND|GDR_CD|YRDOB|EXTRACT_YM|VERSION
*/

--Medical
drop table optum_zip_refresh.mbr_co_enroll;
create table optum_zip_refresh.mbr_co_enroll (
PATID bigint, ELIGEFF date, ELIGEND date, GDR_CD char(1), YRDOB smallint, EXTRACT_YM int , VERSION numeric
) 
WITH (appendonly=true, orientation=column, compresstype=zlib)
distributed randomly;

drop external table ext_mbr_co_enroll;
CREATE EXTERNAL TABLE ext_mbr_co_enroll (
PATID bigint, ELIGEFF date, ELIGEND date, GDR_CD char(1), YRDOB smallint, EXTRACT_YM int , VERSION numeric
) 
LOCATION ( 
'gpfdist://c252-136:8081/zip5_mbr_co_enroll.txt.gz'
)
FORMAT 'CSV' ( HEADER DELIMITER '|' );

-- Test
select *
from ext_mbr_co_enroll
limit 1000;

-- Insert
insert into optum_zip_refresh.mbr_co_enroll
select * from ext_mbr_co_enroll;

-- Analyze
analyze optum_zip_refresh.mbr_co_enroll;

--Verify
select count(*) from optum_zip_refresh.mbr_co_enroll;