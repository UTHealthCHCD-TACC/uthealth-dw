--Medical
drop table optum_zip_refresh.mbr_enroll;
create table optum_zip_refresh.mbr_enroll2 (
PATID bigint, PAT_PLANID bigint, ASO char(1), BUS char(5), CDHP char(1), ELIGEFF date, ELIGEND date, FAMILY_ID numeric, GDR_CD char(1), GROUP_NBR char(20), 
HEALTH_EXCH char(1), LIS_DUAL char(1), PRODUCT char(5), STATE char(2), YRDOB smallint, EXTRACT_YM int , VERSION numeric
) 
WITH (appendonly=true, orientation=column, compresstype=zlib)
distributed randomly;

drop external table ext_mbr_enroll;
CREATE EXTERNAL TABLE ext_mbr_enroll (
PATID bigint, PAT_PLANID bigint, ASO char(1), BUS char(5), CDHP char(1), ELIGEFF date, ELIGEND date, FAMILY_ID numeric, 
GDR_CD char(1), GROUP_NBR char(20), 
HEALTH_EXCH char(1), LIS_DUAL char(1), PRODUCT char(5), STATE char(2), YRDOB smallint, EXTRACT_YM int , VERSION numeric 
) 
LOCATION ( 
'gpfdist://192.168.58.179:8081/zip5_mbr_enroll.txt.gz'
)
FORMAT 'CSV' ( HEADER DELIMITER '|' );

-- Test
select *
from ext_mbr_enroll
limit 1000;

-- Insert
insert into optum_zip_refresh.mbr_enroll2
select * from ext_mbr_enroll;

truncate optum_zip_refresh.mbr_enroll;

-- Analyze
analyze optum_zip_refresh.mbr_enroll;

--Verify
select count(*) from optum_zip_refresh.mbr_enroll;