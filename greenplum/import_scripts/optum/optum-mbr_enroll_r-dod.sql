--DOD Only

--Medical
drop table optum_dod.mbr_enroll_r;
create table optum_dod.mbr_enroll_r (
PATID bigint, PAT_PLANID bigint, ASO char(1), BUS char(5), CDHP char(1), ELIGEFF date, ELIGEND date, FAMILY_ID numeric, GDR_CD char(1), GROUP_NBR char(20), 
HEALTH_EXCH char(1), LIS_DUAL char(1), PRODUCT char(5), RACE varchar, STATE char(2), YRDOB smallint, EXTRACT_YM int , VERSION numeric
) 
WITH (appendonly=true, orientation=column, compresstype=zlib)
distributed BY (patid);

drop external table ext_mbr_enroll_r;
CREATE EXTERNAL TABLE ext_mbr_enroll_r (
PATID bigint, PAT_PLANID bigint, ASO char(1), BUS varchar, CDHP char(1), ELIGEFF date, ELIGEND date, FAMILY_ID numeric, 
GDR_CD char(1), GROUP_NBR varchar, 
HEALTH_EXCH char(1), LIS_DUAL char(1), PRODUCT varchar, RACE varchar, STATE char(2), YRDOB smallint, EXTRACT_YM int , VERSION numeric 
) 
LOCATION ( 
'gpfdist://greenplum01.corral.tacc.utexas.edu:8081/uthealth/OPTUM_NEW/OPT_DOD_APril2021/dod_mbr_enroll_r.txt.gz'
)
FORMAT 'CSV' ( HEADER DELIMITER '|' );

-- Test
select *
from ext_mbr_enroll_r
limit 1000;

-- Insert
insert into optum_dod.mbr_enroll_r
select * from ext_mbr_enroll_r;

truncate optum_dod.mbr_enroll;

-- Analyze
analyze optum_dod.mbr_enroll;

--Verify
select min(eligeff), max(eligeff), count(*) from optum_dod.mbr_enroll;

--Refresh
truncate table optum_dod.mbr_enroll_r;