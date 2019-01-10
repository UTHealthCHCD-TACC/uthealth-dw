--Medical
drop table optum.member;
create table optum.member (
PATID bigint, ASO char(1), BUS char(5), CDHP char(1), ELIGEFF date, ELIGEND date, GDR_CD char(1), GROUP_NBR char(20), 
HEALTH_EXCH char(1), PRODUCT char(5), YRDOB smallint, ZIPCODE_5 char(100), EXTRACT_YM int , VERSION numeric
) 
WITH (appendonly=true, orientation=column)
distributed randomly;

drop external table ext_member;
CREATE EXTERNAL TABLE ext_member (
PATID bigint, ASO char(1), BUS char(5), CDHP char(1), ELIGEFF date, ELIGEND date, GDR_CD char(1), GROUP_NBR char(20), 
HEALTH_EXCH char(1), PRODUCT char(5), YRDOB smallint, ZIPCODE_5 char(100), EXTRACT_YM int , VERSION numeric
) 
LOCATION ( 
'gpfdist://c252-140:8801/zip5_mbr.txt'
)
FORMAT 'CSV' ( HEADER DELIMITER '|' );

-- Test
select *
from ext_member
limit 1000;

-- Insert
insert into optum.member
select * from ext_member;

-- Analyze
analyze optum.member;

--Verify
select count(*) from optum.member;