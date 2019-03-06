--Medical
drop table optum_dod.member;
create table optum_dod.member (
PATID bigint, ASO char(1), BUS char(5), CDHP char(1), ELIGEFF date, ELIGEND date, FAMILY_ID numeric, GDR_CD char(1), GROUP_NBR char(20), 
HEALTH_EXCH char(1), PRODUCT char(5), STATE char(2), YRDOB smallint, EXTRACT_YM int , VERSION numeric
) 
WITH (appendonly=true, orientation=column)
distributed randomly;

drop external table ext_member;
CREATE EXTERNAL TABLE ext_member (
PATID bigint, ASO char(1), BUS char(5), CDHP char(1), ELIGEFF date, ELIGEND date, FAMILY_ID numeric, GDR_CD char(1), GROUP_NBR char(20), 
HEALTH_EXCH char(1), PRODUCT char(5), STATE char(2), YRDOB smallint, EXTRACT_YM int , VERSION numeric
) 
LOCATION ( 
'gpfdist://c252-140:8801/dod_mbr.txt'
)
FORMAT 'CSV' ( HEADER DELIMITER '|' );

-- Test
select *
from ext_member
limit 1000;

-- Insert
insert into optum_dod.member
select * from ext_member;

-- Analyze
analyze optum_dod.member;

--Verify
select count(*) from optum_dod.member;