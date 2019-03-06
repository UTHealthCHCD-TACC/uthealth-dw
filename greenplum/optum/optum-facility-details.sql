--Medical
drop table optum_dod.facility_detail;
create table optum_dod.facility_detail (
year smallint, PATID bigint, PAT_PLANID bigint, CHARGE_ALLOC numeric, CLMID char(19), CLMSEQ char(5), DETAIL_LINE_NBR smallint, FST_DT date, 
PROC_CD char(7), PROCMOD char(5), RVNU_CD char(4), STD_COST_ALLOC numeric, STD_COST_YR smallint, UNITS numeric, EXTRACT_YM int, VERSION numeric
)
WITH (appendonly=true, orientation=column)
distributed randomly;

drop external table ext_facility_detail;
CREATE EXTERNAL TABLE ext_facility_detail (
PATID bigint, PAT_PLANID bigint, CHARGE_ALLOC numeric, CLMID char(19), CLMSEQ char(5), DETAIL_LINE_NBR smallint, FST_DT date, 
PROC_CD char(7), PROCMOD char(5), RVNU_CD char(4), STD_COST_ALLOC numeric, STD_COST_YR smallint, UNITS numeric, EXTRACT_YM int, VERSION numeric
) 
LOCATION ( 
'gpfdist://c252-140:8801/*_fd2*'
)
FORMAT 'CSV' ( HEADER DELIMITER '|' );

-- Test
/*
select *
from ext_facility_detail
limit 1000;
*/
-- Insert
insert into optum_dod.facility_detail
select 0, * from ext_facility_detail;

-- Analyze
analyze optum_dod.facility_detail;

--Verify
select count(*), min(year), max(year), count(distinct year) from optum_dod.facility_detail;