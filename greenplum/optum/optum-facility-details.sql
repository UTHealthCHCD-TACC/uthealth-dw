--Medical
drop table optum.facility_detail;
create table optum.facility_detail (
PATID bigint, PAT_PLANID bigint, CHARGE_ALLOC numeric, CLMID char(19), CLMSEQ char(5), DETAIL_LINE_NBR smallint, FST_DT date, 
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
'gpfdist://c252-140:8801/*'
)
FORMAT 'CSV' ( HEADER DELIMITER '|' );

-- Test
select *
from ext_facility_detail
limit 1000;

-- Insert
insert into optum.facility_detail
select * from ext_facility_detail;

-- Analyze
analyze optum.facility_detail;

--Verify
select count(*) from optum.facility_detail;