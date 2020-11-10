--Medical
drop table optum_zip.facility_detail;
create table optum_zip.facility_detail (
year smallint, file varchar,
PATID bigint, PAT_PLANID bigint, CHARGE_ALLOC numeric, CLMID char(19), CLMSEQ char(5), DETAIL_LINE_NBR smallint, FST_DT date, 
PROC_CD char(7), PROCMOD char(5), RVNU_CD char(4), STD_COST_ALLOC numeric, STD_COST_YR smallint, UNITS numeric, EXTRACT_YM int, VERSION numeric
)
WITH (appendonly=true, orientation=column, compresstype=zlib)
distributed BY (patid);

drop external table ext_facility_detail;
CREATE EXTERNAL TABLE ext_facility_detail (
year smallint, file varchar,
PATID bigint, PAT_PLANID bigint, CHARGE_ALLOC numeric, CLMID char(19), CLMSEQ char(5), DETAIL_LINE_NBR smallint, FST_DT date, 
PROC_CD char(7), PROCMOD char(5), RVNU_CD char(4), STD_COST_ALLOC numeric, STD_COST_YR smallint, UNITS numeric, EXTRACT_YM int, VERSION numeric
) 
LOCATION ( 
'gpfdist://c252-136:8081/optum_zip/*/*_fd2*.txt.gz#transform=add_parentname_filename_comma_filename_vertbar'
)
FORMAT 'CSV' ( HEADER DELIMITER '|' );

-- Test
/*
select *
from ext_facility_detail
limit 1000;
*/
-- Insert
insert into optum_zip.facility_detail
select 0, * from ext_facility_detail;

-- Analyze
analyze optum_zip.facility_detail;

--Verify
select count(*), min(year), max(year), count(distinct year) from optum_zip.facility_detail;