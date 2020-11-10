--Medical
drop table optum_zip.lab_result;
create table optum_zip.lab_result (
year smallint, file varchar,
PATID bigint, PAT_PLANID bigint, ABNL_CD char(2), ANLYTSEQ char(2), FST_DT date, HI_NRML numeric, 
LABCLMID char(19), LOINC_CD char(7), LOW_NRML numeric, PROC_CD char(7), RSLT_NBR numeric, RSLT_TXT char(18), RSLT_UNIT_NM char(18), 
source char(2), TST_DESC char(30), TST_NBR char(10), EXTRACT_YM int, VERSION numeric
) 
WITH (appendonly=true, orientation=column, compresstype=zlib)
distributed by (patid);

drop external table ext_labresult;
CREATE EXTERNAL TABLE ext_labresult (
year smallint, file varchar,
PATID bigint, PAT_PLANID bigint, ABNL_CD char(2), ANLYTSEQ char(2), FST_DT date, HI_NRML numeric, 
LABCLMID char(19), LOINC_CD char(7), LOW_NRML numeric, PROC_CD char(7), RSLT_NBR numeric, RSLT_TXT char(18), RSLT_UNIT_NM char(18), 
source char(2), TST_DESC char(30), TST_NBR char(10), EXTRACT_YM int, VERSION numeric
) 
LOCATION ( 
'gpfdist://192.168.58.179:8081/optum_zip/*/*_lr2*.txt.gz#transform=add_parentname_filename_comma_filename_vertbar'
)
FORMAT 'CSV' ( HEADER DELIMITER '|' );

-- Test
/*
select *
from ext_labresult
limit 1000;
*/
-- Insert
insert into optum_zip.lab_result
select * from ext_labresult;

update optum_zip.lab_result set year=date_part('year', FST_DT);

-- Analyze
analyze optum_zip.lab_result;

--Verify
select count(*), min(year), max(year), count(distinct year) from optum_zip.lab_result;

select year, count(*), min(FST_DT), max(FST_DT)
from optum_zip.lab_result
group by 1
order by 1;
