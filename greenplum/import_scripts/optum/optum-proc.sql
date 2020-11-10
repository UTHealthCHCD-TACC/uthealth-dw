--Medical
drop table optum_zip.procedure;
create table optum_zip.procedure (
year smallint, file varchar,
PATID bigint, PAT_PLANID bigint, CLMID char(19), ICD_FLAG char(2), PROC char(7), PROC_POSITION smallint, EXTRACT_YM int, VERSION numeric, FST_DT date

) 
WITH (appendonly=true, orientation=column, compresstype=zlib)
distributed BY (patid);

drop external table ext_procedure;
CREATE EXTERNAL TABLE ext_procedure (
year smallint, file varchar,
PATID bigint, PAT_PLANID bigint, CLMID char(19), ICD_FLAG char(2), PROC char(7), PROC_POSITION smallint, EXTRACT_YM int, VERSION numeric, FST_DT date
) 
LOCATION ( 
'gpfdist://192.168.58.179:8081/optum_zip/*/zip5_proc2*.txt.gz#transform=add_parentname_filename_comma_filename_vertbar'
)
FORMAT 'CSV' ( HEADER DELIMITER '|' );

-- Test
/*
select *
from ext_procedure
limit 1000;
*/
-- Insert
insert into optum_zip.procedure
select * from ext_procedure;

-- Analyze
analyze optum_zip.procedure;

-- Year & Quarter
select distinct extract(quarter from FST_DT)
from ext_procedure;

-- Verify
select count(*), min(year), max(year), count(distinct year) from optum_zip.procedure;
