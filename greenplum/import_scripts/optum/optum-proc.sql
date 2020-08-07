--Medical
drop table optum_zip_refresh.procedure;
create table optum_zip_refresh.procedure (
year smallint, PATID bigint, PAT_PLANID bigint, CLMID char(19), ICD_FLAG char(2), PROC char(7), PROC_POSITION smallint, EXTRACT_YM int, VERSION numeric, FST_DT date

) 
WITH (appendonly=true, orientation=column, compresstype=zlib)
distributed randomly;

drop external table ext_procedure;
CREATE EXTERNAL TABLE ext_procedure (
PATID bigint, PAT_PLANID bigint, CLMID char(19), ICD_FLAG char(2), PROC char(7), PROC_POSITION smallint, EXTRACT_YM int, VERSION numeric, FST_DT date
) 
LOCATION ( 
'gpfdist://192.168.58.179:8081//zip5_proc2*'
)
FORMAT 'CSV' ( HEADER DELIMITER '|' );

-- Test
/*
select *
from ext_procedure
limit 1000;
*/
-- Insert
insert into optum_zip_refresh.procedure
select 0, * from ext_procedure;


update optum_zip_refresh.procedure set year=date_part('year', FST_DT);


-- Analyze
analyze optum_zip_refresh.procedure;

-- Year & Quarter
select distinct extract(quarter from FST_DT)
from ext_procedure;

-- Verify
select count(*), min(year), max(year), count(distinct year) from optum_zip_refresh.procedure;
