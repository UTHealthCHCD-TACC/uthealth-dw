--Medical
drop table optum_dod.procedure;
create table optum_dod.procedure (
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
'gpfdist://greenplum01.corral.tacc.utexas.edu:8081/uthealth/OPTUM_NEW/OPT_DOD_APril2021/\*/dod_proc2*.txt.gz#transform=add_parentname_filename_vertbar'
)
FORMAT 'CSV' ( HEADER DELIMITER '|' );

-- Test
/*
select *
from ext_procedure
limit 1000;
*/
-- Insert
insert into optum_dod.procedure
select * from ext_procedure;

-- Analyze
analyze optum_dod.procedure;

-- Year & Quarter
select distinct extract(quarter from FST_DT)
from ext_procedure;

-- Verify
select count(*), min(year), max(year), count(distinct year) from optum_dod.procedure;

--Refresh
delete 
--select file, count(*)
from optum_dod."procedure"
where year > 2017 or file like '%2017q4%';

group by 1
order by 1;
