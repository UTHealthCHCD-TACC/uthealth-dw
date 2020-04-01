

--Medical
drop table optum_zip_refresh.lu_procedure;
create table optum_zip_refresh.lu_procedure (
CATEGORY_DTL_CD varchar(100), CATEGORY_DTL_CODE_DESC varchar(100), CATEGORY_GENL_CD varchar(100), CATEGORY_GENL_CODE_DESC varchar(100), 
PROC_CD varchar(100), PROC_DESC varchar(100), PROC_END_DATE date, PROC_TYP_CD varchar(100)
) 
WITH (appendonly=true, orientation=column)
distributed randomly;

drop external table ext_lu_procedure;
CREATE EXTERNAL TABLE ext_lu_procedure (
CATEGORY_DTL_CD varchar(100), CATEGORY_DTL_CODE_DESC varchar(100), CATEGORY_GENL_CD varchar(100), CATEGORY_GENL_CODE_DESC varchar(100), 
PROC_CD varchar(100), PROC_DESC varchar(100), PROC_END_DATE date, PROC_TYP_CD varchar(100)
) 
LOCATION ( 
'gpfdist://c252-136:8081/lu_procedure.txt'
)
FORMAT 'CSV' ( HEADER DELIMITER '|' );

-- Test
select *
from ext_lu_procedure
limit 1000;

-- Insert
insert into optum_zip_refresh.lu_procedure
select * from ext_lu_procedure;

-- Analyze
analyze optum_zip_refresh.lu_procedure;

--Verify
select count(*) from optum_dod.lu_procedure;