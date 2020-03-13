--Medical
drop table optum_dod_refresh.lu_diagnosis;
create table optum_dod_refresh.lu_diagnosis (
DIAG_CD varchar(100),DIAG_DESC varchar(100),DIAG_FST3_CD varchar(100),DIAG_FST3_DESC varchar(100),DIAG_FST4_CD varchar(100),
DIAG_FST4_DESC varchar(100),GDR_SPEC_CD varchar(100),MDC_CD_DESC varchar(100),MDC_CODE varchar(100),ICD_VER_CD varchar(100)
) 
WITH (appendonly=true, orientation=column)
distributed randomly;

drop external table ext_lu_diagnosis;
CREATE EXTERNAL TABLE ext_lu_diagnosis (
DIAG_CD varchar(100),DIAG_DESC varchar(100),DIAG_FST3_CD varchar(100),DIAG_FST3_DESC varchar(100),DIAG_FST4_CD varchar(100),
DIAG_FST4_DESC varchar(100),GDR_SPEC_CD varchar(100),MDC_CD_DESC varchar(100),MDC_CODE varchar(100),ICD_VER_CD varchar(100)
) 
LOCATION ( 
'gpfdist://c252-136:8081/lu_diagnosis.txt'
)
FORMAT 'CSV' ( HEADER DELIMITER '|' );

-- Test
select *
from ext_lu_diagnosis
limit 1000;

-- Insert
insert into optum_dod_refresh.lu_diagnosis
select * from ext_lu_diagnosis;

-- Analyze
analyze optum_dod_refresh.lu_diagnosis;

--Verify
select count(*) from optum_dod_refresh.lu_diagnosis;