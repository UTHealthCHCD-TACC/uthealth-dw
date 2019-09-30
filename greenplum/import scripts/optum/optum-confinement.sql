--Medical
drop table optum_dod.confinement;
create table optum_dod.confinement (
YEAR SMALLINT, PATID bigint, PAT_PLANID bigint, ADMIT_DATE date, CHARGE numeric, COINS numeric, CONF_ID char(21), COPAY numeric, DEDUCT numeric, 
DIAG1 char(7), DIAG2 char(7), DIAG3 char(7), DIAG4 char(7), DIAG5 char(7), DISCH_DATE date, DRG char(5), DSTATUS char(2), ICD_FLAG char(2), IPSTATUS char(1),
LOS numeric, POS char(5), PROC1 char(7), PROC2 char(7), PROC3 char(7), PROC4 char(7), PROC5 char(7),
PROV numeric, STD_COST numeric, STD_COST_YR smallint, TOS_CD char(13), EXTRACT_YM int, VERSION numeric

) 
WITH (appendonly=true, orientation=column)
distributed randomly;

-- NOTE: Load each year 1 by one updating the gpfdist string and hard coded YEAR value in insert statement.

drop external table ext_confinement;
CREATE EXTERNAL TABLE ext_confinement (
PATID bigint, PAT_PLANID bigint, ADMIT_DATE date, CHARGE numeric, COINS numeric, CONF_ID char(21), COPAY numeric, DEDUCT numeric, 
DIAG1 char(7), DIAG2 char(7), DIAG3 char(7), DIAG4 char(7), DIAG5 char(7), DISCH_DATE date, DRG char(5), DSTATUS char(2), ICD_FLAG char(2), IPSTATUS char(1),
LOS numeric, POS char(5), PROC1 char(7), PROC2 char(7), PROC3 char(7), PROC4 char(7), PROC5 char(7),
PROV numeric, STD_COST numeric, STD_COST_YR smallint, TOS_CD char(13), EXTRACT_YM int, VERSION numeric
) 
LOCATION ( 
'gpfdist://c252-140:8801/*_c2*'
)
FORMAT 'CSV' ( HEADER DELIMITER '|' );

-- Test
/*
select *
from ext_confinement
limit 1000;
*/
-- Insert
insert into optum_dod.confinement
select 0, * from ext_confinement;

-- Analyze
analyze optum.confinement;

--Verify
select count(*), max(year) from optum_dod.confinement;