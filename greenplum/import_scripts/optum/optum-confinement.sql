--Medical
drop table optum_dod_refresh.confinement;
create table optum_dod_refresh.confinement2 (
YEAR SMALLINT, PATID bigint, PAT_PLANID bigint, ADMIT_DATE date, CHARGE numeric, COINS numeric, CONF_ID char(21), COPAY numeric, DEDUCT numeric, 
DIAG1 char(7), DIAG2 char(7), DIAG3 char(7), DIAG4 char(7), DIAG5 char(7), DISCH_DATE date, DRG char(5), DSTATUS char(2), ICD_FLAG char(2), IPSTATUS char(1),
LOS numeric, POS char(5), PROC1 char(7), PROC2 char(7), PROC3 char(7), PROC4 char(7), PROC5 char(7),
PROV numeric, STD_COST numeric, STD_COST_YR smallint, TOS_CD char(13), EXTRACT_YM int, VERSION NUMERIC,
ICU_IND text, ICU_SURG_IND text, MAJ_SURG_IND text, MATERNITY_IND text, NEWBORN_IND text, TOS text

) 
WITH (appendonly=true, orientation=column, compresstype=zlib)
distributed randomly;

-- NOTE: Load each year 1 by one updating the gpfdist string and hard coded YEAR value in insert statement.

drop external table ext_confinement;
CREATE EXTERNAL TABLE ext_confinement (
PATID bigint, PAT_PLANID bigint, ADMIT_DATE date, CHARGE numeric, COINS numeric, CONF_ID char(21), COPAY numeric, DEDUCT numeric, 
DIAG1 char(7), DIAG2 char(7), DIAG3 char(7), DIAG4 char(7), DIAG5 char(7), DISCH_DATE date, DRG char(5), DSTATUS char(2), ICD_FLAG char(2), IPSTATUS char(1),
LOS numeric, POS char(5), PROC1 char(7), PROC2 char(7), PROC3 char(7), PROC4 char(7), PROC5 char(7),
PROV numeric, STD_COST numeric, STD_COST_YR smallint, TOS_CD char(13), EXTRACT_YM int, VERSION NUMERIC,
ICU_IND text, ICU_SURG_IND text, MAJ_SURG_IND text, MATERNITY_IND text, NEWBORN_IND text, TOS text
) 
LOCATION ( 
'gpfdist://192.168.58.179:8081/*_c2*'
)
FORMAT 'CSV' ( HEADER DELIMITER '|' );

-- Test
/*
select *
from ext_confinement
limit 1000;
*/
-- Insert
insert into optum_dod_refresh.confinement2
select * from optum_dod_refresh.confinement;

-- 3 secs
update optum_dod_refresh.confinement set year=date_part('year', ADMIT_DATE);

-- Analyze
analyze optum_dod_refresh.confinement;

--Verify
select year, count(*), min(admit_date), max(admit_date)
from optum_dod_refresh.confinement
group by 1
order by 1;