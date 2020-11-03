--Medical
drop table optum_zip_refresh.confinement;
create table optum_zip_refresh.confinement (
YEAR SMALLINT, PATID bigint, PAT_PLANID bigint, ADMIT_DATE date, CHARGE numeric, COINS numeric, CONF_ID char(21), COPAY numeric, DEDUCT numeric, 
DIAG1 char(7), DIAG2 char(7), DIAG3 char(7), DIAG4 char(7), DIAG5 char(7), DISCH_DATE date, DRG char(5), DSTATUS char(2), ICD_FLAG char(2), IPSTATUS char(1),
LOS numeric, POS char(5), PROC1 char(7), PROC2 char(7), PROC3 char(7), PROC4 char(7), PROC5 char(7),
PROV numeric, STD_COST numeric, STD_COST_YR smallint, TOS_CD char(13), EXTRACT_YM int, VERSION NUMERIC,
ICU_IND text, ICU_SURG_IND text, MAJ_SURG_IND text, MATERNITY_IND text, NEWBORN_IND text, TOS text

) 
WITH (appendonly=true, orientation=column, compresstype=zlib)
distributed randomly;

-- NOTE: Load each year 1 by one updating the gpfdist string and hard coded YEAR value in insert statement.

/*
PATID|PAT_PLANID|ADMIT_DATE|CHARGE|COINS|CONF_ID|COPAY|DEDUCT|
DIAG1|DIAG2|DIAG3|DIAG4|DIAG5|DISCH_DATE|DRG|DSTATUS|ICD_FLAG|IPSTATUS|
LOS|POS|PROC1|PROC2|PROC3|PROC4|PROC5|
PROV|STD_COST|STD_COST_YR|TOS_CD|EXTRACT_YM|VERSION|
ICU_IND|ICU_SURG_IND|MAJ_SURG_IND|MATERNITY_IND|NEWBORN_IND|TOS_EXT
 */
drop external table ext_confinement;
CREATE EXTERNAL TABLE ext_confinement (
parentname int, filename text,
PATID bigint, PAT_PLANID bigint, ADMIT_DATE date, CHARGE numeric, COINS numeric, CONF_ID char(21), COPAY numeric, DEDUCT numeric, 
DIAG1 char(7), DIAG2 char(7), DIAG3 char(7), DIAG4 char(7), DIAG5 char(7), DISCH_DATE date, DRG char(5), DSTATUS char(2), ICD_FLAG char(2), IPSTATUS char(1),
LOS numeric, POS char(5), PROC1 char(7), PROC2 char(7), PROC3 char(7), PROC4 char(7), PROC5 char(7),
PROV numeric, STD_COST numeric, STD_COST_YR smallint, TOS_CD char(13), EXTRACT_YM int, VERSION NUMERIC,
ICU_IND text, ICU_SURG_IND text, MAJ_SURG_IND text, MATERNITY_IND text, NEWBORN_IND text, TOS text
) 
LOCATION ( 
'gpfdist://192.168.58.179:8081/optum_dod/2017/dod_c2*.txt.gz#transform=add_parentname_filename_vertbar'
)
FORMAT 'CSV' ( HEADER DELIMITER '|' );

-- Test
/*
select *
from ext_confinement
limit 1000;
*/
-- Insert
insert into optum_dod.confinement (year, file,
PATID, PAT_PLANID, ADMIT_DATE, CHARGE, COINS, CONF_ID, COPAY, DEDUCT, 
DIAG1, DIAG2, DIAG3, DIAG4, DIAG5, DISCH_DATE, DRG, DSTATUS, ICD_FLAG, IPSTATUS,
LOS, POS, PROC1, PROC2, PROC3, PROC4, PROC5,
PROV, STD_COST, STD_COST_YR, TOS_CD, EXTRACT_YM, VERSION,
ICU_IND, ICU_SURG_IND, MAJ_SURG_IND, MATERNITY_IND, NEWBORN_IND, TOS)
select * from ext_confinement;

-- 3 secs
-- alter table optum_dod.confinement add column file text;
-- DEPRECATED: update optum_dod.confinement set year=date_part('year', ADMIT_DATE);

--Refresh
DELETE FROM optum_dod.confinement WHERE YEAR > 2017;
-- Analyze
analyze optum_zip_refresh.confinement;

--Verify
select year, count(*), min(admit_date), max(admit_date)
from optum_zip_refresh.confinement
group by 1
order by 1;