/* 
******************************************************************************************************
 *  This script loads optum_zip/zip.confinement table
 *  refresh table is provided in most recent quarters 
 *  delete quarters provided in refresh, then load
 *
 * data should be staged in a parent folder with the year matching the filename year, a manual step
 * Examples: staging/optum_zip_refresh/2018/zip5_proc2018q1.txt.gz, staging/optum_zip_refresh/2019/zip5_proc2019q1.txt.gz
 * ******************************************************************************************************
 *  Author || Date      || Notes
 * ******************************************************************************************************
 *  wallingTACC  ||8/25/2021 || comments added
 * ******************************************************************************************************
 */

/* Original Create
drop table optum_zip.confinement;
create table optum_zip.confinement (
year SMALLINT, file varchar,
PATID bigint, PAT_PLANID bigint, ADMIT_DATE date, CHARGE numeric, COINS numeric, CONF_ID char(21), COPAY numeric, DEDUCT numeric, 
DIAG1 char(7), DIAG2 char(7), DIAG3 char(7), DIAG4 char(7), DIAG5 char(7), DISCH_DATE date, DRG char(5), DSTATUS char(2), ICD_FLAG char(2), IPSTATUS char(1),
LOS numeric, POS char(5), PROC1 char(7), PROC2 char(7), PROC3 char(7), PROC4 char(7), PROC5 char(7),
PROV numeric, STD_COST numeric, STD_COST_YR smallint, TOS_CD char(13), EXTRACT_YM int, VERSION NUMERIC,
ICU_IND text, ICU_SURG_IND text, MAJ_SURG_IND text, MATERNITY_IND text, NEWBORN_IND text, TOS text

) 
WITH (appendonly=true, orientation=column, compresstype=zlib)
distributed by (patid);
*/
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
year int, filename text,
PATID bigint, PAT_PLANID bigint, ADMIT_DATE date, CHARGE numeric, COINS numeric, CONF_ID char(21), COPAY numeric, DEDUCT numeric, 
DIAG1 char(7), DIAG2 char(7), DIAG3 char(7), DIAG4 char(7), DIAG5 char(7), DISCH_DATE date, DRG char(5), DSTATUS char(2), ICD_FLAG char(2), IPSTATUS char(1),
LOS numeric, POS char(5), PROC1 char(7), PROC2 char(7), PROC3 char(7), PROC4 char(7), PROC5 char(7),
PROV numeric, STD_COST numeric, STD_COST_YR smallint, TOS_CD char(13), EXTRACT_YM int, VERSION NUMERIC,
ICU_IND text, ICU_SURG_IND text, MAJ_SURG_IND text, MATERNITY_IND text, NEWBORN_IND text, TOS text
) 
LOCATION ( 
'gpfdist://greenplum01.corral.tacc.utexas.edu:8081/uthealth/optum_zip/\*/zip5_c2*.txt.gz#transform=add_parentname_filename_vertbar'
)
FORMAT 'CSV' ( HEADER DELIMITER '|' );

-- Test

select *
from ext_confinement
limit 1000;

-- Insert
insert into optum_zip.confinement (year, file, PATID, PAT_PLANID, ADMIT_DATE, CHARGE, COINS, CONF_ID, COPAY, DEDUCT,
DIAG1, DIAG2, DIAG3, DIAG4, DIAG5, DISCH_DATE, DRG, DSTATUS, ICD_FLAG, IPSTATUS,LOS, POS, 
PROC1, PROC2, PROC3, PROC4, PROC5,PROV, STD_COST, STD_COST_YR, TOS_CD, 
EXTRACT_YM, VERSION,ICU_IND, ICU_SURG_IND, MAJ_SURG_IND, MATERNITY_IND, NEWBORN_IND, TOS, member_id_src)
select year, filename, PATID, PAT_PLANID, ADMIT_DATE, CHARGE, COINS, CONF_ID, COPAY, DEDUCT,
DIAG1, DIAG2, DIAG3, DIAG4, DIAG5, DISCH_DATE, DRG, DSTATUS, ICD_FLAG, IPSTATUS,LOS, POS, 
PROC1, PROC2, PROC3, PROC4, PROC5,PROV, STD_COST, STD_COST_YR, TOS_CD, 
EXTRACT_YM, VERSION,ICU_IND, ICU_SURG_IND, MAJ_SURG_IND, MATERNITY_IND, NEWBORN_IND, TOS, patid::text 
from ext_confinement;

-- Analyze
analyze optum_zip.confinement;

--Verify
select year, count(*), min(admit_date), max(admit_date), 
from optum_zip.confinement
group by 1
order by 1;

--Refresh
select file, count(*)
from optum_zip.confinement 
where file >= 'zip5_c2018q1%'
group by 1
order by 1;

delete
from optum_zip.confinement 
where file >= 'zip5_c2018q2%';