/* ******************************************************************************************************
 *  This script loads optum_dod/zip.lu_ndc table.
 *  refresh table is provided as a full replacement
 * ******************************************************************************************************
 *  Author || Date      || Notes
 * ******************************************************************************************************
 *  wallingTACC  ||8/25/2021 || comments added
 * ******************************************************************************************************
 */

/* Original Create
drop table optum_dod.lu_ndc;
create table optum_dod.lu_ndc (
AHFSCLSS varchar(100),AHFSCLSS_DESC varchar(100),BRND_NM varchar(100),
DOSAGE_FM_DESC varchar(100),DRG_STRGTH_DESC varchar(100),DRG_STRGTH_NBR numeric,DRG_STRGTH_UNIT_DESC varchar(100),
DRG_STRGTH_VOL_NBR numeric,DRG_STRGTH_VOL_UNIT_DESC varchar(100),GNRC_IND varchar(100),GNRC_NBR numeric,GNRC_NM varchar(100),GNRC_SQNC_NBR numeric,
NDC varchar(100),NDC_DRG_ROW_EFF_DT date,NDC_DRG_ROW_END_DT date,USC_ID varchar(100),USC_MED_DESC varchar(100)
) 
WITH (appendonly=true, orientation=column)
distributed randomly;
*/
 
drop external table ext_lu_ndc;
CREATE EXTERNAL TABLE ext_lu_ndc (
AHFSCLSS varchar(100),AHFSCLSS_DESC varchar(100),BRND_NM varchar(100),
DOSAGE_FM_DESC varchar(100),DRG_STRGTH_DESC varchar(100),DRG_STRGTH_NBR numeric,DRG_STRGTH_UNIT_DESC varchar(100),
DRG_STRGTH_VOL_NBR numeric,DRG_STRGTH_VOL_UNIT_DESC varchar(100),GNRC_IND varchar(100),GNRC_NBR numeric,GNRC_NM varchar(100),GNRC_SQNC_NBR numeric,
NDC varchar(100),NDC_DRG_ROW_EFF_DT date,NDC_DRG_ROW_END_DT date,USC_ID varchar(100),USC_MED_DESC varchar(100)
) 
LOCATION ( 
'gpfdist://greenplum01.corral.tacc.utexas.edu:8081/uthealth/optum_dod/lu_ndc.txt.gz'
)
FORMAT 'CSV' ( HEADER DELIMITER '|' );

-- Test
select *
from ext_lu_ndc
limit 1000;

-- Insert
insert into optum_dod.lu_ndc
select * from ext_lu_ndc;

-- Analyze
analyze optum_dod.lu_ndc;

--Verify
select count(*) from optum_dod.lu_ndc;

--Refresh
truncate table optum_dod.lu_ndc;