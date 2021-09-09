/* ******************************************************************************************************
 *  This script loads optum_zip/zip.lu_procedure table. 
 *  refresh table is provided as a full replacement
 * ******************************************************************************************************
 *  Author || Date      || Notes
 * ******************************************************************************************************
 *  wallingTACC  ||8/25/2021 || comments added
 * ******************************************************************************************************
 */

/* Original Create
drop table optum_zip.lu_procedure;
create table optum_zip.lu_procedure (
CATEGORY_DTL_CD varchar(100), CATEGORY_DTL_CODE_DESC varchar(100), CATEGORY_GENL_CD varchar(100), CATEGORY_GENL_CODE_DESC varchar(100), 
PROC_CD varchar(100), PROC_DESC varchar(100), PROC_END_DATE date, PROC_TYP_CD varchar(100)
) 
WITH (appendonly=true, orientation=column)
distributed randomly;
*/
 
drop external table ext_lu_procedure;
CREATE EXTERNAL TABLE ext_lu_procedure (
CATEGORY_DTL_CD varchar(100), CATEGORY_DTL_CODE_DESC varchar(100), CATEGORY_GENL_CD varchar(100), CATEGORY_GENL_CODE_DESC varchar(100), 
PROC_CD varchar(100), PROC_DESC varchar(100), PROC_END_DATE date, PROC_TYP_CD varchar(100)
) 
LOCATION ( 
'gpfdist://greenplum01.corral.tacc.utexas.edu:8081/uthealth/OPTUM_NEW/ZIP_july212021/lu_procedure.txt.gz'
)
FORMAT 'CSV' ( HEADER DELIMITER '|' );

-- Test
select *
from ext_lu_procedure
limit 1000;

-- Insert
insert into optum_zip.lu_procedure
select * from ext_lu_procedure;

-- Analyze
analyze optum_zip.lu_procedure;

--Verify
select count(*) from optum_zip.lu_procedure;

--Refresh
truncate table optum_zip.lu_procedure;