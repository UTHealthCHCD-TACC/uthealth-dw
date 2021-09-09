/* ******************************************************************************************************
 *  This script loads optum_zip/zip.mbrwdeath table.  dod only
 *  refresh table is provided as a full replacement
 * ******************************************************************************************************
 *  Author || Date      || Notes
 * ******************************************************************************************************
 *  wallingTACC  ||8/25/2021 || comments added
 * ******************************************************************************************************
 *  wallingTACC  ||8/25/2021 || changed extract_ym to int4 to match other tables.  Did not save code.
 * ******************************************************************************************************
 */

/* original create 
drop table optum_zip.mbrwdeath;
create table optum_zip.mbrwdeath (
PatID bigint, Death_ym Date, Extract_ym Date, VERSION numeric, Mbr_Match_Type int
) 
WITH (appendonly=true, orientation=column, compresstype=zlib)
distributed by (patid);
*/

drop external table ext_mbrwdeath;
CREATE EXTERNAL TABLE ext_mbrwdeath (
PatID bigint, Death_ym int, Extract_ym int, VERSION numeric, Mbr_Match_Type int
) 
LOCATION ( 
'gpfdist://greenplum01.corral.tacc.utexas.edu:8081/uthealth/OPTUM_NEW/ZIP_july212021/zip5_mbrwdeath.txt.gz'
)
FORMAT 'CSV' ( HEADER DELIMITER '|' );

-- Test
select *
from ext_mbrwdeath
limit 1000;

-- Insert
insert into optum_zip.mbrwdeath
select ex.PatID
      ,(ex.Death_ym::varchar || '01')::date
      ,(ex.Extract_ym::varchar || '01')::date
      ,ex.Version
from ext_mbrwdeath ex;

-- Analyze
analyze optum_zip.mbrwdeath;

--Verify
select count(*) from optum_zip.mbrwdeath;

--Refresh
truncate table optum_zip.mbrwdeath;


