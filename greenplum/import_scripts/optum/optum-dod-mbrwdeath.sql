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
PatID bigint, Death_ym int4, Extract_ym int4, VERSION numeric, Mbr_Match_Type int, member_id_src text
) 
WITH (appendonly=true, orientation=column, compresstype=zlib)
distributed by (member_id_src);
*/

drop external table ext_mbrwdeath;
CREATE EXTERNAL TABLE ext_mbrwdeath (
PatID bigint, Death_ym int, Extract_ym int, VERSION numeric, Mbr_Match_Type int
) 
LOCATION ( 
'gpfdist://greenplum01.corral.tacc.utexas.edu:8081/uthealth/optum_zip/zip5_mbrwdeath.txt.gz'
)
FORMAT 'CSV' ( HEADER DELIMITER '|' );

-- Test
select *
from ext_mbrwdeath
limit 1000;

-- Insert
insert into optum_zip.mbrwdeath (
PatID, Death_ym, Extract_ym, VERSION, Mbr_Match_Type, member_id_src
)
select ex.PatID
      ,ex.Death_ym
      ,ex.Extract_ym
      ,ex.version
      ,ex.mbr_match_type
      ,ex.patid::text
from ext_mbrwdeath ex;

-- Fix distribution key
create table optum_zip.mbrwdeath_new
WITH (appendonly=true, orientation=column, compresstype=zlib)
as 
select PatID, Death_ym::int4, Extract_ym, VERSION, Mbr_Match_Type, patid::text as member_id_src
from optum_zip.mbrwdeath
distributed by (member_id_src);

drop table optum_zip.mbrwdeath;
ALTER TABLE optum_zip.mbrwdeath_new RENAME TO mbrwdeath;

-- Analyze
analyze optum_zip.mbrwdeath;

--Verify
select count(*) from optum_zip.mbrwdeath;

--Refresh
truncate table optum_zip.mbrwdeath;


