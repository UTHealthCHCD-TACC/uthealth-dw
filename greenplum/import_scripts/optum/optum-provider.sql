/* ******************************************************************************************************
 *  This script loads optum_dod/zip.provider table
 *  refresh table is provided as a full replacement
 * ******************************************************************************************************
 *  Author || Date      || Notes
 * ******************************************************************************************************
 *  wallingTACC  ||8/25/2021 || comments added
 * ******************************************************************************************************
 */

/* Original Create
drop table optum_dod.provider;
create table optum_dod.provider (
PROV_UNIQUE bigint, BED_SZ_RANGE text, CRED_TYPE text, GRP_PRACTICE int, HOSP_AFFIL int, PROV_STATE text, PROV_TYPE text, PROVCAT text, 
TAXONOMY1 text, TAXONOMY2 text, EXTRACT_YM int, VERSION numeric
)
WITH (appendonly=true, orientation=column, compresstype=zlib)
distributed randomly;
*/

drop external table ext_provider;
CREATE EXTERNAL TABLE ext_provider (
PROV_UNIQUE bigint, BED_SZ_RANGE text, CRED_TYPE text, GRP_PRACTICE int, HOSP_AFFIL int, PROV_STATE text, PROV_TYPE text, PROVCAT text, 
TAXONOMY1 text, TAXONOMY2 text, EXTRACT_YM int, VERSION numeric
) 
LOCATION ( 
'gpfdist://greenplum01.corral.tacc.utexas.edu:8081/uthealth/optum_dod/dod_provider.txt.gz'
)
FORMAT 'CSV' ( HEADER DELIMITER '|' );

-- Test
select count(*)
from ext_provider
limit 1000;

-- Insert
insert into optum_dod.provider
select * from ext_provider;

-- Analyze
analyze optum_dod.provider;

--Verify
select count(*) from optum_dod.provider;

--Refresh
truncate table optum_dod.provider;