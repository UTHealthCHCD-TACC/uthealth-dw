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
drop table optum_dod.provider_bridge;
create table optum_dod.provider_bridge (
PROV_UNIQUE bigint, DEA char(9), NPI char(10), PROV bigint, EXTRACT_YM int, VERSION numeric
)
WITH (appendonly=true, orientation=column, compresstype=zlib)
distributed randomly;
*/

drop external table ext_provider_bridge;
CREATE EXTERNAL TABLE ext_provider_bridge (
PROV_UNIQUE bigint, DEA char(9), NPI char(10), PROV bigint, EXTRACT_YM int, VERSION numeric
) 
LOCATION ( 
'gpfdist://greenplum01.corral.tacc.utexas.edu:8081/uthealth/optum_dod/dod_provider_bridge.txt.gz'
)
FORMAT 'CSV' ( HEADER DELIMITER '|' );

-- Test
select *
from ext_provider_bridge
limit 1000;

-- Insert
insert into optum_dod.provider_bridge
select * from ext_provider_bridge;

-- Analyze
analyze optum_dod.provider_bridge;

--Verify
select count(*) from optum_dod.provider_bridge;

--Refresh
truncate table optum_dod.provider_bridge;