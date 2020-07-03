--Medical
drop table optum_zip_refresh.provider_bridge;
create table optum_zip_refresh.provider_bridge (
PROV_UNIQUE bigint, DEA char(9), NPI char(10), PROV bigint, EXTRACT_YM int, VERSION numeric
)
WITH (appendonly=true, orientation=column, compresstype=zlib)
distributed randomly;

drop external table ext_provider_bridge;
CREATE EXTERNAL TABLE ext_provider_bridge (
PROV_UNIQUE bigint, DEA char(9), NPI char(10), PROV bigint, EXTRACT_YM int, VERSION numeric
) 
LOCATION ( 
'gpfdist://192.168.58.179:8081/zip5_provider_bridge.txt.gz'
)
FORMAT 'CSV' ( HEADER DELIMITER '|' );

-- Test
select *
from ext_provider_bridge
limit 1000;

-- Insert
insert into optum_zip_refresh.provider_bridge
select * from ext_provider_bridge;

-- Analyze
analyze optum_zip_refresh.provider_bridge;

--Verify
select count(*) from optum_zip_refresh.provider_bridge;