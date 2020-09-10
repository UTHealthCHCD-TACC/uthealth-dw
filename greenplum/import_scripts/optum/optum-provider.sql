--Medical
drop table optum_zip_refresh.provider;
create table optum_zip_refresh.provider (
PROV_UNIQUE bigint, BED_SZ_RANGE text, CRED_TYPE text, GRP_PRACTICE int, HOSP_AFFIL int, PROV_STATE text, PROV_TYPE text, PROVCAT text, 
TAXONOMY1 text, TAXONOMY2 text, EXTRACT_YM int, VERSION numeric
)
WITH (appendonly=true, orientation=column, compresstype=zlib)
distributed randomly;

drop external table ext_provider;
CREATE EXTERNAL TABLE ext_provider (
PROV_UNIQUE bigint, BED_SZ_RANGE text, CRED_TYPE text, GRP_PRACTICE int, HOSP_AFFIL int, PROV_STATE text, PROV_TYPE text, PROVCAT text, 
TAXONOMY1 text, TAXONOMY2 text, EXTRACT_YM int, VERSION numeric
) 
LOCATION ( 
'gpfdist://192.168.58.179:8081/zip5_provider.txt.gz'
)
FORMAT 'CSV' ( HEADER DELIMITER '|' );

-- Test
select *
from ext_provider
limit 1000;

-- Insert
insert into optum_zip_refresh.provider
select * from ext_provider;

-- Analyze
analyze optum_zip_refresh.provider;

--Verify
select count(*) from optum_zip_refresh.provider;