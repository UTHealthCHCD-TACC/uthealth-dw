--Medical
drop table optum_dod.provider;
create table optum_dod.provider (
PROV_UNIQUE bigint, BED_SZ_RANGE char(20), CRED_TYPE char(20), GRP_PRACTICE int, HOSP_AFFIL int, PROV_STATE char(2), PROV_TYPE char(3), PROVCAT char(4), 
TAXONOMY1 char(10), TAXONOMY2 char(10), EXTRACT_YM int, VERSION numeric
)
WITH (appendonly=true, orientation=column)
distributed randomly;

drop external table ext_provider;
CREATE EXTERNAL TABLE ext_provider (
PROV_UNIQUE bigint, BED_SZ_RANGE char(20), CRED_TYPE char(20), GRP_PRACTICE int, HOSP_AFFIL int, PROV_STATE char(2), PROV_TYPE char(3), PROVCAT char(4), 
TAXONOMY1 char(10), TAXONOMY2 char(10), EXTRACT_YM int, VERSION numeric
) 
LOCATION ( 
'gpfdist://c252-140:8801/dod_provider.txt'
)
FORMAT 'CSV' ( HEADER DELIMITER '|' );

-- Test
select *
from ext_provider
limit 1000;

-- Insert
insert into optum_dod.provider
select * from ext_provider;

-- Analyze
analyze optum_dod.provider;

--Verify
select count(*) from optum_dod.provider;