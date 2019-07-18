--Optum_DOD mbrwdeath load
drop table optum_dod.member_wdeath;
create table optum_dod.member_wdeath (
PatID bigint, Death_year_month Date, Extract_year_month Date, VERSION numeric
) 
WITH (appendonly=true, orientation=column)
distributed randomly;

drop external table ext_member_wdeath;
CREATE EXTERNAL TABLE ext_member_wdeath (
PatID bigint, Death_year_month Date, Extract_year_month Date, VERSION numeric
) 
LOCATION ( 
'gpfdist://c252-140:8801/dod_mbrwdeath.txt'
)
FORMAT 'CSV' ( HEADER DELIMITER '|' );

-- Test
select *
from ext_member_wdeath
limit 1000;

-- Insert
insert into optum_dod.member_wdeath
select * from ext_member_wdeath;

-- Analyze
analyze optum_dod.member_wdeath;

--Verify
select count(*) from optum_dod.member_wdeath;
