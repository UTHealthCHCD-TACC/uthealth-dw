--Optum_DOD mbrwdeath load
drop table optum_dod.member_wdeath;
create table optum_dod.member_wdeath (
PatID bigint, Death_ym Date, Extract_ym Date, VERSION numeric
) 
WITH (appendonly=true, orientation=column)
distributed randomly;

drop external table ext_member_wdeath;
CREATE EXTERNAL TABLE ext_member_wdeath (
PatID bigint, Death_ym int, Extract_ym int, VERSION numeric
) 
LOCATION ( 
'gpfdist://c252-140:8801/optum/dod/dod_mbrwdeath.txt'
)
FORMAT 'CSV' ( HEADER DELIMITER '|' );

-- Test
select *
from ext_member_wdeath
limit 1000;

-- Insert
insert into optum_dod.member_wdeath
select ex.PatID
      ,(ex.Death_ym::varchar || '01')::date
      ,(ex.Extract_ym::varchar || '01')::date
      ,ex.Version
from ext_member_wdeath ex;

-- Analyze
analyze optum_dod.member_wdeath;

--Verify
select count(*) from optum_dod.member_wdeath;
