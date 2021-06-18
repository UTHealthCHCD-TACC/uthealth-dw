--optum_zip mbrwdeath load
drop table optum_dod.mbrwdeath;
create table optum_dod.mbrwdeath (
PatID bigint, Death_ym Date, Extract_ym Date, VERSION numeric, Mbr_Match_Type int
) 
WITH (appendonly=true, orientation=column, compresstype=zlib)
distributed by (patid);

drop external table ext_mbrwdeath;
CREATE EXTERNAL TABLE ext_mbrwdeath (
PatID bigint, Death_ym int, Extract_ym int, VERSION numeric, Mbr_Match_Type int
) 
LOCATION ( 
'gpfdist://greenplum01.corral.tacc.utexas.edu:8081/uthealth/OPTUM_NEW/OPT_DOD_APril2021/dod_mbrwdeath.txt.gz'
)
FORMAT 'CSV' ( HEADER DELIMITER '|' );

-- Test
select *
from ext_mbrwdeath
limit 1000;

-- Insert
insert into optum_dod.mbrwdeath
select ex.PatID
      ,(ex.Death_ym::varchar || '01')::date
      ,(ex.Extract_ym::varchar || '01')::date
      ,ex.Version
from ext_mbrwdeath ex;

-- Analyze
analyze optum_dod.mbrwdeath;

--Verify
select count(*) from optum_dod.mbrwdeath;


