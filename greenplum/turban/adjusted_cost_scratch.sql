--Medical
drop table optum_zip_refresh.confinement;
create table optum_zip_refresh.confinement (
YEAR SMALLINT, PATID bigint, PAT_PLANID bigint, ADMIT_DATE date, CHARGE numeric, COINS numeric, CONF_ID char(21), COPAY numeric, DEDUCT numeric, 
DIAG1 char(7), DIAG2 char(7), DIAG3 char(7), DIAG4 char(7), DIAG5 char(7), DISCH_DATE date, DRG char(5), DSTATUS char(2), ICD_FLAG char(2), IPSTATUS char(1),
LOS numeric, POS char(5), PROC1 char(7), PROC2 char(7), PROC3 char(7), PROC4 char(7), PROC5 char(7),
PROV numeric, STD_COST numeric, STD_COST_YR smallint, TOS_CD char(13), EXTRACT_YM int, VERSION NUMERIC,
ICU_IND text, ICU_SURG_IND text, MAJ_SURG_IND text, MATERNITY_IND text, NEWBORN_IND text, TOS text,
-- *** Adding adjusted cost column - TCU ***
adj_cost numeric,
adj_cost_year smallint
-- *** Adding adjusted cost column - TCU ***
) 
WITH (appendonly=true, orientation=column, compresstype=zlib)
distributed randomly;

-- NOTE: Load each year 1 by one updating the gpfdist string and hard coded YEAR value in insert statement.

drop external table ext_confinement;
CREATE EXTERNAL TABLE ext_confinement (
PATID bigint, PAT_PLANID bigint, ADMIT_DATE date, CHARGE numeric, COINS numeric, CONF_ID char(21), COPAY numeric, DEDUCT numeric, 
DIAG1 char(7), DIAG2 char(7), DIAG3 char(7), DIAG4 char(7), DIAG5 char(7), DISCH_DATE date, DRG char(5), DSTATUS char(2), ICD_FLAG char(2), IPSTATUS char(1),
LOS numeric, POS char(5), PROC1 char(7), PROC2 char(7), PROC3 char(7), PROC4 char(7), PROC5 char(7),
PROV numeric, STD_COST numeric, STD_COST_YR smallint, TOS_CD char(13), EXTRACT_YM int, VERSION NUMERIC,
ICU_IND text, ICU_SURG_IND text, MAJ_SURG_IND text, MATERNITY_IND text, NEWBORN_IND text, TOS text
) 
LOCATION ( 
'gpfdist://192.168.58.179:8081/zip5_c2*'
)
FORMAT 'CSV' ( HEADER DELIMITER '|' );

-- Test
/*
select count(pat_planid)
from ext_confinement
limit 1000;
*/
-- Insert
insert into optum_zip_refresh.confinement
select 0, * from ext_confinement;
-- *** TCU ***
-- Using existing import table as fake external source.
select c.std_cost, c.std_cost_yr, c.tos_cd, cf.cost_factor, left(c.tos_cd, (position('.' in c.tos_cd)-1)) as service_type_code, (c.std_cost * cf.cost_factor) as adj_cost
from optum_zip.confinement c
join reference_tables.ref_optum_cost_factor cf on cf.service_type = left(c.tos_cd, (position('.' in c.tos_cd)-1)) and cf.standard_price_year = c.std_cost_yr 
where c.std_cost_yr <> 2019
limit 100;
-- Looking at records in medical table so we can find some non-2019 cost years.
select c.std_cost, c.std_cost_yr::int, c.tos_cd, cf.cost_factor, left(c.tos_cd, (position('.' in c.tos_cd)-1)) as service_type_code, (c.std_cost * cf.cost_factor) as adj_cost
from optum_zip.medical c
join reference_tables.ref_optum_cost_factor cf on cf.service_type = left(c.tos_cd, (position('.' in c.tos_cd)-1)) and cf.standard_price_year = c.std_cost_yr::int
where c.std_cost_yr::int <> 2019
limit 100;
-- Looking at records in rx table so we can find some non-2019 cost years.
select c.std_cost, c.std_cost_yr, cf.cost_factor, 'PHARM' as service_type_code, (c.std_cost * cf.cost_factor) as adj_cost
from optum_zip.rx c
join reference_tables.ref_optum_cost_factor cf on cf.service_type = 'PHARM' and cf.standard_price_year = c.std_cost_yr
where c.std_cost_yr::int <> 2019
limit 100;

-- examine TOS codes, medical table
select distinct left(c.tos_cd, (position('.' in c.tos_cd)-1)) as service_type_code, count(*) as "total"
from optum_zip.medical c
group by service_type_code;
--- *** TCU *** ---

-- 3 secs
update optum_zip_refresh.confinement set year=date_part('year', ADMIT_DATE);

-- Analyze
analyze optum_zip_refresh.confinement;

--Verify
select year, count(*), min(admit_date), max(admit_date)
from optum_zip_refresh.confinement
group by 1
order by 1;