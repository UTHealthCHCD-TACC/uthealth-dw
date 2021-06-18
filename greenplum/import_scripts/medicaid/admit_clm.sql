--Medical
drop table medicaid.admit_clm;
create table medicaid.admit_clm (
ADMIT_ID varchar, PERS_ID varchar, CLM_ID varchar, BILL varchar, ADM_DT date, DSCHRG_DT date, FROM_DT date, TO_DT date
) 
WITH (appendonly=true, orientation=column, compresstype=zlib)
distributed by (ADMIT_ID);

drop external table ext_medicaid_admit_clm;
CREATE EXTERNAL TABLE ext_medicaid_admit_clm (
ADMIT_ID varchar, PERS_ID varchar, CLM_ID varchar, BILL varchar, ADM_DT date, DSCHRG_DT date, FROM_DT date, TO_DT date
) 
LOCATION ( 
'gpfdist://greenplum01:8081/uthealth/medicaid/ADMIT_CLM.csv'
)
FORMAT 'CSV' ( HEADER DELIMITER ',' );

-- Test
/*
select *
from ext_medicaid_admit_clm
limit 10;
*/
-- Insert
insert into medicaid.admit_clm
select * from ext_medicaid_admit_clm;


-- Analyze
analyze medicaid.admit_clm;
 
-- Verify
select count(*), min(year_fy), max(year_fy), count(distinct year_fy) from medicaid.admit_clm;


select year_fy, date_part('quarter', FST_DT) as quarter, count(*), min(FST_DT), max(FST_DT)
from medicaid.chip_prov
group by 1, 2
order by 1, 2;
