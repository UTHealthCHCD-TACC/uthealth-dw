--Medical
drop table medicaid.admit;
create table medicaid.admit (
ADMIT_ID varchar, PERS_ID varchar, ADM_DT date, DSCHRG_DT date, pat_stat varchar
) 
WITH (appendonly=true, orientation=column, compresstype=zlib)
distributed by (ADMIT_ID);

drop external table ext_medicaid_admit;
CREATE EXTERNAL TABLE ext_medicaid_admit (
ADMIT_ID varchar, PERS_ID varchar, ADM_DT date, DSCHRG_DT date, pat_stat varchar
) 
LOCATION ( 
'gpfdist://greenplum01:8081/uthealth/medicaid/ADMIT.csv'
)
FORMAT 'CSV' ( HEADER DELIMITER ',' );

-- Test
/*
select *
from ext_medicaid_admit
limit 10;
*/
-- Insert
insert into medicaid.admit
select * from ext_medicaid_admit;


-- Analyze
analyze medicaid.admit;
 
-- Verify
select count(*), min(year_fy), max(year_fy), count(distinct year_fy) from medicaid.admit;


select year_fy, date_part('quarter', FST_DT) as quarter, count(*), min(FST_DT), max(FST_DT)
from medicaid.chip_prov
group by 1, 2
order by 1, 2;
