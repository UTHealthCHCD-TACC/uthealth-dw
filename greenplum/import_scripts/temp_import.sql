
drop external table ext_temp;
CREATE EXTERNAL TABLE ext_temp (
admit_id varchar, pers_id varchar, clm_id varchar, bill varchar, adm_dt date, dschrg_dt date, from_dt date, to_dt date
) 
LOCATION ( 
'gpfdist://c252-136:8081/MEDICARE_ADMIT_CLM_202109160835.csv'
)
FORMAT 'CSV' ( HEADER DELIMITER ',' );

-- Test
select *
from ext_temp
limit 1000;

-- Insert

truncate dev.medicare_admit_clm ;
insert into dev.medicare_admit_clm 
select * from ext_temp;
