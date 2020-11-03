drop external table ext_admit_clm;

CREATE EXTERNAL TABLE ext_admit_clm (
year text,
ADMIT_ID varchar,PERS_ID varchar,CLM_ID varchar,BILL varchar,ADM_DT varchar,DSCHRG_DT varchar,FROM_DT varchar, TO_DT varchar

) 
LOCATION ( 
'gpfdist://192.168.58.179:8081/medicare_national/*/*admit_clm_20*.csv.gz#transform=add_parentname'
)
FORMAT 'CSV' ( HEADER DELIMITER ',' );

select *
from ext_admit_clm
limit 1000;

create table medicare_national.admit_clm
WITH (appendonly=true, orientation=column, compresstype=zlib)
as

--insert into medicare_national.dme_demo_codes 
select * 
from ext_admit_clm

distributed by (admit_id);

select count(*)
from medicare_national.admit_clm;