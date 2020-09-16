drop external table ext_admit;

CREATE EXTERNAL TABLE ext_admit (
year text,
ADMIT_ID varchar, PERS_ID varchar, ADM_DT varchar,DSCHRG_DT varchar, pat_stat varchar, LOS varchar
) 
LOCATION ( 
'gpfdist://192.168.58.179:8081/medicare/*/*admit_20*.csv.gz#transform=add_parentname'
)
FORMAT 'CSV' ( HEADER DELIMITER ',' );

select *
from ext_admit
limit 1000;

create table medicare.admit
WITH (appendonly=true, orientation=column, compresstype=zlib)
as

--insert into medicare.dme_demo_codes 
select * 
from ext_admit

distributed by (admit_id);

select count(*)
from medicare.dme_demo_codes;