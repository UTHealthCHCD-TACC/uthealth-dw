drop external table ext_admit;

CREATE EXTERNAL TABLE ext_admit (
year text,
ADMIT_ID varchar, PERS_ID varchar, ADM_DT varchar,DSCHRG_DT varchar, pat_stat varchar, LOS varchar
) 
LOCATION ( 
'gpfdist://greenplum01:8081/uthealth/medicare_national/*/*admit_20*.csv.gz#transform=add_parentname_filename_comma'
)
FORMAT 'CSV' ( HEADER DELIMITER ',' );

select *
from ext_admit
limit 1000;

create table uthealth/medicare_national.admit
WITH (appendonly=true, orientation=column, compresstype=zlib)
as

--insert into uthealth/medicare_national.dme_demo_codes 
select * 
from ext_admit

distributed by (admit_id);

select count(*)
from uthealth/medicare_national.dme_demo_codes;