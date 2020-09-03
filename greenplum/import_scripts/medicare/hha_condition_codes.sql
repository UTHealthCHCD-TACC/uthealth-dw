drop external table ext_hha_condition_codes;

CREATE EXTERNAL TABLE ext_hha_condition_codes (
year text,
BENE_ID varchar, CLM_ID varchar, NCH_CLM_TYPE_CD varchar, RLT_COND_CD_SEQ varchar, CLM_RLT_COND_CD varchar
) 
LOCATION ( 
'gpfdist://192.168.58.179:8081/medicare/*/*hha_condition_codes.csv.gz#transform=add_parentname'
)
FORMAT 'CSV' ( HEADER DELIMITER ',' );

select *
from ext_hha_condition_codes
limit 1000;

create table medicare.hha_condition_codes
WITH (appendonly=true, orientation=column, compresstype=zlib)
as

--insert into medicare.hha_condition_codes 
select * 
from ext_hha_condition_codes

distributed randomly;

select count(*)
from medicare.hha_condition_codes;

