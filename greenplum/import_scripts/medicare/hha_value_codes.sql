drop external table ext_hha_value_codes;

CREATE EXTERNAL TABLE ext_hha_value_codes (
year text,
BENE_ID varchar, CLM_ID varchar, NCH_CLM_TYPE_CD varchar, RLT_VAL_CD_SEQ varchar, CLM_VAL_CD varchar, CLM_VAL_AMT varchar
) 
LOCATION ( 
'gpfdist://192.168.58.179:8081/medicare/*/*hha_value_codes.csv.gz#transform=add_parentname'
)
FORMAT 'CSV' ( HEADER DELIMITER ',' );

select *
from ext_hha_value_codes
limit 1000;

create table medicare.hha_value_codes
WITH (appendonly=true, orientation=column, compresstype=zlib)
as

--insert into medicare.hha_value_codes 
select * 
from ext_hha_value_codes

distributed randomly;

select count(*)
from medicare.hha_value_codes;