drop external table ext_snf_condition_codes;

CREATE EXTERNAL TABLE ext_snf_condition_codes (
YEAR text,
BENE_ID varchar, CLM_ID varchar, NCH_CLM_TYPE_CD varchar, RLT_COND_CD_SEQ varchar, CLM_RLT_COND_CD varchar
) 
LOCATION ( 
'gpfdist://192.168.58.179:8081/medicare/*/*snf_condition_codes.csv.gz#transform=add_parentname'
)
FORMAT 'CSV' ( HEADER DELIMITER ',' );

select *
from ext_snf_condition_codes
limit 1000;

create table medicare.snf_condition_codes
WITH (appendonly=true, orientation=column, compresstype=zlib)
as
select * 
from ext_snf_condition_codes
distributed randomly;

select count(*)
from medicare.snf_condition_codes;