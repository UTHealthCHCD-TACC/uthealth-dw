drop external table ext_snf_value_codes;

CREATE EXTERNAL TABLE ext_snf_value_codes (
BENE_ID varchar, CLM_ID varchar, NCH_CLM_TYPE_CD varchar, RLT_VAL_CD_SEQ varchar, CLM_VAL_CD varchar, CLM_VAL_AMT varchar
) 
LOCATION ( 
'gpfdist://c252-140:8801/medicare/201*/snf_value_codes.csv'
)
FORMAT 'CSV' ( HEADER DELIMITER ',' );

select *
from ext_snf_value_codes
limit 1000;

create table medicare.snf_value_codes
WITH (appendonly=true, orientation=column)
as
select * 
from ext_snf_value_codes
distributed randomly;

select count(*)
from medicare.snf_value_codes;