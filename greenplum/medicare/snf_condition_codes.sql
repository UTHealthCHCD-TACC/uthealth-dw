drop external table ext_snf_condition_codes;

CREATE EXTERNAL TABLE ext_snf_condition_codes (
BENE_ID varchar, CLM_ID varchar, NCH_CLM_TYPE_CD varchar, RLT_COND_CD_SEQ varchar, CLM_RLT_COND_CD varchar
) 
LOCATION ( 
'gpfdist://c252-140:8801/medicare/201*/snf_condition_codes.csv'
)
FORMAT 'CSV' ( HEADER DELIMITER ',' );

select *
from ext_snf_condition_codes
limit 1000;

create table medicare.snf_condition_codes
WITH (appendonly=true, orientation=column)
as
select * 
from ext_snf_condition_codes
distributed randomly;

select count(*)
from medicare.snf_condition_codes;