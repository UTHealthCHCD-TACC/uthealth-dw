drop external table ext_snf_condition_codes;

CREATE EXTERNAL TABLE ext_snf_condition_codes (
YEAR text, filename text,
BENE_ID varchar, CLM_ID varchar, NCH_CLM_TYPE_CD varchar, RLT_COND_CD_SEQ varchar, CLM_RLT_COND_CD varchar
) 
LOCATION ( 
'gpfdist://greenplum01:8081/uthealth/uthealth/medicare_national/*/SNF_CONDITION_CODES.CSV#transform=add_parentname_filename_comma'
)
FORMAT 'CSV' ( HEADER DELIMITER ',' );

select *
from ext_snf_condition_codes
limit 1000;

create table uthealth/medicare_national.snf_condition_codes
WITH (appendonly=true, orientation=column, compresstype=zlib)
as

insert into uthealth/medicare_national.snf_condition_codes (year,
BENE_ID, CLM_ID, NCH_CLM_TYPE_CD, RLT_COND_CD_SEQ, CLM_RLT_COND_CD
)
select year,
BENE_ID, CLM_ID, NCH_CLM_TYPE_CD, RLT_COND_CD_SEQ, CLM_RLT_COND_CD
from ext_snf_condition_codes

distributed randomly;

select count(*)
from uthealth/medicare_national.snf_condition_codes;