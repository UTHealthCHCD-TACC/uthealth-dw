drop external table ext_outpatient_value_codes;

CREATE EXTERNAL TABLE ext_outpatient_value_codes (
year text, filename text,
BENE_ID varchar, CLM_ID varchar, NCH_CLM_TYPE_CD varchar, RLT_VAL_CD_SEQ varchar, CLM_VAL_CD varchar, CLM_VAL_AMT varchar
) 
LOCATION ( 
'gpfdist://greenplum01:8081/uthealth/medicare_national/*/*outpatient_value_codes.csv.gz#transform=add_parentname_filename_comma'
)
FORMAT 'CSV' ( HEADER DELIMITER ',' );

select *
from ext_outpatient_value_codes
limit 1000;

create table uthealth/medicare_national.outpatient_value_codes
WITH (appendonly=true, orientation=column, compresstype=zlib)
as

insert into uthealth/medicare_national.outpatient_value_codes (year,
BENE_ID, CLM_ID, NCH_CLM_TYPE_CD, RLT_VAL_CD_SEQ, CLM_VAL_CD, CLM_VAL_AMT
)
select year,
BENE_ID, CLM_ID, NCH_CLM_TYPE_CD, RLT_VAL_CD_SEQ, CLM_VAL_CD, CLM_VAL_AMT 
from ext_outpatient_value_codes

distributed randomly;

select count(*)
from uthealth/medicare_national.outpatient_value_codes;