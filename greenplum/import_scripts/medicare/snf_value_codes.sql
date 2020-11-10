drop external table ext_snf_value_codes;

CREATE EXTERNAL TABLE ext_snf_value_codes (
year text, filename text,
BENE_ID varchar, CLM_ID varchar, NCH_CLM_TYPE_CD varchar, RLT_VAL_CD_SEQ varchar, CLM_VAL_CD varchar, CLM_VAL_AMT varchar
) 
LOCATION ( 
'gpfdist://192.168.58.179:8081/medicare_texas/*/*snf_value_codes.csv.gz#transform=add_parentname_filename_comma'
)
FORMAT 'CSV' ( HEADER DELIMITER ',' );

select *
from ext_snf_value_codes
limit 1000;

create table medicare_texas.snf_value_codes
WITH (appendonly=true, orientation=column, compresstype=zlib)
as

insert into medicare_texas.snf_value_codes (year,
BENE_ID, CLM_ID, NCH_CLM_TYPE_CD, RLT_VAL_CD_SEQ, CLM_VAL_CD, CLM_VAL_AMT
)
select year,
BENE_ID, CLM_ID, NCH_CLM_TYPE_CD, RLT_VAL_CD_SEQ, CLM_VAL_CD, CLM_VAL_AMT 
from ext_snf_value_codes

distributed randomly;

select count(*)
from medicare_texas.snf_value_codes;