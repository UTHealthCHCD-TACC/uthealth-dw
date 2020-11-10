drop external table ext_inpatient_condition_codes;

CREATE EXTERNAL TABLE ext_inpatient_condition_codes (
year text, filename text,
BENE_ID, CLM_ID, NCH_CLM_TYPE_CD, RLT_COND_CD_SEQ, CLM_RLT_COND_CD
) 
LOCATION ( 
'gpfdist://192.168.58.179:8081/medicare_texas/*/*inpatient_condition_codes.csv.gz#transform=add_parentname_filename_comma'
)
FORMAT 'CSV' ( HEADER DELIMITER ',' );

select *
from ext_inpatient_condition_codes
limit 1000;

create table medicare_texas.inpatient_condition_codes
WITH (appendonly=true, orientation=column, compresstype=zlib)
as

insert into medicare_texas.inpatient_condition_codes (year,
BENE_ID, CLM_ID, NCH_CLM_TYPE_CD, RLT_COND_CD_SEQ, CLM_RLT_COND_CD
)
select year,
BENE_ID, CLM_ID, NCH_CLM_TYPE_CD, RLT_COND_CD_SEQ, CLM_RLT_COND_CD 
from ext_inpatient_condition_codes

distributed randomly;

select count(*)
from medicare_texas.inpatient_condition_codes;