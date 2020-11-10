drop external table ext_inpatient_occurrnce_codes;

CREATE EXTERNAL TABLE ext_inpatient_occurrnce_codes (
year text, filename text,
BENE_ID, CLM_ID, NCH_CLM_TYPE_CD, RLT_OCRNC_CD_SEQ, CLM_RLT_OCRNC_CD, CLM_RLT_OCRNC_DT
) 
LOCATION ( 
'gpfdist://192.168.58.179:8081/medicare_texas/*/*inpatient_occurrnce_codes.csv.gz#transform=add_parentname_filename_comma'
)
FORMAT 'CSV' ( HEADER DELIMITER ',' );

select *
from ext_inpatient_occurrnce_codes
limit 1000;

create table medicare_texas.inpatient_occurrnce_codes
WITH (appendonly=true, orientation=column, compresstype=zlib)
as

insert into medicare_texas.inpatient_occurrnce_codes (year,
BENE_ID, CLM_ID, NCH_CLM_TYPE_CD, RLT_OCRNC_CD_SEQ, CLM_RLT_OCRNC_CD, CLM_RLT_OCRNC_DT
)
select year,
BENE_ID, CLM_ID, NCH_CLM_TYPE_CD, RLT_OCRNC_CD_SEQ, CLM_RLT_OCRNC_CD, CLM_RLT_OCRNC_DT 
from ext_inpatient_occurrnce_codes

distributed randomly;

select count(*)
from medicare_texas.inpatient_occurrnce_codes;